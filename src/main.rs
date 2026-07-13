use std::{
    collections::{HashMap, HashSet},
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::{Stream, StreamExt, TryStreamExt};
use reqwest::{header, Body, Client, StatusCode};
use reqwest_011::{Client as TelegramApiClient, Proxy as TelegramApiProxy};
use serde::{Deserialize, Serialize};
use serde_json::json;
use teloxide::{
    prelude::*,
    types::{Document, MediaKind, MessageKind, PhotoSize, Video},
};
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const RESUMABLE_UPLOAD_URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name,size,webViewLink&supportsAllDrives=true";
const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // Google resumable uploads require multiples of 256 KiB.

#[derive(Clone)]
struct AppState {
    telegram_http: Client,
    storage: Arc<StorageClient>,
    access: Arc<AccessControl>,
    runtime: Arc<RuntimeConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    telegram_token: String,
    #[serde(default)]
    allowed_telegram_usernames: Vec<String>,
    #[serde(default)]
    admin_telegram_usernames: Vec<String>,
    #[serde(default)]
    features: FeatureConfig,
    #[serde(default)]
    bale: Option<BaleConfig>,
    #[serde(default)]
    google_drive_folder_id: Option<String>,
    #[serde(default)]
    storage: Option<StorageConfig>,
    #[serde(default)]
    google: Option<GoogleConfig>,
    #[serde(default)]
    oauth_server: OAuthServerConfig,
    #[serde(default)]
    proxy: ProxyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FeatureConfig {
    #[serde(default = "default_true")]
    telegram_bot: bool,
    #[serde(default = "default_true")]
    telegram_uploads: bool,
    #[serde(default = "default_true")]
    direct_url_uploads: bool,
    #[serde(default = "default_true")]
    google_drive_uploads: bool,
    #[serde(default = "default_true")]
    b2_uploads: bool,
    #[serde(default)]
    bale_bot: bool,
    #[serde(default)]
    telegram_to_bale: bool,
    #[serde(default)]
    bale_to_telegram: bool,
    #[serde(default = "default_true")]
    admin_config_reload: bool,
}

impl Default for FeatureConfig {
    fn default() -> Self {
        Self {
            telegram_bot: true,
            telegram_uploads: true,
            direct_url_uploads: true,
            google_drive_uploads: true,
            b2_uploads: true,
            bale_bot: false,
            telegram_to_bale: false,
            bale_to_telegram: false,
            admin_config_reload: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
struct BaleConfig {
    token: String,
    #[serde(default)]
    target_chat_id: Option<i64>,
}

#[derive(Debug)]
struct RuntimeConfig {
    path: PathBuf,
    current: Mutex<Config>,
}

impl RuntimeConfig {
    async fn reload(&self) -> Result<()> {
        let config = read_config_from_path(&self.path).await?;
        *self.current.lock().await = config;
        Ok(())
    }

    async fn snapshot(&self) -> Config {
        self.current.lock().await.clone()
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ProxyConfig {
    #[serde(default)]
    all: Option<String>,
    #[serde(default)]
    telegram_receive: Option<String>,
    #[serde(default)]
    google_drive: Option<String>,
    #[serde(default)]
    b2: Option<String>,
}

impl ProxyConfig {
    fn telegram_receive(&self) -> Option<&str> {
        normalized_proxy(self.telegram_receive.as_ref())
            .or_else(|| normalized_proxy(self.all.as_ref()))
    }

    fn google_drive(&self) -> Option<&str> {
        normalized_proxy(self.google_drive.as_ref()).or_else(|| normalized_proxy(self.all.as_ref()))
    }

    fn b2(&self) -> Option<&str> {
        normalized_proxy(self.b2.as_ref()).or_else(|| normalized_proxy(self.all.as_ref()))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case")]
enum StorageConfig {
    GoogleDrive {
        #[serde(default)]
        folder_id: Option<String>,
        google: GoogleConfig,
    },
    B2 {
        key_id: String,
        application_key: String,
        bucket_id: String,
        #[serde(default)]
        bucket_name: Option<String>,
        #[serde(default)]
        file_prefix: Option<String>,
        #[serde(default = "default_b2_part_size")]
        recommended_part_size: u64,
    },
}

fn default_b2_part_size() -> u64 {
    100 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum GoogleConfig {
    #[serde(rename = "oauth", alias = "o_auth")]
    OAuth {
        client_id: String,
        client_secret: String,
        redirect_uri: String,
        #[serde(default = "default_oauth_tokens_file")]
        tokens_file: PathBuf,
    },
}

#[derive(Debug, Clone, Deserialize)]
struct OAuthServerConfig {
    #[serde(default = "default_oauth_bind_addr")]
    bind_addr: SocketAddr,
}

impl Default for OAuthServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_oauth_bind_addr(),
        }
    }
}

fn default_oauth_bind_addr() -> SocketAddr {
    "0.0.0.0:8080".parse().expect("valid default bind address")
}

fn default_oauth_tokens_file() -> PathBuf {
    PathBuf::from("google-oauth-tokens.json")
}

fn config_path() -> PathBuf {
    env::args()
        .nth(1)
        .or_else(|| env::var("CONFIG_PATH").ok())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config.json"))
}

async fn load_config() -> Result<(PathBuf, Config)> {
    let path = config_path();
    let config = read_config_from_path(&path).await?;
    Ok((path, config))
}

async fn read_config_from_path(path: &PathBuf) -> Result<Config> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read config file at {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("invalid JSON config at {}", path.display()))
}

#[derive(Debug)]
struct AccessControl {
    allowed_usernames: HashSet<String>,
    admin_usernames: HashSet<String>,
}

impl AccessControl {
    fn from_config(config: &Config) -> Self {
        let allowed_usernames = config
            .allowed_telegram_usernames
            .iter()
            .map(|username| normalize_username(username))
            .filter(|username| !username.is_empty())
            .collect();
        let admin_usernames = config
            .admin_telegram_usernames
            .iter()
            .map(|username| normalize_username(username))
            .filter(|username| !username.is_empty())
            .collect();
        Self {
            allowed_usernames,
            admin_usernames,
        }
    }

    fn is_admin(&self, msg: &Message) -> bool {
        msg.from
            .as_ref()
            .and_then(|user| user.username.as_deref())
            .map(normalize_username)
            .is_some_and(|username| self.admin_usernames.contains(&username))
    }

    fn is_allowed(&self, msg: &Message) -> bool {
        // If no list is configured, keep local/dev usage convenient and allow everyone.
        if self.allowed_usernames.is_empty() {
            return true;
        }

        msg.from
            .as_ref()
            .and_then(|user| user.username.as_deref())
            .map(normalize_username)
            .is_some_and(|username| self.allowed_usernames.contains(&username))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedOAuthToken {
    access_token: String,
    refresh_token: String,
    expires_at_unix: u64,
}

#[derive(Debug)]
enum StorageClient {
    GoogleDrive(Arc<DriveClient>),
    B2(B2Client),
}

#[derive(Debug)]
struct UploadedFile {
    name: Option<String>,
    size: Option<u64>,
    link: Option<String>,
}

#[derive(Debug)]
struct B2Client {
    http: Client,
    key_id: String,
    application_key: String,
    bucket_id: String,
    bucket_name: Option<String>,
    file_prefix: Option<String>,
    recommended_part_size: u64,
    auth: Mutex<Option<B2Auth>>,
    upload: Mutex<Option<B2UploadUrl>>,
}

#[derive(Debug, Clone)]
struct B2Auth {
    authorization_token: String,
    api_url: String,
    download_url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct B2AuthorizeResponse {
    authorization_token: String,
    api_url: String,
    download_url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct B2UploadUrl {
    upload_url: String,
    authorization_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct B2UploadResponse {
    file_name: String,
    content_length: u64,
}

impl StorageClient {
    async fn from_config(google_http: Client, b2_http: Client, config: &Config) -> Result<Self> {
        let storage = match (&config.storage, &config.google) {
            (Some(storage), _) => storage.clone(),
            (None, Some(google)) => StorageConfig::GoogleDrive {
                folder_id: config.google_drive_folder_id.clone(),
                google: google.clone(),
            },
            (None, None) => {
                return Err(anyhow!(
                    "config requires either storage or legacy google section"
                ))
            }
        };

        match storage {
            StorageConfig::GoogleDrive { folder_id, google } => {
                let drive_config = Config {
                    telegram_token: config.telegram_token.clone(),
                    allowed_telegram_usernames: config.allowed_telegram_usernames.clone(),
                    admin_telegram_usernames: config.admin_telegram_usernames.clone(),
                    features: config.features.clone(),
                    bale: config.bale.clone(),
                    google_drive_folder_id: folder_id
                        .or_else(|| config.google_drive_folder_id.clone()),
                    storage: None,
                    google: Some(google),
                    oauth_server: config.oauth_server.clone(),
                    proxy: config.proxy.clone(),
                };
                Ok(Self::GoogleDrive(Arc::new(
                    DriveClient::from_config(google_http, &drive_config).await?,
                )))
            }
            StorageConfig::B2 {
                key_id,
                application_key,
                bucket_id,
                bucket_name,
                file_prefix,
                recommended_part_size,
            } => Ok(Self::B2(B2Client {
                http: b2_http,
                key_id,
                application_key,
                bucket_id,
                bucket_name,
                file_prefix: file_prefix.and_then(|p| normalize_b2_prefix(&p)),
                recommended_part_size,
                auth: Mutex::new(None),
                upload: Mutex::new(None),
            })),
        }
    }

    fn drive(&self) -> Option<Arc<DriveClient>> {
        match self {
            Self::GoogleDrive(drive) => Some(drive.clone()),
            Self::B2(_) => None,
        }
    }

    fn uses_oauth(&self) -> bool {
        matches!(self, Self::GoogleDrive(drive) if drive.uses_oauth())
    }

    async fn has_oauth_token(&self, telegram_user_id: u64) -> bool {
        match self {
            Self::GoogleDrive(drive) => drive.has_oauth_token(telegram_user_id).await,
            Self::B2(_) => true,
        }
    }

    async fn authorization_url(&self, telegram_user_id: u64, chat_id: ChatId) -> Result<String> {
        match self {
            Self::GoogleDrive(drive) => drive.authorization_url(telegram_user_id, chat_id).await,
            Self::B2(_) => Err(anyhow!("/auth is only used by Google Drive OAuth mode")),
        }
    }

    async fn upload_stream<S>(
        &self,
        telegram_user_id: u64,
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
        stream: S,
    ) -> Result<UploadedFile>
    where
        S: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin + Send + 'static,
    {
        match self {
            Self::GoogleDrive(drive) => {
                let file = drive
                    .upload_stream(telegram_user_id, name, mime_type, total_size, stream)
                    .await?;
                Ok(UploadedFile {
                    name: file.name,
                    size: file.size.and_then(|s| s.parse::<u64>().ok()),
                    link: file.web_view_link.or_else(|| {
                        Some(format!("https://drive.google.com/file/d/{}/view", file.id))
                    }),
                })
            }
            Self::B2(b2) => b2.upload_stream(name, mime_type, total_size, stream).await,
        }
    }
}

impl B2Client {
    async fn upload_stream<S>(
        &self,
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
        stream: S,
    ) -> Result<UploadedFile>
    where
        S: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin + Send + 'static,
    {
        if total_size > 5 * 1024 * 1024 * 1024 {
            return Err(anyhow!(
                "B2 single-call streaming uploads are limited to 5 GiB; configure Telegram/direct-link limits below that or add multipart upload support with at least {} buffering",
                format_bytes(self.recommended_part_size)
            ));
        }
        let auth = self.authorize().await?;
        let upload = self.upload_url(&auth).await?;
        let file_name = self.b2_file_name(name);
        let body_stream = stream.map_err(std::io::Error::other);
        let response = self
            .http
            .post(&upload.upload_url)
            .header(header::AUTHORIZATION, upload.authorization_token)
            .header("X-Bz-File-Name", percent_encode_b2_name(&file_name))
            .header(header::CONTENT_TYPE, mime_type.unwrap_or("b2/x-auto"))
            .header(header::CONTENT_LENGTH, total_size)
            .header("X-Bz-Content-Sha1", "do_not_verify")
            .body(Body::wrap_stream(body_stream))
            .send()
            .await
            .context("failed to upload stream to Backblaze B2")?;
        let status = response.status();
        if !status.is_success() {
            *self.upload.lock().await = None;
            let err_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("B2 upload failed: HTTP {status} - {err_text}"));
        }
        let uploaded: B2UploadResponse = response
            .json()
            .await
            .context("failed to parse B2 upload response")?;
        Ok(UploadedFile {
            name: Some(uploaded.file_name.clone()),
            size: Some(uploaded.content_length),
            link: Some(format!(
                "{}/file/{}/{}",
                auth.download_url,
                self.bucket_name.as_deref().unwrap_or(&self.bucket_id),
                percent_encode_b2_download_name(&uploaded.file_name)
            )),
        })
    }

    async fn authorize(&self) -> Result<B2Auth> {
        if let Some(auth) = self.auth.lock().await.clone() {
            return Ok(auth);
        }
        let basic = STANDARD.encode(format!("{}:{}", self.key_id, self.application_key));
        let response = self
            .http
            .get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
            .header(header::AUTHORIZATION, format!("Basic {basic}"))
            .send()
            .await
            .context("failed to authorize Backblaze B2 account")?;
        let status = response.status();
        if !status.is_success() {
            let err_text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "B2 authorization failed: HTTP {status} - {err_text}"
            ));
        }
        let raw: B2AuthorizeResponse = response
            .json()
            .await
            .context("failed to parse B2 authorization response")?;
        let auth = B2Auth {
            authorization_token: raw.authorization_token,
            api_url: raw.api_url,
            download_url: raw.download_url,
        };
        *self.auth.lock().await = Some(auth.clone());
        Ok(auth)
    }

    async fn upload_url(&self, auth: &B2Auth) -> Result<B2UploadUrl> {
        if let Some(upload) = self.upload.lock().await.clone() {
            return Ok(upload);
        }
        let response = self
            .http
            .post(format!("{}/b2api/v2/b2_get_upload_url", auth.api_url))
            .header(header::AUTHORIZATION, &auth.authorization_token)
            .json(&json!({ "bucketId": self.bucket_id }))
            .send()
            .await
            .context("failed to request Backblaze B2 upload URL")?;
        let status = response.status();
        if !status.is_success() {
            let err_text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "B2 get upload URL failed: HTTP {status} - {err_text}"
            ));
        }
        let upload: B2UploadUrl = response
            .json()
            .await
            .context("failed to parse B2 upload URL response")?;
        *self.upload.lock().await = Some(upload.clone());
        Ok(upload)
    }

    fn b2_file_name(&self, name: &str) -> String {
        let safe = name.trim().trim_start_matches('/');
        match &self.file_prefix {
            Some(prefix) => format!("{prefix}/{safe}"),
            None => safe.to_owned(),
        }
    }
}

fn normalize_b2_prefix(prefix: &str) -> Option<String> {
    let trimmed = prefix.trim().trim_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn percent_encode_b2_name(name: &str) -> String {
    urlencoding::encode(name).replace("%2F", "/")
}

fn percent_encode_b2_download_name(name: &str) -> String {
    urlencoding::encode(name)
        .replace("%2F", "/")
        .replace("%20", "+")
}

#[derive(Debug)]
enum DriveAuth {
    OAuth {
        client_id: String,
        client_secret: String,
        redirect_uri: String,
        tokens_path: PathBuf,
        tokens: Mutex<HashMap<u64, PersistedOAuthToken>>,
        pending_states: Mutex<HashMap<String, PendingOAuth>>,
    },
}

#[derive(Debug, Clone)]
struct PendingOAuth {
    telegram_user_id: u64,
    chat_id: ChatId,
}

#[derive(Debug)]
struct DriveClient {
    http: Client,
    folder_id: Option<String>,
    auth: DriveAuth,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DriveFile {
    id: String,
    name: Option<String>,
    size: Option<String>,
    web_view_link: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OAuthCallback {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("info".parse().unwrap()),
        )
        .init();

    if let Err(err) = run().await {
        error!(error = ?err, "bot stopped");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let (config_file, config) = load_config().await?;
    let telegram_http = build_http_client(config.proxy.telegram_receive())?;
    let telegram_api_http = build_telegram_api_client(config.proxy.telegram_receive())?;
    let google_http = build_http_client(config.proxy.google_drive())?;
    let b2_http = build_http_client(config.proxy.b2())?;
    let bot = Bot::with_client(config.telegram_token.clone(), telegram_api_http);
    let storage = Arc::new(StorageClient::from_config(google_http, b2_http, &config).await?);
    ensure_enabled_storage(&config, &storage)?;
    let runtime = Arc::new(RuntimeConfig {
        path: config_file,
        current: Mutex::new(config.clone()),
    });

    if let Some(drive) = storage.drive() {
        if drive.uses_oauth() {
            spawn_oauth_server(bot.clone(), drive.clone(), config.oauth_server.bind_addr).await?;
        }
    }

    let state = AppState {
        telegram_http,
        storage,
        access: Arc::new(AccessControl::from_config(&config)),
        runtime: runtime.clone(),
    };

    if config.features.bale_bot {
        if let Some(bale) = config.bale.clone() {
            tokio::spawn(run_bale_bot(
                bale,
                state.clone(),
                config.telegram_token.clone(),
            ));
        }
    }

    if !config.features.telegram_bot {
        info!("Telegram bot disabled; Bale bot/background services are running");
        tokio::signal::ctrl_c().await?;
        return Ok(());
    }

    info!("starting Telegram uploader bot");
    teloxide::repl(bot, move |bot: Bot, msg: Message| {
        let state = state.clone();
        async move {
            if !state.access.is_allowed(&msg) {
                return respond(());
            }

            if let Err(err) = handle_message(bot.clone(), msg.clone(), state).await {
                error!(chat_id = msg.chat.id.0, error = ?err, "failed to process message");
                bot.send_message(msg.chat.id, format!("Upload failed: {err:#}"))
                    .await?;
            }
            respond(())
        }
    })
    .await;
    Ok(())
}

async fn spawn_oauth_server(bot: Bot, drive: Arc<DriveClient>, addr: SocketAddr) -> Result<()> {
    let app = Router::new()
        .route("/oauth2/callback", get(oauth_callback))
        .with_state((bot, drive));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            error!(error = ?err, "OAuth callback server stopped");
        }
    });
    info!(%addr, "OAuth callback server listening");
    Ok(())
}

async fn oauth_callback(
    State((bot, drive)): State<(Bot, Arc<DriveClient>)>,
    Query(query): Query<OAuthCallback>,
) -> impl IntoResponse {
    match oauth_callback_inner(bot, drive, query).await {
        Ok(()) => Html(
            "<h1>Google Drive connected</h1><p>You can close this page and return to Telegram.</p>",
        )
        .into_response(),
        Err(err) => Html(format!("<h1>Authorization failed</h1><p>{err:#}</p>")).into_response(),
    }
}

async fn oauth_callback_inner(
    bot: Bot,
    drive: Arc<DriveClient>,
    query: OAuthCallback,
) -> Result<()> {
    if let Some(error) = query.error {
        return Err(anyhow!("Google returned OAuth error: {error}"));
    }
    let code = query.code.context("missing OAuth code")?;
    let state = query.state.context("missing OAuth state")?;
    let pending = drive.complete_oauth(&state, &code).await?;
    bot.send_message(
        pending.chat_id,
        "Google Drive connected. Send a file and I will upload it to your Drive.",
    )
    .await?;
    Ok(())
}

async fn handle_message(bot: Bot, msg: Message, state: AppState) -> Result<()> {
    if let Some(text) = msg.text() {
        let trimmed = text.trim();
        if trimmed == "/reload" && state.access.is_admin(&msg) {
            if !state.runtime.snapshot().await.features.admin_config_reload {
                bot.send_message(msg.chat.id, "Admin config reload is disabled.")
                    .await?;
                return Ok(());
            }
            state.runtime.reload().await?;
            bot.send_message(msg.chat.id, "Reloaded config.json for runtime feature checks. Restart to change tokens, storage clients, proxies, or allow-lists.").await?;
            return Ok(());
        }
        if trimmed == "/config" && state.access.is_admin(&msg) {
            let config = state.runtime.snapshot().await;
            bot.send_message(
                msg.chat.id,
                format!(
                    "Runtime feature config:
```json
{}
```",
                    serde_json::to_string_pretty(&config.features)?
                ),
            )
            .await?;
            return Ok(());
        }
        if trimmed == "/start" || trimmed == "/help" {
            bot.send_message(msg.chat.id, help_text(&state.storage))
                .await?;
            return Ok(());
        }
        if trimmed == "/auth" {
            let user_id = telegram_user_id(&msg).context("could not identify Telegram user")?;
            let url = state
                .storage
                .authorization_url(user_id, msg.chat.id)
                .await?;
            bot.send_message(
                msg.chat.id,
                format!("Open this URL to connect Google Drive:\n{url}"),
            )
            .await?;
            return Ok(());
        }

        if let Some(url) = extract_direct_link(trimmed) {
            if !state.runtime.snapshot().await.features.direct_url_uploads {
                bot.send_message(
                    msg.chat.id,
                    "Direct URL uploads are disabled in config.json.",
                )
                .await?;
                return Ok(());
            }
            let user_id = telegram_user_id(&msg).context("could not identify Telegram user")?;
            if state.storage.uses_oauth() && !state.storage.has_oauth_token(user_id).await {
                let auth_url = state
                    .storage
                    .authorization_url(user_id, msg.chat.id)
                    .await?;
                bot.send_message(
                    msg.chat.id,
                    format!("Please connect Google Drive first:\n{auth_url}"),
                )
                .await?;
                return Ok(());
            }

            let parsed_url = validate_direct_url(url)?;
            bot.send_message(
                msg.chat.id,
                "Fetching direct link and uploading to configured storage…",
            )
            .await?;
            let (name, mime_type, total_size, stream) =
                direct_url_stream(&state.telegram_http, parsed_url).await?;
            let uploaded = state
                .storage
                .upload_stream(user_id, &name, mime_type.as_deref(), total_size, stream)
                .await?;
            let link = uploaded
                .link
                .unwrap_or_else(|| "No public link returned".to_owned());
            let uploaded_size = format_bytes(uploaded.size.unwrap_or(total_size));
            bot.send_message(
                msg.chat.id,
                format!(
                    "Uploaded from URL: {} ({uploaded_size})\n{}",
                    uploaded.name.unwrap_or(name),
                    link
                ),
            )
            .await?;
            return Ok(());
        }
    }

    if !state.runtime.snapshot().await.features.telegram_uploads {
        bot.send_message(
            msg.chat.id,
            "Telegram file uploads are disabled in config.json.",
        )
        .await?;
        return Ok(());
    }

    let Some(incoming) = extract_incoming_file(&msg) else {
        bot.send_message(
            msg.chat.id,
            "Send me a file to upload. Use /auth first when OAuth mode is enabled.",
        )
        .await?;
        return Ok(());
    };
    let user_id = telegram_user_id(&msg).context("could not identify Telegram user")?;
    if state.storage.uses_oauth() && !state.storage.has_oauth_token(user_id).await {
        let url = state
            .storage
            .authorization_url(user_id, msg.chat.id)
            .await?;
        bot.send_message(
            msg.chat.id,
            format!("Please connect Google Drive first:\n{url}"),
        )
        .await?;
        return Ok(());
    }

    bot.send_message(
        msg.chat.id,
        format!("Uploading {} to configured storage…", incoming.name),
    )
    .await?;
    let file = bot
        .get_file(incoming.file_id.clone())
        .await
        .context("failed to get Telegram file metadata")?;
    let total_size = file.size as u64;
    let telegram_stream =
        telegram_file_stream(&state.telegram_http, bot.token(), &file.path).await?;
    let uploaded = state
        .storage
        .upload_stream(
            user_id,
            &incoming.name,
            incoming.mime_type.as_deref(),
            total_size,
            telegram_stream,
        )
        .await?;
    let link = uploaded
        .link
        .unwrap_or_else(|| "No public link returned".to_owned());
    let uploaded_size = format_bytes(uploaded.size.unwrap_or(total_size));
    let uploaded_name = uploaded.name.unwrap_or(incoming.name.clone());
    bot.send_message(
        msg.chat.id,
        format!("Uploaded: {uploaded_name} ({uploaded_size})\n{link}"),
    )
    .await?;
    let runtime_config = state.runtime.snapshot().await;
    if runtime_config.features.telegram_to_bale {
        if let Some(bale) = runtime_config.bale {
            if let Some(target_chat_id) = bale.target_chat_id {
                let telegram_url = format!(
                    "https://api.telegram.org/file/bot{}/{}",
                    bot.token(),
                    file.path
                );
                send_bale_document(
                    &state.telegram_http,
                    &bale.token,
                    target_chat_id,
                    &telegram_url,
                    Some(&uploaded_name),
                )
                .await?;
            }
        }
    }
    Ok(())
}

fn help_text(storage: &StorageClient) -> &'static str {
    match storage {
        StorageClient::GoogleDrive(drive) if drive.uses_oauth() => {
            "Send /auth to connect your Google Drive, then send files or /url <https://...> to upload."
        }
        StorageClient::GoogleDrive(_) => {
            "Send files or /url <https://...> and I will upload them to the configured Google Drive destination."
        }
        StorageClient::B2(_) => {
            "Send files or /url <https://...> and I will upload them to the configured Backblaze B2 bucket."
        }
    }
}

fn telegram_user_id(msg: &Message) -> Option<u64> {
    msg.from.as_ref().map(|user| user.id.0)
}

fn normalize_username(username: &str) -> String {
    username.trim().trim_start_matches('@').to_ascii_lowercase()
}

fn normalized_proxy(value: Option<&String>) -> Option<&str> {
    value.and_then(|proxy| {
        let trimmed = proxy.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}

fn build_http_client(proxy: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true);
    if let Some(proxy_url) = proxy {
        let proxy = reqwest::Proxy::all(proxy_url)
            .with_context(|| format!("invalid proxy URL in config.proxy: {proxy_url}"))?;
        builder = builder.proxy(proxy);
    }
    builder.build().context("failed to build HTTP client")
}

fn build_telegram_api_client(proxy: Option<&str>) -> Result<TelegramApiClient> {
    let mut builder = TelegramApiClient::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true);
    if let Some(proxy_url) = proxy {
        let proxy = TelegramApiProxy::all(proxy_url)
            .with_context(|| format!("invalid proxy URL in config.proxy: {proxy_url}"))?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .context("failed to build Telegram API client")
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.2} {}", UNITS[unit])
    }
}

struct IncomingFile {
    file_id: String,
    name: String,
    mime_type: Option<String>,
}

fn extract_incoming_file(msg: &Message) -> Option<IncomingFile> {
    match &msg.kind {
        MessageKind::Common(common) => match &common.media_kind {
            MediaKind::Document(d) => Some(from_document(&d.document)),
            MediaKind::Video(v) => Some(from_video(&v.video)),
            MediaKind::Audio(a) => Some(IncomingFile {
                file_id: a.audio.file.id.clone(),
                name: a.audio.file_name.clone().unwrap_or_else(|| "audio".into()),
                mime_type: a.audio.mime_type.as_ref().map(ToString::to_string),
            }),
            MediaKind::Voice(v) => Some(IncomingFile {
                file_id: v.voice.file.id.clone(),
                name: "voice.oga".into(),
                mime_type: v.voice.mime_type.as_ref().map(ToString::to_string),
            }),
            MediaKind::Photo(p) => largest_photo(&p.photo).map(|photo| IncomingFile {
                file_id: photo.file.id.clone(),
                name: "photo.jpg".into(),
                mime_type: Some("image/jpeg".into()),
            }),
            _ => None,
        },
        _ => None,
    }
}

fn from_document(doc: &Document) -> IncomingFile {
    IncomingFile {
        file_id: doc.file.id.clone(),
        name: doc.file_name.clone().unwrap_or_else(|| "document".into()),
        mime_type: doc.mime_type.as_ref().map(ToString::to_string),
    }
}

fn from_video(video: &Video) -> IncomingFile {
    IncomingFile {
        file_id: video.file.id.clone(),
        name: video
            .file_name
            .clone()
            .unwrap_or_else(|| "video.mp4".into()),
        mime_type: video.mime_type.as_ref().map(ToString::to_string),
    }
}

fn largest_photo(photos: &[PhotoSize]) -> Option<&PhotoSize> {
    photos.iter().max_by_key(|p| p.file.size)
}

async fn telegram_file_stream(
    client: &Client,
    token: &str,
    path: &str,
) -> Result<impl Stream<Item = reqwest::Result<bytes::Bytes>>> {
    let url = format!("https://api.telegram.org/file/bot{token}/{path}");
    let response = client
        .get(url)
        .send()
        .await
        .context("failed to start Telegram file download")?
        .error_for_status()?;
    Ok(response.bytes_stream())
}

fn extract_direct_link(text: &str) -> Option<&str> {
    let mut parts = text.split_whitespace();
    let first = parts.next()?;
    if is_command(first, "url") || is_command(first, "link") {
        return parts.next();
    }

    if text.starts_with("http://") || text.starts_with("https://") {
        return Some(text);
    }
    None
}

fn is_command(token: &str, command: &str) -> bool {
    token
        .strip_prefix('/')
        .and_then(|value| value.split('@').next())
        .is_some_and(|value| value.eq_ignore_ascii_case(command))
}

fn validate_direct_url(raw: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(raw).context("invalid URL format")?;
    match url.scheme() {
        "http" | "https" => {}
        _ => return Err(anyhow!("URL must use http:// or https://")),
    }
    let host = url.host_str().context("URL host is missing")?;
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return Err(anyhow!("localhost URLs are not allowed"));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if blocked_ip(ip) {
            return Err(anyhow!("private or local IP URLs are not allowed"));
        }
    }
    Ok(url)
}

fn blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            ipv4.is_private()
                || ipv4.is_loopback()
                || ipv4.is_link_local()
                || ipv4.is_multicast()
                || ipv4.is_broadcast()
                || ipv4.is_unspecified()
                || ipv4.is_documentation()
        }
        IpAddr::V6(ipv6) => {
            ipv6.is_loopback()
                || ipv6.is_unspecified()
                || ipv6.is_multicast()
                || ipv6.is_unique_local()
                || ipv6.is_unicast_link_local()
        }
    }
}

async fn direct_url_stream(
    client: &Client,
    url: reqwest::Url,
) -> Result<(
    String,
    Option<String>,
    u64,
    impl Stream<Item = reqwest::Result<bytes::Bytes>>,
)> {
    let response = client
        .get(url.clone())
        .send()
        .await
        .context("failed to start direct-link download")?
        .error_for_status()
        .context("direct-link request failed")?;
    let total_size = response
        .content_length()
        .context("direct link must include Content-Length header")?;
    if total_size == 0 {
        return Err(anyhow!("direct link returned empty content"));
    }
    let mime_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let filename = filename_from_url(&url);
    Ok((filename, mime_type, total_size, response.bytes_stream()))
}

fn filename_from_url(url: &reqwest::Url) -> String {
    url.path_segments()
        .and_then(|segments| segments.last())
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_owned())
        .unwrap_or_else(|| "download.bin".to_owned())
}

fn ensure_enabled_storage(config: &Config, storage: &StorageClient) -> Result<()> {
    match storage {
        StorageClient::GoogleDrive(_) if !config.features.google_drive_uploads => Err(anyhow!(
            "Google Drive storage is configured but disabled in features.google_drive_uploads"
        )),
        StorageClient::B2(_) if !config.features.b2_uploads => Err(anyhow!(
            "Backblaze B2 storage is configured but disabled in features.b2_uploads"
        )),
        _ => Ok(()),
    }
}

async fn send_bale_document(
    client: &Client,
    token: &str,
    chat_id: i64,
    document_url: &str,
    caption: Option<&str>,
) -> Result<()> {
    let url = format!("https://tapi.bale.ai/bot{token}/sendDocument");
    let mut payload = json!({ "chat_id": chat_id, "document": document_url });
    if let Some(caption) = caption {
        payload["caption"] = json!(caption);
    }
    let response = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .context("failed to send Bale document")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!("Bale sendDocument failed: HTTP {status} - {body}"));
    }
    Ok(())
}

async fn run_bale_bot(config: BaleConfig, state: AppState, telegram_token: String) {
    let mut offset = 0_i64;
    loop {
        match poll_bale_once(&config, &state, &telegram_token, &mut offset).await {
            Ok(()) => {}
            Err(err) => error!(error = ?err, "Bale polling failed"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Debug, Deserialize)]
struct BaleUpdates {
    ok: bool,
    result: Vec<BaleUpdate>,
}
#[derive(Debug, Deserialize)]
struct BaleUpdate {
    update_id: i64,
    message: Option<BaleMessage>,
}
#[derive(Debug, Deserialize)]
struct BaleMessage {
    chat: BaleChat,
    text: Option<String>,
}
#[derive(Debug, Deserialize)]
struct BaleChat {
    id: i64,
}

async fn poll_bale_once(
    config: &BaleConfig,
    state: &AppState,
    telegram_token: &str,
    offset: &mut i64,
) -> Result<()> {
    let url = format!("https://tapi.bale.ai/bot{}/getUpdates", config.token);
    let updates: BaleUpdates = state
        .telegram_http
        .get(url)
        .query(&[("offset", *offset), ("timeout", 20)])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if !updates.ok {
        return Err(anyhow!("Bale getUpdates returned ok=false"));
    }
    for update in updates.result {
        *offset = update.update_id + 1;
        let Some(message) = update.message else {
            continue;
        };
        let Some(text) = message.text.as_deref().and_then(extract_direct_link) else {
            continue;
        };
        let runtime = state.runtime.snapshot().await;
        if runtime.features.bale_to_telegram {
            if let Some(target) = runtime.bale.as_ref().and_then(|b| b.target_chat_id) {
                send_telegram_document(&state.telegram_http, telegram_token, target, text).await?;
            }
        } else if runtime.features.direct_url_uploads {
            let parsed_url = validate_direct_url(text)?;
            let (name, mime_type, total_size, stream) =
                direct_url_stream(&state.telegram_http, parsed_url).await?;
            state
                .storage
                .upload_stream(
                    message.chat.id as u64,
                    &name,
                    mime_type.as_deref(),
                    total_size,
                    stream,
                )
                .await?;
        }
    }
    Ok(())
}

async fn send_telegram_document(
    client: &Client,
    token: &str,
    chat_id: i64,
    document_url: &str,
) -> Result<()> {
    let url = format!("https://api.telegram.org/bot{token}/sendDocument");
    let response = client
        .post(url)
        .json(&json!({ "chat_id": chat_id, "document": document_url }))
        .send()
        .await
        .context("failed to send Telegram document")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "Telegram sendDocument failed: HTTP {status} - {body}"
        ));
    }
    Ok(())
}

impl DriveClient {
    async fn from_config(http: Client, config: &Config) -> Result<Self> {
        let folder_id = config
            .google_drive_folder_id
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned();
        let google = config
            .google
            .as_ref()
            .context("Google Drive storage requires google config")?;
        let GoogleConfig::OAuth {
            client_id,
            client_secret,
            redirect_uri,
            tokens_file,
        } = google;
        let tokens = load_oauth_tokens(tokens_file).await?;
        let auth = DriveAuth::OAuth {
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
            redirect_uri: redirect_uri.clone(),
            tokens_path: tokens_file.clone(),
            tokens: Mutex::new(tokens),
            pending_states: Mutex::new(HashMap::new()),
        };

        Ok(Self {
            http,
            folder_id,
            auth,
        })
    }

    fn uses_oauth(&self) -> bool {
        true
    }

    async fn has_oauth_token(&self, telegram_user_id: u64) -> bool {
        match &self.auth {
            DriveAuth::OAuth { tokens, .. } => tokens.lock().await.contains_key(&telegram_user_id),
        }
    }

    async fn authorization_url(&self, telegram_user_id: u64, chat_id: ChatId) -> Result<String> {
        let DriveAuth::OAuth {
            client_id,
            redirect_uri,
            pending_states,
            ..
        } = &self.auth;
        let state = Uuid::new_v4().to_string();
        pending_states.lock().await.insert(
            state.clone(),
            PendingOAuth {
                telegram_user_id,
                chat_id,
            },
        );
        Ok(format!(
            "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope={}&access_type=offline&prompt=consent&state={}",
            urlencoding::encode(client_id),
            urlencoding::encode(redirect_uri),
            urlencoding::encode(DRIVE_SCOPE),
            urlencoding::encode(&state)
        ))
    }

    async fn complete_oauth(&self, state: &str, code: &str) -> Result<PendingOAuth> {
        let DriveAuth::OAuth {
            client_id,
            client_secret,
            redirect_uri,
            tokens_path,
            tokens,
            pending_states,
        } = &self.auth;
        let pending = pending_states
            .lock()
            .await
            .remove(state)
            .context("unknown or expired OAuth state")?;
        let response: TokenResponse = self
            .http
            .post(TOKEN_URL)
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("code", code),
                ("grant_type", "authorization_code"),
                ("redirect_uri", redirect_uri.as_str()),
            ])
            .send()
            .await
            .context("failed to exchange OAuth code")?
            .error_for_status()?
            .json()
            .await
            .context("failed to parse OAuth token response")?;
        let refresh_token = response.refresh_token.context(
            "Google did not return a refresh token; revoke app access and run /auth again",
        )?;
        let token = PersistedOAuthToken {
            access_token: response.access_token,
            refresh_token,
            expires_at_unix: unix_now() + response.expires_in,
        };
        let mut guard = tokens.lock().await;
        guard.insert(pending.telegram_user_id, token);
        save_oauth_tokens(tokens_path, &guard).await?;
        Ok(pending)
    }

    async fn upload_stream<S>(
        &self,
        telegram_user_id: u64,
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
        mut stream: S,
    ) -> Result<DriveFile>
    where
        S: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    {
        let token = self.access_token(telegram_user_id).await?;
        let session = self
            .create_resumable_session(&token, name, mime_type, total_size)
            .await?;
        let mut offset = 0_u64;
        let mut buffer = Vec::with_capacity(CHUNK_SIZE as usize);

        while let Some(chunk) = stream.next().await {
            buffer.extend_from_slice(&chunk.context("Telegram download stream failed")?);
            while buffer.len() as u64 >= CHUNK_SIZE {
                let upload = buffer.drain(..CHUNK_SIZE as usize).collect::<Vec<_>>();
                self.put_chunk(&session, offset, total_size, upload).await?;
                offset += CHUNK_SIZE;
            }
        }

        let final_chunk = std::mem::take(&mut buffer);
        self.put_final_chunk(&session, offset, total_size, final_chunk)
            .await
    }

    async fn create_resumable_session(
        &self,
        token: &str,
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
    ) -> Result<String> {
        let mut metadata = json!({ "name": name });
        if let Some(folder_id) = &self.folder_id {
            metadata["parents"] = json!([folder_id]);
        }
        let response = self
            .http
            .post(RESUMABLE_UPLOAD_URL)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, "application/json; charset=UTF-8")
            .header("X-Upload-Content-Length", total_size)
            .header(
                "X-Upload-Content-Type",
                mime_type.unwrap_or("application/octet-stream"),
            )
            .json(&metadata)
            .send()
            .await
            .context("failed to create Drive resumable upload session")?;

        let status = response.status();
        if !status.is_success() {
            let err_text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "failed to create Drive resumable upload session: HTTP {status} - {err_text}"
            ));
        }

        response
            .headers()
            .get(header::LOCATION)
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("Drive did not return an upload session URL"))
    }

    async fn put_chunk(&self, session: &str, start: u64, total: u64, bytes: Vec<u8>) -> Result<()> {
        let end = start + bytes.len() as u64 - 1;
        let response = self
            .http
            .put(session)
            .header(header::CONTENT_LENGTH, bytes.len())
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}"),
            )
            .body(bytes)
            .send()
            .await?;
        let status = response.status();
        if status == StatusCode::PERMANENT_REDIRECT
            || status == StatusCode::OK
            || status == StatusCode::CREATED
        {
            Ok(())
        } else {
            let err_text = response.text().await.unwrap_or_default();
            Err(anyhow!(
                "Drive chunk upload failed with status {status} - {err_text}"
            ))
        }
    }

    async fn put_final_chunk(
        &self,
        session: &str,
        start: u64,
        total: u64,
        bytes: Vec<u8>,
    ) -> Result<DriveFile> {
        let response = if bytes.is_empty() && total == start {
            self.http
                .put(session)
                .header(header::CONTENT_LENGTH, 0)
                .header(header::CONTENT_RANGE, format!("bytes */{total}"))
                .send()
                .await?
        } else {
            let end = start + bytes.len() as u64 - 1;
            self.http
                .put(session)
                .header(header::CONTENT_LENGTH, bytes.len())
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{total}"),
                )
                .body(bytes)
                .send()
                .await?
        };

        let status = response.status();
        if !status.is_success() {
            let err_text = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "Drive final chunk upload failed: HTTP {status} - {err_text}"
            ));
        }

        response
            .json()
            .await
            .context("failed to parse Drive upload response")
    }

    async fn access_token(&self, telegram_user_id: u64) -> Result<String> {
        let DriveAuth::OAuth {
            tokens_path,
            tokens,
            ..
        } = &self.auth;
        let current = tokens
            .lock()
            .await
            .get(&telegram_user_id)
            .cloned()
            .context("run /auth to connect Google Drive first")?;
        if current.expires_at_unix > unix_now() + 60 {
            return Ok(current.access_token);
        }
        let refreshed = self
            .refresh_oauth_access_token(&current.refresh_token)
            .await?;
        let value = refreshed.access_token.clone();
        let mut guard = tokens.lock().await;
        guard.insert(telegram_user_id, refreshed);
        save_oauth_tokens(tokens_path, &guard).await?;
        Ok(value)
    }

    async fn refresh_oauth_access_token(&self, refresh_token: &str) -> Result<PersistedOAuthToken> {
        let DriveAuth::OAuth {
            client_id,
            client_secret,
            ..
        } = &self.auth;
        let response: TokenResponse = self
            .http
            .post(TOKEN_URL)
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("refresh_token", refresh_token),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .await
            .context("failed to refresh OAuth token")?
            .error_for_status()?
            .json()
            .await
            .context("failed to parse OAuth refresh response")?;
        Ok(PersistedOAuthToken {
            access_token: response.access_token,
            refresh_token: response
                .refresh_token
                .unwrap_or_else(|| refresh_token.to_owned()),
            expires_at_unix: unix_now() + response.expires_in,
        })
    }
}

async fn load_oauth_tokens(path: &PathBuf) -> Result<HashMap<u64, PersistedOAuthToken>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).context("invalid OAuth token store"),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(err) => Err(err).context("failed to read OAuth token store"),
    }
}

async fn save_oauth_tokens(
    path: &PathBuf,
    tokens: &HashMap<u64, PersistedOAuthToken>,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(tokens)?;
    tokio::fs::write(path, bytes)
        .await
        .context("failed to write OAuth token store")
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
