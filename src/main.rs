use std::{
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{Stream, StreamExt};
use reqwest::{header, Client, StatusCode};
use reqwest_011::{Client as TelegramApiClient, Proxy as TelegramApiProxy};
use ring::signature::RsaKeyPair;
use serde::{Deserialize, Serialize};
use serde_json::json;
use teloxide::{
    prelude::*,
    types::{Document, MediaKind, MessageKind, PhotoSize, Video},
};
use tokio::sync::Mutex;
use tracing::{error, info};
use uuid::Uuid;

const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive.file";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const RESUMABLE_UPLOAD_URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name,size,webViewLink";
const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // Google resumable uploads require multiples of 256 KiB.

#[derive(Clone)]
struct AppState {
    telegram_http: Client,
    drive: Arc<DriveClient>,
    access: Arc<AccessControl>,
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    telegram_token: String,
    #[serde(default)]
    allowed_telegram_usernames: Vec<String>,
    #[serde(default)]
    google_drive_folder_id: Option<String>,
    google: GoogleConfig,
    #[serde(default)]
    oauth_server: OAuthServerConfig,
    #[serde(default)]
    proxy: ProxyConfig,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ProxyConfig {
    #[serde(default)]
    all: Option<String>,
    #[serde(default)]
    telegram_receive: Option<String>,
    #[serde(default)]
    google_drive: Option<String>,
}

impl ProxyConfig {
    fn telegram_receive(&self) -> Option<&str> {
        normalized_proxy(self.telegram_receive.as_ref())
            .or_else(|| normalized_proxy(self.all.as_ref()))
    }

    fn google_drive(&self) -> Option<&str> {
        normalized_proxy(self.google_drive.as_ref()).or_else(|| normalized_proxy(self.all.as_ref()))
    }
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
    ServiceAccount {
        #[serde(default)]
        json_file: Option<PathBuf>,
        #[serde(default)]
        json: Option<String>,
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

async fn load_config() -> Result<Config> {
    let path = env::args()
        .nth(1)
        .or_else(|| env::var("CONFIG_PATH").ok())
        .unwrap_or_else(|| "config.json".to_owned());
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("failed to read config file at {path}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("invalid JSON config at {path}"))
}

#[derive(Debug)]
struct AccessControl {
    allowed_usernames: HashSet<String>,
}

impl AccessControl {
    fn from_config(config: &Config) -> Self {
        let allowed_usernames = config
            .allowed_telegram_usernames
            .iter()
            .map(|username| normalize_username(username))
            .filter(|username| !username.is_empty())
            .collect();
        Self { allowed_usernames }
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

#[derive(Debug, Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

fn default_token_uri() -> String {
    TOKEN_URL.to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedOAuthToken {
    access_token: String,
    refresh_token: String,
    expires_at_unix: u64,
}

#[derive(Debug)]
struct AccessToken {
    value: String,
    expires_at: Instant,
}

#[derive(Debug)]
enum DriveAuth {
    ServiceAccount {
        key: ServiceAccountKey,
        token: Mutex<Option<AccessToken>>,
    },
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

#[derive(Debug, Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
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
    let config = load_config().await?;
    let telegram_http = build_http_client(config.proxy.telegram_receive())?;
    let telegram_api_http = build_telegram_api_client(config.proxy.telegram_receive())?;
    let drive_http = build_http_client(config.proxy.google_drive())?;
    let bot = Bot::with_client(config.telegram_token.clone(), telegram_api_http);
    let drive = Arc::new(DriveClient::from_config(drive_http, &config).await?);

    if drive.uses_oauth() {
        spawn_oauth_server(bot.clone(), drive.clone(), config.oauth_server.bind_addr).await?;
    }

    let state = AppState {
        telegram_http,
        drive,
        access: Arc::new(AccessControl::from_config(&config)),
    };

    info!("starting Telegram → Google Drive uploader bot");
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
        if text == "/start" || text == "/help" {
            bot.send_message(msg.chat.id, help_text(state.drive.uses_oauth()))
                .await?;
            return Ok(());
        }
        if text == "/auth" {
            let user_id = telegram_user_id(&msg).context("could not identify Telegram user")?;
            let url = state.drive.authorization_url(user_id, msg.chat.id).await?;
            bot.send_message(
                msg.chat.id,
                format!("Open this URL to connect Google Drive:\n{url}"),
            )
            .await?;
            return Ok(());
        }
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
    if state.drive.uses_oauth() && !state.drive.has_oauth_token(user_id).await {
        let url = state.drive.authorization_url(user_id, msg.chat.id).await?;
        bot.send_message(
            msg.chat.id,
            format!("Please connect Google Drive first:\n{url}"),
        )
        .await?;
        return Ok(());
    }

    bot.send_message(
        msg.chat.id,
        format!("Uploading {} to Google Drive…", incoming.name),
    )
    .await?;
    let file = bot
        .get_file(incoming.file_id.clone())
        .await
        .context("failed to get Telegram file metadata")?;
    let total_size = file.size as u64;
    let telegram_stream = telegram_file_stream(&state.telegram_http, bot.token(), &file.path).await?;
    let uploaded = state
        .drive
        .upload_stream(
            user_id,
            &incoming.name,
            incoming.mime_type.as_deref(),
            total_size,
            telegram_stream,
        )
        .await?;
    let link = uploaded
        .web_view_link
        .unwrap_or_else(|| format!("https://drive.google.com/file/d/{}/view", uploaded.id));
    let uploaded_size = uploaded
        .size
        .as_deref()
        .and_then(|size| size.parse::<u64>().ok())
        .map(format_bytes)
        .unwrap_or_else(|| format_bytes(total_size));
    bot.send_message(
        msg.chat.id,
        format!(
            "Uploaded: {} ({uploaded_size})\n{}",
            uploaded.name.unwrap_or(incoming.name),
            link
        ),
    )
    .await?;
    Ok(())
}

fn help_text(oauth_enabled: bool) -> &'static str {
    if oauth_enabled {
        "Send /auth to connect your Google Drive, then send me files to upload."
    } else {
        "Send me files and I will upload them to the configured Google Drive service-account destination."
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

impl DriveClient {
    async fn from_config(http: Client, config: &Config) -> Result<Self> {
        let folder_id = config
            .google_drive_folder_id
            .as_ref()
            .filter(|s| !s.is_empty())
            .cloned();
        let auth = match &config.google {
            GoogleConfig::OAuth {
                client_id,
                client_secret,
                redirect_uri,
                tokens_file,
            } => {
                let tokens = load_oauth_tokens(tokens_file).await?;
                DriveAuth::OAuth {
                    client_id: client_id.clone(),
                    client_secret: client_secret.clone(),
                    redirect_uri: redirect_uri.clone(),
                    tokens_path: tokens_file.clone(),
                    tokens: Mutex::new(tokens),
                    pending_states: Mutex::new(HashMap::new()),
                }
            }
            GoogleConfig::ServiceAccount { json_file, json } => {
                let key_json = match (json, json_file) {
                    (Some(raw), _) if !raw.is_empty() => raw.clone(),
                    (_, Some(path)) => {
                        tokio::fs::read_to_string(path).await.with_context(|| {
                            format!("failed to read service-account JSON at {}", path.display())
                        })?
                    }
                    _ => {
                        return Err(anyhow!(
                            "service_account mode requires google.json_file or google.json"
                        ))
                    }
                };
                DriveAuth::ServiceAccount {
                    key: serde_json::from_str(&key_json)
                        .context("invalid Google service-account JSON")?,
                    token: Mutex::new(None),
                }
            }
        };

        Ok(Self {
            http,
            folder_id,
            auth,
        })
    }

    fn uses_oauth(&self) -> bool {
        matches!(self.auth, DriveAuth::OAuth { .. })
    }

    async fn has_oauth_token(&self, telegram_user_id: u64) -> bool {
        match &self.auth {
            DriveAuth::OAuth { tokens, .. } => tokens.lock().await.contains_key(&telegram_user_id),
            DriveAuth::ServiceAccount { .. } => true,
        }
    }

    async fn authorization_url(&self, telegram_user_id: u64, chat_id: ChatId) -> Result<String> {
        let DriveAuth::OAuth {
            client_id,
            redirect_uri,
            pending_states,
            ..
        } = &self.auth
        else {
            return Err(anyhow!("OAuth is not enabled for this bot"));
        };
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
        } = &self.auth
        else {
            return Err(anyhow!("OAuth is not enabled for this bot"));
        };
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
            .context("failed to create Drive resumable upload session")?
            .error_for_status()?;
        response
            .headers()
            .get(header::LOCATION)
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("Drive did not return an upload session URL"))
    }

    async fn put_chunk(&self, session: &str, start: u64, total: u64, bytes: Vec<u8>) -> Result<()> {
        let end = start + bytes.len() as u64 - 1;
        let status = self
            .http
            .put(session)
            .header(header::CONTENT_LENGTH, bytes.len())
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}"),
            )
            .body(bytes)
            .send()
            .await?
            .status();
        if status == StatusCode::PERMANENT_REDIRECT
            || status == StatusCode::OK
            || status == StatusCode::CREATED
        {
            Ok(())
        } else {
            Err(anyhow!("Drive chunk upload failed with status {status}"))
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
        response
            .error_for_status()?
            .json()
            .await
            .context("failed to parse Drive upload response")
    }

    async fn access_token(&self, telegram_user_id: u64) -> Result<String> {
        match &self.auth {
            DriveAuth::ServiceAccount { key, token } => {
                let now = Instant::now();
                if let Some(token) = token.lock().await.as_ref() {
                    if token.expires_at > now + Duration::from_secs(60) {
                        return Ok(token.value.clone());
                    }
                }
                let fresh = self.fetch_service_account_access_token(key).await?;
                let value = fresh.value.clone();
                *token.lock().await = Some(fresh);
                Ok(value)
            }
            DriveAuth::OAuth {
                tokens_path,
                tokens,
                ..
            } => {
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
        }
    }

    async fn fetch_service_account_access_token(
        &self,
        key: &ServiceAccountKey,
    ) -> Result<AccessToken> {
        let assertion = self.signed_jwt(key)?;
        let response: TokenResponse = self
            .http
            .post(&key.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .await
            .context("failed to request Google access token")?
            .error_for_status()?
            .json()
            .await
            .context("failed to parse Google token response")?;
        Ok(AccessToken {
            value: response.access_token,
            expires_at: Instant::now() + Duration::from_secs(response.expires_in),
        })
    }

    async fn refresh_oauth_access_token(&self, refresh_token: &str) -> Result<PersistedOAuthToken> {
        let DriveAuth::OAuth {
            client_id,
            client_secret,
            ..
        } = &self.auth
        else {
            return Err(anyhow!("OAuth is not enabled for this bot"));
        };
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

    fn signed_jwt(&self, key: &ServiceAccountKey) -> Result<String> {
        let iat = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let header = json!({ "alg": "RS256", "typ": "JWT" });
        let claims = Claims {
            iss: &key.client_email,
            scope: DRIVE_SCOPE,
            aud: &key.token_uri,
            iat,
            exp: iat + 3600,
        };
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?)
        );
        let der = pem_to_der(&key.private_key)?;
        let key_pair = RsaKeyPair::from_pkcs8(&der)
            .map_err(|_| anyhow!("invalid service-account private key"))?;
        let mut signature = vec![0; key_pair.public().modulus_len()];
        key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &ring::rand::SystemRandom::new(),
                signing_input.as_bytes(),
                &mut signature,
            )
            .map_err(|_| anyhow!("failed to sign Google JWT"))?;
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature)
        ))
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

fn pem_to_der(pem: &str) -> Result<Vec<u8>> {
    let body = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .context("failed to decode PEM private key")
}
