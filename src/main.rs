use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use futures_util::{Stream, StreamExt};
use reqwest::{header, Client, StatusCode};
use ring::signature::RsaKeyPair;
use serde::{Deserialize, Serialize};
use serde_json::json;
use teloxide::{
    prelude::*,
    types::{Document, MediaKind, MessageKind, PhotoSize, Video},
};
use tokio::sync::Mutex;
use tracing::{error, info};

const DRIVE_SCOPE: &str = "https://www.googleapis.com/auth/drive.file";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const RESUMABLE_UPLOAD_URL: &str = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&fields=id,name,size,webViewLink";
const CHUNK_SIZE: u64 = 8 * 1024 * 1024; // Google resumable uploads require multiples of 256 KiB.

#[derive(Clone)]
struct AppState {
    http: Client,
    drive: Arc<DriveClient>,
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

#[derive(Debug)]
struct AccessToken {
    value: String,
    expires_at: Instant,
}

#[derive(Debug)]
struct DriveClient {
    http: Client,
    key: ServiceAccountKey,
    folder_id: Option<String>,
    token: Mutex<Option<AccessToken>>,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
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
    let bot = Bot::from_env();
    let http = Client::builder()
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
        .build()?;
    let drive = Arc::new(DriveClient::from_env(http.clone())?);
    let state = AppState { http, drive };

    info!("starting Telegram → Google Drive uploader bot");
    teloxide::repl(bot, move |bot: Bot, msg: Message| {
        let state = state.clone();
        async move {
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

async fn handle_message(bot: Bot, msg: Message, state: AppState) -> Result<()> {
    let Some(incoming) = extract_incoming_file(&msg) else {
        bot.send_message(msg.chat.id, "Send me a document, video, audio, voice, photo, or other file and I will stream it to Google Drive.").await?;
        return Ok(());
    };

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
    let telegram_stream = telegram_file_stream(&state.http, bot.token(), &file.path).await?;
    let uploaded = state
        .drive
        .upload_stream(
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
    fn from_env(http: Client) -> Result<Self> {
        let key_json = match env::var("GOOGLE_SERVICE_ACCOUNT_JSON") {
            Ok(raw) => raw,
            Err(_) => std::fs::read_to_string(
                env::var("GOOGLE_SERVICE_ACCOUNT_FILE")
                    .context("set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE")?,
            )?,
        };
        Ok(Self {
            http,
            key: serde_json::from_str(&key_json).context("invalid Google service-account JSON")?,
            folder_id: env::var("GOOGLE_DRIVE_FOLDER_ID")
                .ok()
                .filter(|s| !s.is_empty()),
            token: Mutex::new(None),
        })
    }

    async fn upload_stream<S>(
        &self,
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
        mut stream: S,
    ) -> Result<DriveFile>
    where
        S: Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    {
        let session = self
            .create_resumable_session(name, mime_type, total_size)
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
        name: &str,
        mime_type: Option<&str>,
        total_size: u64,
    ) -> Result<String> {
        let token = self.access_token().await?;
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

    async fn access_token(&self) -> Result<String> {
        let now = Instant::now();
        if let Some(token) = self.token.lock().await.as_ref() {
            if token.expires_at > now + Duration::from_secs(60) {
                return Ok(token.value.clone());
            }
        }
        let fresh = self.fetch_access_token().await?;
        let value = fresh.value.clone();
        *self.token.lock().await = Some(fresh);
        Ok(value)
    }

    async fn fetch_access_token(&self) -> Result<AccessToken> {
        let assertion = self.signed_jwt()?;
        let response: TokenResponse = self
            .http
            .post(&self.key.token_uri)
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

    fn signed_jwt(&self) -> Result<String> {
        let iat = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let header = json!({ "alg": "RS256", "typ": "JWT" });
        let claims = Claims {
            iss: &self.key.client_email,
            scope: DRIVE_SCOPE,
            aud: &self.key.token_uri,
            iat,
            exp: iat + 3600,
        };
        let signing_input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?)
        );
        let der = pem_to_der(&self.key.private_key)?;
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

fn pem_to_der(pem: &str) -> Result<Vec<u8>> {
    let body = pem
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<String>();
    base64::engine::general_purpose::STANDARD
        .decode(body)
        .context("failed to decode PEM private key")
}
