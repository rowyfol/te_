# Telegram / Bale → cloud storage uploader bot

A resource-efficient Rust bot that streams files from Telegram, Bale, or direct HTTP/HTTPS links to Google Drive OAuth or Backblaze B2 without staging whole files on your VPS disk.

## Features

- One Linux executable plus one JSON config file for deployment.
- Feature switches in `config.json` let you enable/disable Telegram uploads, direct URL uploads, Google Drive, B2, Bale, and cross-messenger transfers.
- Admin Telegram users can run `/reload` to apply runtime feature-toggle changes from `config.json` without restarting the process.
- Accepts Telegram documents, videos, audio, voice messages, photos, and direct links via `/url <link>`.
- Uploads to Google Drive with per-user OAuth resumable uploads, or streams directly to Backblaze B2 single-file upload.
- Generates Backblaze B2 friendly download URLs with the configured bucket name, for example `https://f003.backblazeb2.com/file/ashbox/telegram-uploads/Mohan+-+Power+Electronics.pdf`.
- Optional Bale bot support can transfer Telegram files to Bale by URL and can forward Bale direct links to Telegram or upload them to the configured storage.
- Supports per-service HTTP proxies.

## Known platform limits

Telegram Bot API can return `Bad Request: file is too big` before this program can download the file. That limit is enforced by Telegram for bot file metadata/downloads, not by this uploader. For oversized files, send a direct HTTP/HTTPS link with `/url <link>` so the bot can stream from the original host instead.

Transient `UnexpectedEof` or TLS handshake EOF errors from Telegram/B2 are network/provider connection failures. Configure `proxy.telegram_receive` or `proxy.b2` if your server has unreliable routes to those services.

## Configuration file

The bot reads `config.json` by default. You can pass another path as the first CLI argument or set `CONFIG_PATH`:

```bash
./telegram-drive-uploader /etc/telegram-drive-uploader/config.json
# or
CONFIG_PATH=/etc/telegram-drive-uploader/config.json ./telegram-drive-uploader
```

Start from one of the included templates:

- `config.example.json` for Google Drive per-user OAuth mode.
- `config.b2.example.json` for Backblaze B2 mode.

### Common JSON fields

```json
{
  "telegram_token": "123456:telegram-token",
  "allowed_telegram_usernames": ["alice", "@bob"],
  "admin_telegram_usernames": ["alice"],
  "features": {
    "telegram_bot": true,
    "telegram_uploads": true,
    "direct_url_uploads": true,
    "google_drive_uploads": true,
    "b2_uploads": true,
    "bale_bot": false,
    "telegram_to_bale": false,
    "bale_to_telegram": false,
    "admin_config_reload": true
  },
  "bale": {
    "token": "123456:bale-token",
    "target_chat_id": 123456789
  },
  "proxy": {
    "all": "socks5h://127.0.0.1:1080",
    "telegram_receive": "",
    "google_drive": "",
    "b2": ""
  }
}
```

- `allowed_telegram_usernames` is optional. If it is non-empty, other Telegram users are ignored.
- `admin_telegram_usernames` can run `/config` and `/reload`.
- `/reload` applies feature-toggle changes to the running process. Restart the program to change bot tokens, storage credentials, proxies, OAuth server binding, or allow-lists.
- `storage.provider` is `google_drive` or `b2`.
- Empty proxy strings are ignored.

## Google Drive OAuth mode

Use this mode when each allowed Telegram user should connect their own Google Drive account through a browser consent page. Service-account mode has been removed; OAuth is the supported Google Drive mode.

```json
{
  "telegram_token": "123456:telegram-token",
  "allowed_telegram_usernames": ["alice", "@bob"],
  "oauth_server": {
    "bind_addr": "0.0.0.0:8080"
  },
  "storage": {
    "provider": "google_drive",
    "folder_id": "optional-folder-id",
    "google": {
      "mode": "oauth",
      "client_id": "google-client-id.apps.googleusercontent.com",
      "client_secret": "google-client-secret",
      "redirect_uri": "https://your-domain.example/oauth2/callback",
      "tokens_file": "./google-oauth-tokens.json"
    }
  }
}
```

Run the bot and tell each allowed user to send `/auth`. The bot stores refresh tokens by Telegram user id in `tokens_file`.

## Backblaze B2 mode

```json
{
  "telegram_token": "123456:telegram-token",
  "allowed_telegram_usernames": ["alice", "@bob"],
  "storage": {
    "provider": "b2",
    "key_id": "backblaze-key-id",
    "application_key": "backblaze-application-key",
    "bucket_id": "backblaze-bucket-id",
    "bucket_name": "ashbox",
    "file_prefix": "telegram-uploads"
  }
}
```

Set `bucket_name` to the human bucket name shown in Backblaze. The bot still uses `bucket_id` for the B2 API, but download links use `bucket_name`.

## Bale support

Enable `features.bale_bot` and provide `bale.token`. The Bale integration uses URL forwarding where possible so files do not need to be stored on your VPS:

- `telegram_to_bale`: after a Telegram upload succeeds, the bot sends Bale a Telegram file URL.
- `bale_to_telegram`: Bale direct-link messages are forwarded to Telegram with `sendDocument`.
- If `bale_to_telegram` is disabled and `direct_url_uploads` is enabled, Bale direct-link messages are streamed into the configured storage.

The `target_chat_id` is used as the destination chat for cross-messenger transfers.

## Run locally

```bash
cp config.example.json config.json
# edit config.json
cargo run --release -- config.json
```

Send `/help` to see mode-specific instructions. In Google Drive OAuth mode, send `/auth` before sending files or `/url <link>`.

## GitHub Actions release workflow

The workflow at `.github/workflows/release.yml` builds `x86_64-unknown-linux-gnu` with `cargo build --release --locked`, packages the binary plus example configs, uploads a workflow artifact, and publishes the archive on tag pushes matching `v*`.

```bash
git tag v0.1.0
git push origin v0.1.0
```
