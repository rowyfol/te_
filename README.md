# Telegram → Google Drive uploader bot

A resource-efficient Telegram bot written with [`teloxide`](https://github.com/teloxide/teloxide) that streams incoming files to Google Drive with Google Drive resumable uploads.

## Features

- One constant Linux executable plus one JSON config file for deployment.
- Accepts documents, videos, audio, voice messages, and photos.
- Downloads from Telegram as an async byte stream.
- Uploads to Google Drive in 8 MiB resumable chunks, so files are never fully buffered in RAM or written to disk.
- Reuses one `reqwest::Client` for connection pooling.
- Supports two Google Drive authentication modes:
  - **Per-user OAuth:** each allowed Telegram user runs `/auth`, opens a Google consent page, and uploads go to that user's Drive.
  - **Service account:** uploads go to a shared-drive folder or (with domain-wide delegation) a delegated Workspace user's Drive.
- Supports multiple Telegram users with separate OAuth token storage.
- Supports an optional Telegram username allow-list; disallowed users are ignored completely.
- Supports an optional destination Drive folder.
- Includes a GitHub Actions workflow that builds and publishes a Linux AMD64 binary archive.

## Configuration file

The bot reads `config.json` by default. You can pass another path as the first CLI argument or set `CONFIG_PATH`:

```bash
./telegram-drive-uploader /etc/telegram-drive-uploader/config.json
# or
CONFIG_PATH=/etc/telegram-drive-uploader/config.json ./telegram-drive-uploader
```

Start from one of the included templates:

- `config.example.json` for per-user OAuth mode.
- `config.service-account.example.json` for service-account mode.

### Common JSON fields

```json
{
  "telegram_token": "123456:telegram-token",
  "allowed_telegram_usernames": ["alice", "@bob"],
  "google_drive_folder_id": "optional-folder-id",
  "proxy": {
    "all": "socks5h://127.0.0.1:1080",
    "telegram_receive": "",
    "google_drive": ""
  }
}
```

- `telegram_token` is required.
- `allowed_telegram_usernames` is optional. If the list is non-empty, anyone not in the list is ignored without a reply. Usernames are case-insensitive and may include or omit `@`.
- `google_drive_folder_id` is optional.
- `proxy` is optional:
  - `proxy.all` applies one proxy URL to all HTTP traffic.
  - `proxy.telegram_receive` overrides proxy only for Telegram receive/download flow.
  - `proxy.google_drive` overrides proxy only for Google Drive upload/auth flow.
  - Empty strings are ignored.

## Option A: per-user OAuth consent flow

Use this mode when multiple users should connect their own Google Drive accounts through a browser consent page.

1. In Google Cloud Console, enable the Google Drive API and create an OAuth client for a web application.

2. Add your public callback URL to the OAuth client's authorized redirect URIs. The bot serves this endpoint:

   ```text
   https://your-domain.example/oauth2/callback
   ```

3. Configure OAuth in JSON:

   ```json
   {
     "telegram_token": "123456:telegram-token",
     "allowed_telegram_usernames": ["alice", "@bob"],
     "google_drive_folder_id": "optional-folder-id",
     "oauth_server": {
       "bind_addr": "0.0.0.0:8080"
     },
     "google": {
       "mode": "oauth",
       "client_id": "google-client-id.apps.googleusercontent.com",
       "client_secret": "google-client-secret",
       "redirect_uri": "https://your-domain.example/oauth2/callback",
       "tokens_file": "./google-oauth-tokens.json"
     }
   }
   ```

4. Run the bot and tell each allowed user to send `/auth`. The bot replies with a Google consent URL. After consent, tokens are stored by Telegram user id in `tokens_file`, so each user's files upload to their own Drive.

## Option B: service-account mode

Use this mode when uploads should use one service account identity.

1. Create a Google Cloud service account, enable the Google Drive API, and download the service-account JSON key.

2. Choose one destination strategy:
   - **Shared Drive folder (recommended):** create or choose a folder in a Shared Drive and share it with the service account `client_email`, then set `google_drive_folder_id`.
   - **Workspace domain-wide delegation:** configure domain-wide delegation for the service account and set `google.delegated_user` to a user in your Workspace domain.

3. Configure service-account mode in JSON:

   ```json
   {
     "telegram_token": "123456:telegram-token",
     "allowed_telegram_usernames": ["alice", "@bob"],
     "google_drive_folder_id": "shared-drive-folder-id",
     "google": {
       "mode": "service_account",
       "json_file": "/secure/path/service-account.json",
       "delegated_user": "optional-user@your-workspace-domain.com"
     }
   }
   ```

   You can also use a `json` string field instead of `json_file`, but `json_file` is recommended so the main bot config stays readable. If you do not set `google.delegated_user`, set `google_drive_folder_id` to a Shared Drive folder ID.

## Run locally

```bash
cp config.example.json config.json
# edit config.json
cargo run --release -- config.json
```

Send `/help` to see the mode-specific instructions. In OAuth mode, send `/auth` before sending files. In service-account mode, send files directly.

## Deploy as one executable plus one config file

Download the Linux AMD64 release archive from GitHub Releases, then place the binary and config wherever you want:

```bash
tar -xzf telegram-drive-uploader-linux-amd64.tar.gz
sudo install -m 0755 telegram-drive-uploader-linux-amd64 /usr/local/bin/telegram-drive-uploader
sudo mkdir -p /etc/telegram-drive-uploader
sudo cp config.example.json /etc/telegram-drive-uploader/config.json
sudoedit /etc/telegram-drive-uploader/config.json
telegram-drive-uploader /etc/telegram-drive-uploader/config.json
```

For OAuth mode, make sure your reverse proxy forwards the public `GOOGLE_OAUTH_REDIRECT_URI` equivalent from the JSON config to the configured `oauth_server.bind_addr`.

## GitHub Actions release workflow

The workflow at `.github/workflows/release.yml` builds `x86_64-unknown-linux-gnu` with `cargo build --release --locked`, packages the binary plus example configs, uploads a workflow artifact, and publishes the archive on tag pushes matching `v*`.

To publish a release:

```bash
git tag v0.1.0
git push origin v0.1.0
```

You can also run the workflow manually from GitHub Actions to get an artifact without publishing a GitHub Release.

## Notes for large files

Telegram bot download limits still apply to bot accounts, and Google Drive quota/rate limits still apply to the connected user, service account, or shared destination. The bot itself keeps memory bounded to approximately one upload chunk plus transport overhead.
