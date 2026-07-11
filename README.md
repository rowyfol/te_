# Telegram → Google Drive uploader bot

A resource-efficient Telegram bot written with [`teloxide`](https://github.com/teloxide/teloxide) that streams incoming files to Google Drive with Google Drive resumable uploads.

## Features

- Accepts documents, videos, audio, voice messages, and photos.
- Downloads from Telegram as an async byte stream.
- Uploads to Google Drive in 8 MiB resumable chunks, so files are never fully buffered in RAM or written to disk.
- Reuses one `reqwest::Client` for connection pooling.
- Supports two Google Drive authentication modes:
  - **Per-user OAuth:** each allowed Telegram user runs `/auth`, opens a Google consent page, and uploads go to that user's Drive.
  - **Service account:** all uploads go to a service-account-accessible Drive destination.
- Supports multiple Telegram users with separate OAuth token storage.
- Supports an optional Telegram username allow-list; disallowed users are ignored completely.
- Supports an optional destination Drive folder.

## Common setup

1. Create a Telegram bot with BotFather and export its token:

   ```bash
   export TELOXIDE_TOKEN='123456:telegram-token'
   ```

2. Optionally restrict the bot to specific Telegram usernames. Usernames are comma-separated, case-insensitive, and may include or omit `@`:

   ```bash
   export ALLOWED_TELEGRAM_USERNAMES='alice,@bob,charlie'
   ```

   If this variable is set, anyone not on the list is ignored without a reply.

3. Optionally target a specific Drive folder:

   ```bash
   export GOOGLE_DRIVE_FOLDER_ID='your-folder-id'
   ```

## Option A: per-user OAuth consent flow

Use this mode when multiple users should connect their own Google Drive accounts through a browser consent page.

1. In Google Cloud Console, enable the Google Drive API and create an OAuth client for a web application.

2. Add your public callback URL to the OAuth client's authorized redirect URIs. The bot serves this endpoint:

   ```text
   https://your-domain.example/oauth2/callback
   ```

3. Export OAuth settings:

   ```bash
   export GOOGLE_OAUTH_CLIENT_ID='google-client-id.apps.googleusercontent.com'
   export GOOGLE_OAUTH_CLIENT_SECRET='google-client-secret'
   export GOOGLE_OAUTH_REDIRECT_URI='https://your-domain.example/oauth2/callback'
   export OAUTH_BIND_ADDR='0.0.0.0:8080'
   export GOOGLE_OAUTH_TOKENS_FILE='./google-oauth-tokens.json'
   ```

4. Run the bot and tell each allowed user to send `/auth`. The bot replies with a Google consent URL. After consent, tokens are stored by Telegram user id in `GOOGLE_OAUTH_TOKENS_FILE`, so each user's files upload to their own Drive.

## Option B: service-account mode

Use this mode when all uploads should go to one service-account-accessible Drive folder.

1. Create a Google Cloud service account, enable the Google Drive API, and download the service-account JSON key.

2. Give that service account access to your Drive destination. For example, create or choose a Drive folder and share it with the service account `client_email`.

3. Export the Google credentials either as raw JSON or as a file path:

   ```bash
   export GOOGLE_SERVICE_ACCOUNT_FILE='/secure/path/service-account.json'
   # or:
   export GOOGLE_SERVICE_ACCOUNT_JSON="$(cat /secure/path/service-account.json)"
   ```

OAuth mode is selected automatically when `GOOGLE_OAUTH_CLIENT_ID` is set. Otherwise, the bot falls back to service-account mode.

## Run

```bash
cargo run --release
```

Send `/help` to see the mode-specific instructions. In OAuth mode, send `/auth` before sending files. In service-account mode, send files directly.

## Notes for large files

Telegram bot download limits still apply to bot accounts, and Google Drive quota/rate limits still apply to the connected user, service account, or shared destination. The bot itself keeps memory bounded to approximately one upload chunk plus transport overhead.
