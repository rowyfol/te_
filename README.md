# Telegram → Google Drive uploader bot

A resource-efficient Telegram bot written with [`teloxide`](https://github.com/teloxide/teloxide) that streams incoming files to Google Drive with Google Drive resumable uploads.

## Features

- Accepts documents, videos, audio, voice messages, and photos.
- Downloads from Telegram as an async byte stream.
- Uploads to Google Drive in 8 MiB resumable chunks, so files are never fully buffered in RAM or written to disk.
- Reuses one `reqwest::Client` for connection pooling.
- Caches Google access tokens until shortly before expiry.
- Supports an optional destination Drive folder.

## Setup

1. Create a Telegram bot with BotFather and export its token:

   ```bash
   export TELOXIDE_TOKEN='123456:telegram-token'
   ```

2. Create a Google Cloud service account, enable the Google Drive API, and download the service-account JSON key.

3. Give that service account access to your Drive destination. For example, create or choose a Drive folder and share it with the service account `client_email`.

4. Export the Google credentials either as raw JSON or as a file path:

   ```bash
   export GOOGLE_SERVICE_ACCOUNT_FILE='/secure/path/service-account.json'
   # or:
   export GOOGLE_SERVICE_ACCOUNT_JSON="$(cat /secure/path/service-account.json)"
   ```

5. Optionally target a specific shared folder:

   ```bash
   export GOOGLE_DRIVE_FOLDER_ID='your-folder-id'
   ```

## Run

```bash
cargo run --release
```

Send the bot a file in Telegram. It will stream the download directly into a Google Drive resumable upload session and reply with the Drive link.

## Notes for large files

Telegram bot download limits still apply to bot accounts, and Google Drive quota/rate limits still apply to the service account or shared destination. The bot itself keeps memory bounded to approximately one upload chunk plus transport overhead.
