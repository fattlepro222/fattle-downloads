# Fattle Downloader — 4 Render workers + private Telegram archive (no VPS)

Upload this repository to GitHub, then deploy the **same repo to four Render Web Services**.

## Delivery model

No Local Telegram Bot API server or VPS is required.

- Files up to `TELEGRAM_DIRECT_MAX_BYTES` (default ~49 MB): Render downloads the completed object from R2, uploads it **once** to your private Telegram archive channel, then uses `copyMessage` to deliver it to the user. The copied message does not expose the archive channel as a forwarded source.
- Larger files: the object stays in Cloudflare R2 and the bot sends the user a temporary signed **Download File** button. Large-file metadata can also be recorded in the private archive channel.
- The normal hosted Bot API upload limit still applies. A private channel cannot bypass that initial upload limit.

Use it only for public/authorized content. This project does not bypass DRM, passwords, paywalls, private shares, or access controls.

## Render #1 (coordinator + worker)

Set:

```text
DOWNLOADER_ROLE=coordinator_worker
DOWNLOADER_API_SECRET=...
WORKER_SECRET=...
MONGODB_URI=...
MONGODB_DB=fattle_downloader
WORKER_1=https://...
WORKER_2=https://...
WORKER_3=https://...
WORKER_4=https://...

STORAGE_ENDPOINT=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
STORAGE_BUCKET=fattle-downloads
STORAGE_ACCESS_KEY=...
STORAGE_SECRET_KEY=...
STORAGE_REGION=auto
MAX_FILE_BYTES=1900000000

BOT_TOKEN=YOUR_EXISTING_TELEGRAM_BOT_TOKEN
ARCHIVE_CHAT_ID=-1001234567890
TELEGRAM_DIRECT_MAX_BYTES=49000000
R2_LINK_TTL_SECONDS=86400
ARCHIVE_LARGE_LINKS=true
DELETE_R2_AFTER_TELEGRAM_ARCHIVE=false
```

Add the bot as an **administrator** of the private archive channel with permission to post messages.

## Render #2–#4

Set only worker/storage settings:

```text
DOWNLOADER_ROLE=worker
WORKER_SECRET=THE_SAME_WORKER_SECRET
STORAGE_ENDPOINT=...
STORAGE_BUCKET=fattle-downloads
STORAGE_ACCESS_KEY=...
STORAGE_SECRET_KEY=...
STORAGE_REGION=auto
MAX_FILE_BYTES=1900000000
```

They do not need `BOT_TOKEN` or `ARCHIVE_CHAT_ID`.

## Build / start

Build:

```text
pip install -r requirements.txt
```

Start:

```text
uvicorn app:app --host 0.0.0.0 --port $PORT
```

## Supported sources

- Public direct HTTP/HTTPS files with byte ranges: up to four Render workers cooperate on one file.
- Public YouTube videos: `yt-dlp` on one worker.
- TeraBox only when the supplied URL resolves to a public direct downloadable file. Private/login/password-protected shares are not bypassed.

## Private archive behavior

Small file:

```text
R2 -> private channel -> copyMessage -> user
```

Large file:

```text
R2 -> temporary signed link -> user
     -> metadata entry in private channel (optional)
```

The endpoint `GET /api/jobs/{job_id}/download-link` can generate a fresh R2 link for a completed stored job.


## MTProto private archive delivery

Large completed files can now be streamed from R2 into Telegram through MTProto
on Render #1, then delivered to the user with Telegram's server-side
`copyMessage`.

Add on Render #1 only:

```text
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=YOUR_API_HASH
TELEGRAM_MTPROTO_MAX_BYTES=1900000000
MTPROTO_R2_BUFFER_BYTES=8388608
```

Flow:

```text
4 Render workers -> R2 -> Render #1 MTProto
-> private archive channel -> copyMessage -> user
```

The private archive channel is not shown as a forwarded source. The large file
is not re-uploaded for each user. The MTProto uploader uses buffered R2 range
reads instead of writing the complete multi-GB object to Render's local disk.

If MTProto upload fails, the user receives an expiring R2 link as fallback.
