import concurrent.futures
import asyncio
import io
import hashlib
import ipaddress
import json
import logging
import mimetypes
import os
import re
import socket
import tempfile
import html
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urljoin, urlparse

import boto3
import certifi
import requests
from botocore.config import Config
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING
from pymongo.server_api import ServerApi
from yt_dlp import YoutubeDL
from requests_toolbelt.multipart.encoder import MultipartEncoder
from telethon import TelegramClient, utils as telethon_utils
from telethon.sessions import StringSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("fattle-downloader")

ROLE = os.getenv("DOWNLOADER_ROLE", "worker").strip().lower()
COORDINATOR_ENABLED = ROLE in {"coordinator", "coordinator_worker", "all"}
API_SECRET = os.getenv("DOWNLOADER_API_SECRET", "").strip()
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()
WORKER_URLS = [
    os.getenv("WORKER_1", "").strip().rstrip("/"),
    os.getenv("WORKER_2", "").strip().rstrip("/"),
    os.getenv("WORKER_3", "").strip().rstrip("/"),
    os.getenv("WORKER_4", "").strip().rstrip("/"),
]
WORKER_URLS = [x for x in WORKER_URLS if x]

MONGODB_URI = os.getenv("MONGODB_URI", "").strip()
MONGODB_DB = os.getenv("MONGODB_DB", "fattle_downloader").strip()

STORAGE_ENDPOINT = os.getenv("STORAGE_ENDPOINT", "").strip()
STORAGE_BUCKET = os.getenv("STORAGE_BUCKET", "").strip()
STORAGE_ACCESS_KEY = os.getenv("STORAGE_ACCESS_KEY", "").strip()
STORAGE_SECRET_KEY = os.getenv("STORAGE_SECRET_KEY", "").strip()
STORAGE_REGION = os.getenv("STORAGE_REGION", "auto").strip() or "auto"

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ARCHIVE_CHAT_ID = os.getenv("ARCHIVE_CHAT_ID", "").strip()

# Normal Bot API for small-file upload and Telegram server-side copyMessage.
TELEGRAM_DIRECT_MAX_BYTES = int(os.getenv("TELEGRAM_DIRECT_MAX_BYTES", "49000000"))

# MTProto for larger archive uploads.
TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "").strip()
TELEGRAM_MTPROTO_MAX_BYTES = int(os.getenv("TELEGRAM_MTPROTO_MAX_BYTES", "1900000000"))
MTPROTO_R2_BUFFER_BYTES = max(
    1024 * 1024,
    min(32 * 1024 * 1024, int(os.getenv("MTPROTO_R2_BUFFER_BYTES", str(8 * 1024 * 1024))))
)

R2_LINK_TTL_SECONDS = max(300, min(604800, int(os.getenv("R2_LINK_TTL_SECONDS", "86400"))))
ARCHIVE_LARGE_LINKS = os.getenv("ARCHIVE_LARGE_LINKS", "true").strip().lower() not in {"0", "false", "no", "off"}
DELETE_R2_AFTER_TELEGRAM_ARCHIVE = os.getenv("DELETE_R2_AFTER_TELEGRAM_ARCHIVE", "false").strip().lower() in {"1", "true", "yes", "on"}

MAX_FILE_BYTES = int(os.getenv("MAX_FILE_BYTES", "1900000000"))
MAX_REDIRECTS = max(1, min(10, int(os.getenv("MAX_REDIRECTS", "5"))))
DOWNLOAD_TIMEOUT = max(30, min(3600, int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "900"))))
MIN_MULTIPART_PART = 5 * 1024 * 1024
MAX_WORKERS_PER_JOB = 4

YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be", "music.youtube.com"}
TERABOX_HINTS = ("terabox", "1024tera", "nephobox", "4funbox", "mirrobox")

app = FastAPI(title="Fattle Downloader", version="1.0")

mongo = None
jobs = None
if MONGODB_URI:
    mongo = MongoClient(
        MONGODB_URI,
        connect=False,
        tls=True,
        tlsCAFile=certifi.where(),
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=7000,
        connectTimeoutMS=7000,
        socketTimeoutMS=15000,
    )
    jobs = mongo[MONGODB_DB]["download_jobs"]
    try:
        jobs.create_index("job_id", unique=True)
        jobs.create_index([("user_id", 1), ("created_at", DESCENDING)])
    except Exception:
        log.exception("MongoDB index setup failed; service can still start")


def now():
    return datetime.now(timezone.utc)


def auth_client(value):
    if not API_SECRET or value != API_SECRET:
        raise HTTPException(status_code=401, detail="Invalid downloader secret")


def auth_worker(value):
    if not WORKER_SECRET or value != WORKER_SECRET:
        raise HTTPException(status_code=401, detail="Invalid worker secret")


def require_storage():
    missing = [k for k, v in {
        "STORAGE_ENDPOINT": STORAGE_ENDPOINT,
        "STORAGE_BUCKET": STORAGE_BUCKET,
        "STORAGE_ACCESS_KEY": STORAGE_ACCESS_KEY,
        "STORAGE_SECRET_KEY": STORAGE_SECRET_KEY,
    }.items() if not v]
    if missing:
        raise RuntimeError("Missing storage variables: " + ", ".join(missing))


def s3():
    require_storage()
    return boto3.client(
        "s3",
        endpoint_url=STORAGE_ENDPOINT,
        aws_access_key_id=STORAGE_ACCESS_KEY,
        aws_secret_access_key=STORAGE_SECRET_KEY,
        region_name=STORAGE_REGION,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
    )


def telegram_api(method, *, data=None, files=None, timeout=(15, 180)):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured on the coordinator")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    r = requests.post(url, data=data, files=files, timeout=timeout)
    try:
        payload = r.json()
    except Exception:
        payload = {"ok": False, "description": r.text[:500]}
    if r.status_code >= 400 or not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload.get('description') or r.text[:300]}")
    return payload.get("result")


def telegram_post_form(method, encoder, *, timeout=(15, 300)):
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured on the coordinator")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    headers = {"Content-Type": encoder.content_type}
    r = requests.post(url, data=encoder, headers=headers, timeout=timeout)
    try:
        payload = r.json()
    except Exception:
        payload = {"ok": False, "description": r.text[:500]}
    if r.status_code >= 400 or not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload.get('description') or r.text[:300]}")
    return payload.get("result")


def generate_download_url(key, expires=None):
    ttl = int(expires or R2_LINK_TTL_SECONDS)
    return s3().generate_presigned_url(
        "get_object",
        Params={"Bucket": STORAGE_BUCKET, "Key": key},
        ExpiresIn=ttl,
    )



class R2BufferedReader(io.RawIOBase):
    """Seekable buffered reader backed by R2/S3 Range requests."""

    def __init__(self, client, bucket, key, total_size, *, buffer_bytes=8 * 1024 * 1024, name="download.bin"):
        super().__init__()
        self.client = client
        self.bucket = bucket
        self.key = key
        self.total_size = int(total_size)
        self.buffer_bytes = int(buffer_bytes)
        self._pos = 0
        self._buffer = b""
        self._buffer_start = -1
        self.name = str(name or "download.bin")
        self.mode = "rb"

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            new_pos = int(offset)
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + int(offset)
        elif whence == io.SEEK_END:
            new_pos = self.total_size + int(offset)
        else:
            raise ValueError("Invalid whence")
        if new_pos < 0:
            raise ValueError("Negative seek position")
        self._pos = min(new_pos, self.total_size)
        return self._pos

    def _ensure_buffer(self):
        if self._pos >= self.total_size:
            self._buffer = b""
            self._buffer_start = self._pos
            return
        if self._buffer and self._buffer_start <= self._pos < self._buffer_start + len(self._buffer):
            return
        start = self._pos
        end = min(self.total_size - 1, start + self.buffer_bytes - 1)
        obj = self.client.get_object(
            Bucket=self.bucket,
            Key=self.key,
            Range=f"bytes={start}-{end}",
        )
        try:
            data = obj["Body"].read()
        finally:
            try:
                obj["Body"].close()
            except Exception:
                pass
        if not data:
            raise IOError(f"R2 returned no data for bytes {start}-{end}")
        self._buffer = data
        self._buffer_start = start

    def read(self, size=-1):
        if self._pos >= self.total_size:
            return b""
        if size is None or int(size) < 0:
            size = min(self.buffer_bytes, self.total_size - self._pos)
        else:
            size = min(int(size), self.total_size - self._pos)

        out = bytearray()
        while len(out) < size and self._pos < self.total_size:
            self._ensure_buffer()
            offset = self._pos - self._buffer_start
            available = len(self._buffer) - offset
            take = min(size - len(out), available)
            if take <= 0:
                self._buffer = b""
                continue
            out.extend(self._buffer[offset:offset + take])
            self._pos += take
        return bytes(out)


def mtproto_configured():
    return bool(BOT_TOKEN and ARCHIVE_CHAT_ID and TELEGRAM_API_ID > 0 and TELEGRAM_API_HASH)


async def _resolve_archive_entity(client):
    wanted = int(ARCHIVE_CHAT_ID)
    async for dialog in client.iter_dialogs():
        try:
            if int(telethon_utils.get_peer_id(dialog.entity)) == wanted:
                return dialog.entity
        except Exception:
            continue
    raise RuntimeError(
        "Archive channel was not found. Add the bot as an admin of the private "
        "channel and allow it to post messages."
    )


async def _archive_large_mtproto_async(job_id, key, filename, content_type, size):
    client = TelegramClient(
        StringSession(),
        TELEGRAM_API_ID,
        TELEGRAM_API_HASH,
        connection_retries=5,
        request_retries=5,
    )
    await client.start(bot_token=BOT_TOKEN)
    reader = None
    try:
        archive_entity = await _resolve_archive_entity(client)
        reader = R2BufferedReader(
            s3(),
            STORAGE_BUCKET,
            key,
            int(size),
            buffer_bytes=MTPROTO_R2_BUFFER_BYTES,
            name=filename,
        )

        size_mb = int(size) / (1024 * 1024)
        size_text = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.1f} MB"
        caption = (
            "✅ Download complete\n\n"
            f"📄 {filename}\n"
            f"📦 {size_text}"
        )[:1024]

        last_report = {"percent": -1}

        def progress(current, total):
            total = max(1, int(total or size))
            percent = min(100, int(int(current) * 100 / total))
            if percent >= last_report["percent"] + 2 or percent == 100:
                last_report["percent"] = percent
                set_job(
                    job_id,
                    status="uploading_telegram",
                    telegram_upload_progress=percent,
                )

        uploaded = await client.upload_file(
            reader,
            file_size=int(size),
            file_name=filename,
            part_size_kb=512,
            progress_callback=progress,
        )
        message = await client.send_file(
            archive_entity,
            uploaded,
            caption=caption,
            force_document=True,
        )
        return int(message.id)
    finally:
        try:
            if reader is not None:
                reader.close()
        except Exception:
            pass
        await client.disconnect()


def archive_large_mtproto(job_id, chat_id, key, filename, content_type, size):
    if not mtproto_configured():
        raise RuntimeError(
            "MTProto is not configured. Set TELEGRAM_API_ID and TELEGRAM_API_HASH on Render #1."
        )

    message_id = asyncio.run(
        _archive_large_mtproto_async(
            job_id,
            key,
            filename,
            content_type,
            int(size),
        )
    )

    copied = telegram_api(
        "copyMessage",
        data={
            "chat_id": str(int(chat_id)),
            "from_chat_id": str(ARCHIVE_CHAT_ID),
            "message_id": str(message_id),
        },
        timeout=(15, 90),
    )

    if DELETE_R2_AFTER_TELEGRAM_ARCHIVE:
        try:
            s3().delete_object(Bucket=STORAGE_BUCKET, Key=key)
        except Exception:
            log.exception("Could not delete MTProto-archived R2 object %s", key)

    return {
        "delivery_mode": "telegram_mtproto_archive_copy",
        "archive_message_id": message_id,
        "user_message_id": int((copied or {}).get("message_id") or 0),
        "telegram_upload_progress": 100,
    }


def archive_small_file(job_id, chat_id, key, filename, content_type, size):
    if not ARCHIVE_CHAT_ID:
        raise RuntimeError("ARCHIVE_CHAT_ID is not configured")
    client = s3()
    with tempfile.NamedTemporaryFile(prefix=f"archive-{job_id}-", suffix="-" + Path(filename).name, delete=True) as tmp:
        client.download_fileobj(STORAGE_BUCKET, key, tmp)
        tmp.flush()
        tmp.seek(0)
        size_mb = int(size) / (1024 * 1024)
        size_text = f"{size_mb:.1f} MB"
        caption = (
            "✅ Download complete\n\n"
            f"📄 {filename}\n"
            f"📦 {size_text}"
        )
        encoder = MultipartEncoder(fields={
            "chat_id": str(ARCHIVE_CHAT_ID),
            "caption": caption[:1024],
            "document": (filename, tmp, content_type or "application/octet-stream"),
        })
        result = telegram_post_form("sendDocument", encoder, timeout=(15, 600))

    message_id = int(result.get("message_id"))
    document = result.get("document") or {}
    file_id = str(document.get("file_id") or "")
    copy = telegram_api(
        "copyMessage",
        data={
            "chat_id": str(int(chat_id)),
            "from_chat_id": str(ARCHIVE_CHAT_ID),
            "message_id": str(message_id),
        },
        timeout=(15, 60),
    )
    if DELETE_R2_AFTER_TELEGRAM_ARCHIVE:
        try:
            client.delete_object(Bucket=STORAGE_BUCKET, Key=key)
        except Exception:
            log.exception("Could not delete archived R2 object %s", key)
    return {
        "delivery_mode": "telegram_archive_copy",
        "archive_message_id": message_id,
        "telegram_file_id": file_id,
        "user_message_id": int((copy or {}).get("message_id") or 0),
    }


def deliver_large_link(job_id, chat_id, key, filename, content_type, size):
    url = generate_download_url(key)
    size_mb = int(size) / (1024 * 1024)
    size_text = f"{size_mb / 1024:.2f} GB" if size_mb >= 1024 else f"{size_mb:.1f} MB"
    body = (
        "✅ <b>Download complete</b>\n\n"
        f"📄 <code>{html.escape(filename)}</code>\n"
        f"📦 <b>{size_text}</b>\n\n"
        "This file is larger than the normal Telegram Bot API upload limit, "
        "so use the temporary download button below."
    )
    markup = json.dumps({"inline_keyboard": [[{"text": "⬇️ Download File", "url": url}]]})

    archive_message_id = 0
    if ARCHIVE_CHAT_ID and ARCHIVE_LARGE_LINKS:
        try:
            archived = telegram_api(
                "sendMessage",
                data={
                    "chat_id": str(ARCHIVE_CHAT_ID),
                    "text": (
                        "📦 <b>Large Downloader Archive</b>\n\n"
                        f"📄 <code>{html.escape(filename)}</code>\n"
                        f"📦 {size_text}\n"
                        f"🆔 <code>{job_id}</code>\n"
                        f"🗄 <code>{html.escape(key)}</code>"
                    ),
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
                timeout=(15, 60),
            )
            archive_message_id = int((archived or {}).get("message_id") or 0)
        except Exception:
            log.exception("Could not write large-file metadata to archive channel")

    sent = telegram_api(
        "sendMessage",
        data={
            "chat_id": str(int(chat_id)),
            "text": body,
            "parse_mode": "HTML",
            "reply_markup": markup,
            "disable_web_page_preview": "true",
        },
        timeout=(15, 60),
    )
    return {
        "delivery_mode": "r2_temporary_link",
        "archive_message_id": archive_message_id,
        "user_message_id": int((sent or {}).get("message_id") or 0),
        "link_expires_seconds": R2_LINK_TTL_SECONDS,
    }


def deliver_completed_file(job_id, chat_id, key, filename, content_type, size):
    """Archive once in Telegram, then server-side copyMessage to the user.

    Small files use the normal Bot API. Larger files use MTProto streamed from
    R2. If Telegram delivery fails, an expiring R2 link is sent as a fallback.
    """
    if not BOT_TOKEN:
        set_job(job_id, delivery_mode="r2_only", delivery_status="not_configured")
        return {"delivery_mode": "r2_only"}

    size = int(size)

    if size <= TELEGRAM_DIRECT_MAX_BYTES and ARCHIVE_CHAT_ID:
        try:
            result = archive_small_file(job_id, chat_id, key, filename, content_type, size)
            set_job(job_id, delivery_status="sent", **result)
            return result
        except Exception as exc:
            log.exception("Small Telegram archive upload failed")
            set_job(job_id, telegram_archive_error=str(exc)[:1000])

    if size <= TELEGRAM_MTPROTO_MAX_BYTES and ARCHIVE_CHAT_ID and mtproto_configured():
        try:
            set_job(job_id, status="uploading_telegram", telegram_upload_progress=0)
            result = archive_large_mtproto(job_id, chat_id, key, filename, content_type, size)
            set_job(job_id, delivery_status="sent", **result)
            return result
        except Exception as exc:
            log.exception("MTProto archive upload failed; falling back to R2 link")
            set_job(job_id, telegram_mtproto_error=str(exc)[:1000])

    result = deliver_large_link(job_id, chat_id, key, filename, content_type, size)
    set_job(job_id, delivery_status="sent", **result)
    return result


def set_job(job_id, **fields):
    fields["updated_at"] = now()
    if jobs is not None:
        jobs.update_one({"job_id": job_id}, {"$set": fields}, upsert=True)


def get_job(job_id):
    if jobs is None:
        return None
    doc = jobs.find_one({"job_id": job_id}, {"_id": 0})
    if doc:
        for k in ("created_at", "updated_at", "completed_at"):
            if hasattr(doc.get(k), "isoformat"):
                doc[k] = doc[k].isoformat()
    return doc


def normalize_host(host):
    return (host or "").strip(".").lower()


def validate_public_url(url):
    p = urlparse(url)
    if p.scheme not in {"http", "https"}:
        raise ValueError("Only http:// and https:// URLs are allowed")
    host = normalize_host(p.hostname)
    if not host or host == "localhost" or host.endswith(".local"):
        raise ValueError("Local/private hosts are not allowed")
    try:
        infos = socket.getaddrinfo(host, p.port or (443 if p.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError("Could not resolve source host") from exc
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if any((ip.is_private, ip.is_loopback, ip.is_link_local, ip.is_multicast, ip.is_reserved, ip.is_unspecified)):
            raise ValueError("Private/reserved network addresses are not allowed")
    return url


def safe_request(method, url, *, headers=None, stream=False, timeout=None):
    current = url
    for _ in range(MAX_REDIRECTS + 1):
        validate_public_url(current)
        r = requests.request(
            method,
            current,
            headers=headers or {},
            stream=stream,
            timeout=timeout or (15, DOWNLOAD_TIMEOUT),
            allow_redirects=False,
        )
        if r.status_code in {301, 302, 303, 307, 308}:
            loc = r.headers.get("Location")
            r.close()
            if not loc:
                raise ValueError("Source returned a redirect without a Location header")
            current = urljoin(current, loc)
            continue
        return r, current
    raise ValueError("Too many redirects")


def source_kind(url):
    host = normalize_host(urlparse(url).hostname)
    if host in YOUTUBE_HOSTS or host.endswith(".youtube.com"):
        return "youtube"
    if any(x in host for x in TERABOX_HINTS):
        return "terabox"
    return "direct"


def parse_filename(response, final_url):
    cd = response.headers.get("Content-Disposition", "")
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd, re.I)
    if m:
        name = unquote(m.group(1))
    else:
        m = re.search(r'filename="?([^";]+)', cd, re.I)
        name = m.group(1) if m else Path(unquote(urlparse(final_url).path)).name
    name = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", name or "download.bin").strip(" .")
    return (name or "download.bin")[:180]


def probe_direct(url):
    r, final_url = safe_request("GET", url, headers={"Range": "bytes=0-0", "User-Agent": "FattleDownloader/1.0"}, stream=True)
    try:
        content_type = (r.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0].strip()
        filename = parse_filename(r, final_url)
        total = None
        ranges = False
        if r.status_code == 206:
            cr = r.headers.get("Content-Range", "")
            m = re.match(r"bytes\s+\d+-\d+/(\d+|\*)", cr, re.I)
            if m and m.group(1).isdigit():
                total = int(m.group(1))
                ranges = True
        elif r.status_code == 200:
            cl = r.headers.get("Content-Length")
            total = int(cl) if cl and cl.isdigit() else None
            ranges = "bytes" in (r.headers.get("Accept-Ranges") or "").lower()
        else:
            raise ValueError(f"Source returned HTTP {r.status_code}")
        if not total:
            raise ValueError("The source did not provide a reliable file size")
        if total > MAX_FILE_BYTES:
            raise ValueError(f"File is too large ({total} bytes). Limit is {MAX_FILE_BYTES} bytes")
        if content_type.startswith("text/html") and source_kind(url) == "terabox":
            raise ValueError("This TeraBox share page is not a direct public file URL. This build does not bypass TeraBox login/share restrictions.")
        return {"url": final_url, "size": total, "range": ranges, "filename": filename, "content_type": content_type}
    finally:
        r.close()


def choose_part_count(size, range_supported):
    if not range_supported:
        return 1
    max_parts_by_size = max(1, size // MIN_MULTIPART_PART)
    return max(1, min(MAX_WORKERS_PER_JOB, len(WORKER_URLS) or 1, max_parts_by_size))


def split_ranges(size, count):
    base = size // count
    result = []
    start = 0
    for i in range(count):
        end = size - 1 if i == count - 1 else start + base - 1
        result.append((i + 1, start, end))
        start = end + 1
    return result


class JobCreate(BaseModel):
    user_id: int
    chat_id: int
    url: str = Field(min_length=8, max_length=4096)
    quality: str = "720p"


class WorkerRange(BaseModel):
    job_id: str
    source_url: str
    storage_key: str
    upload_id: str
    part_number: int
    start: int
    end: int


class WorkerYoutube(BaseModel):
    job_id: str
    source_url: str
    storage_key_prefix: str
    quality: str = "720p"


class CancelRequest(BaseModel):
    user_id: int


def worker_download_range(body: WorkerRange):
    headers = {
        "Range": f"bytes={body.start}-{body.end}",
        "User-Agent": "FattleDownloader/1.0",
        "Accept-Encoding": "identity",
    }
    response, _ = safe_request("GET", body.source_url, headers=headers, stream=True)
    try:
        if response.status_code != 206:
            raise RuntimeError(f"Range download expected HTTP 206 but received {response.status_code}")
        expected = body.end - body.start + 1
        with tempfile.NamedTemporaryFile(prefix=f"{body.job_id}-p{body.part_number}-", delete=True) as tmp:
            received = 0
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                tmp.write(chunk)
                received += len(chunk)
                if received > expected:
                    raise RuntimeError("Origin returned more bytes than requested")
            if received != expected:
                raise RuntimeError(f"Incomplete range: expected {expected}, got {received}")
            tmp.flush()
            tmp.seek(0)
            result = s3().upload_part(
                Bucket=STORAGE_BUCKET,
                Key=body.storage_key,
                UploadId=body.upload_id,
                PartNumber=body.part_number,
                Body=tmp,
                ContentLength=expected,
            )
        return {"PartNumber": body.part_number, "ETag": result["ETag"], "bytes": expected}
    finally:
        response.close()


def ytdlp_format(quality):
    q = (quality or "720p").lower()
    if q == "audio":
        return "bestaudio/best"
    height = 720
    m = re.search(r"(360|480|720|1080)", q)
    if m:
        height = int(m.group(1))
    # Prefer a progressive MP4 to avoid requiring FFmpeg. Public videos only.
    return f"best[ext=mp4][height<={height}]/best[height<={height}]/best"


def worker_download_youtube(body: WorkerYoutube):
    validate_public_url(body.source_url)
    with tempfile.TemporaryDirectory(prefix=f"yt-{body.job_id}-") as td:
        outtmpl = str(Path(td) / "%(title).120B-%(id)s.%(ext)s")
        opts = {
            "format": ytdlp_format(body.quality),
            "outtmpl": outtmpl,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "restrictfilenames": True,
            "socket_timeout": 30,
        }
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(body.source_url, download=True)
            path = Path(ydl.prepare_filename(info))
        if not path.exists():
            files = [p for p in Path(td).iterdir() if p.is_file()]
            if not files:
                raise RuntimeError("yt-dlp did not produce a file")
            path = max(files, key=lambda p: p.stat().st_size)
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            raise RuntimeError(f"YouTube result is too large ({size} bytes)")
        filename = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", path.name)[:180]
        key = f"{body.storage_key_prefix}/{filename}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        s3().upload_file(str(path), STORAGE_BUCKET, key, ExtraArgs={"ContentType": content_type})
        return {"storage_key": key, "filename": filename, "size": size, "content_type": content_type}


def call_worker(url, path, payload):
    r = requests.post(
        url + path,
        json=payload,
        headers={"X-Worker-Secret": WORKER_SECRET},
        timeout=(15, DOWNLOAD_TIMEOUT),
    )
    if r.status_code >= 400:
        raise RuntimeError(f"Worker {url} failed: HTTP {r.status_code}: {r.text[:300]}")
    return r.json()


def maybe_deliver(job_id, chat_id, key, filename, content_type, size):
    try:
        return deliver_completed_file(job_id, chat_id, key, filename, content_type, size)
    except Exception as exc:
        # The download itself is still valid in R2 even if Telegram notification fails.
        log.exception("Delivery failed for job %s", job_id)
        set_job(job_id, delivery_status="failed", delivery_error=str(exc)[:1000])
        return None


def run_direct_job(job_id, request: JobCreate):
    probe = probe_direct(request.url)
    set_job(job_id, status="preparing", source_type="direct", filename=probe["filename"], size=probe["size"], content_type=probe["content_type"], progress=2)
    client = s3()
    key = f"jobs/{job_id}/{probe['filename']}"
    count = choose_part_count(probe["size"], probe["range"])

    if count == 1:
        # One worker still uses multipart with one part. S3 permits a final part below 5 MB.
        count = 1
    upload = client.create_multipart_upload(Bucket=STORAGE_BUCKET, Key=key, ContentType=probe["content_type"])
    upload_id = upload["UploadId"]
    set_job(job_id, status="downloading", storage_key=key, worker_count=count, progress=5)
    try:
        ranges = split_ranges(probe["size"], count)
        workers = (WORKER_URLS or [os.getenv("SELF_URL", "").strip().rstrip("/")])[:count]
        if len(workers) < count or any(not x for x in workers):
            raise RuntimeError("Not enough worker URLs are configured")
        payloads = []
        for (part_number, start, end), worker in zip(ranges, workers):
            payloads.append((worker, {
                "job_id": job_id,
                "source_url": probe["url"],
                "storage_key": key,
                "upload_id": upload_id,
                "part_number": part_number,
                "start": start,
                "end": end,
            }))
        parts = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as ex:
            futures = [ex.submit(call_worker, worker, "/worker/range", payload) for worker, payload in payloads]
            for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
                parts.append(fut.result())
                set_job(job_id, progress=min(85, 5 + int(75 * idx / count)))
        parts.sort(key=lambda x: x["PartNumber"])
        client.complete_multipart_upload(
            Bucket=STORAGE_BUCKET,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": p["PartNumber"], "ETag": p["ETag"]} for p in parts]},
        )
    except Exception:
        try:
            client.abort_multipart_upload(Bucket=STORAGE_BUCKET, Key=key, UploadId=upload_id)
        except Exception:
            pass
        raise

    set_job(job_id, status="stored", progress=92, storage_key=key)
    maybe_deliver(job_id, request.chat_id, key, probe["filename"], probe["content_type"], probe["size"])
    set_job(job_id, status="complete", progress=100, completed_at=now())


def run_youtube_job(job_id, request: JobCreate):
    if not WORKER_URLS:
        raise RuntimeError("At least one worker URL is required for YouTube")
    set_job(job_id, status="downloading", source_type="youtube", progress=5, worker_count=1)
    result = call_worker(WORKER_URLS[0], "/worker/youtube", {
        "job_id": job_id,
        "source_url": request.url,
        "storage_key_prefix": f"jobs/{job_id}",
        "quality": request.quality,
    })
    set_job(job_id, status="stored", progress=92, storage_key=result["storage_key"], filename=result["filename"], size=result["size"], content_type=result["content_type"])
    maybe_deliver(job_id, request.chat_id, result["storage_key"], result["filename"], result["content_type"], result["size"])
    set_job(job_id, status="complete", progress=100, completed_at=now())


def run_job(job_id, request: JobCreate):
    try:
        if jobs is not None:
            doc = jobs.find_one({"job_id": job_id}, {"cancel_requested": 1}) or {}
            if doc.get("cancel_requested"):
                set_job(job_id, status="cancelled")
                return
        kind = source_kind(request.url)
        if kind == "youtube":
            run_youtube_job(job_id, request)
        else:
            run_direct_job(job_id, request)
    except Exception as exc:
        log.exception("Download job %s failed", job_id)
        set_job(job_id, status="failed", error=str(exc)[:1000])


@app.get("/")
def root():
    return {"service": "fattle-downloader", "role": ROLE, "coordinator": COORDINATOR_ENABLED, "mtproto": mtproto_configured()}


@app.get("/health")
@app.head("/health")
def health():
    return {"ok": True, "role": ROLE, "coordinator": COORDINATOR_ENABLED}


@app.post("/worker/range")
def worker_range(body: WorkerRange, x_worker_secret: str | None = Header(default=None)):
    auth_worker(x_worker_secret)
    return worker_download_range(body)


@app.post("/worker/youtube")
def worker_youtube(body: WorkerYoutube, x_worker_secret: str | None = Header(default=None)):
    auth_worker(x_worker_secret)
    return worker_download_youtube(body)


@app.post("/api/jobs")
def create_job(body: JobCreate, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if not COORDINATOR_ENABLED:
        raise HTTPException(status_code=404, detail="Coordinator API is disabled on this service")
    if jobs is None:
        raise HTTPException(status_code=503, detail="MONGODB_URI is required on the coordinator")
    try:
        validate_public_url(body.url)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    job_id = uuid.uuid4().hex
    jobs.insert_one({
        "job_id": job_id,
        "user_id": int(body.user_id),
        "chat_id": int(body.chat_id),
        "source_url": body.url,
        "status": "queued",
        "progress": 0,
        "created_at": now(),
        "updated_at": now(),
        "cancel_requested": False,
    })
    threading.Thread(target=run_job, args=(job_id, body), daemon=True).start()
    return {"ok": True, "job_id": job_id, "status": "queued"}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    doc = get_job(job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    return doc


@app.get("/api/users/{user_id}/recent")
def recent_jobs(user_id: int, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if jobs is None:
        raise HTTPException(status_code=503, detail="MongoDB is not configured")
    result = []
    for doc in jobs.find({"user_id": int(user_id)}, {"_id": 0}).sort("created_at", -1).limit(10):
        for k in ("created_at", "updated_at", "completed_at"):
            if hasattr(doc.get(k), "isoformat"):
                doc[k] = doc[k].isoformat()
        result.append(doc)
    return {"jobs": result}


@app.get("/api/jobs/{job_id}/download-link")
def job_download_link(job_id: str, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    doc = get_job(job_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found")
    key = str(doc.get("storage_key") or "")
    if not key:
        raise HTTPException(status_code=409, detail="The file is not stored yet")
    if not STORAGE_BUCKET:
        raise HTTPException(status_code=503, detail="Object storage is not configured")
    return {
        "ok": True,
        "url": generate_download_url(key),
        "expires_in": R2_LINK_TTL_SECONDS,
        "filename": doc.get("filename"),
        "size": doc.get("size"),
    }


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str, body: CancelRequest, x_downloader_secret: str | None = Header(default=None)):
    auth_client(x_downloader_secret)
    if jobs is None:
        raise HTTPException(status_code=503, detail="MongoDB is not configured")
    result = jobs.update_one({"job_id": job_id, "user_id": int(body.user_id)}, {"$set": {"cancel_requested": True, "updated_at": now()}})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    # Cancellation is cooperative. Already-running HTTP range requests may finish their current part.
    return {"ok": True, "cancel_requested": True}
