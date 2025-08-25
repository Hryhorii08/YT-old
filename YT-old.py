# ============================ БИБЛИОТЕКИ ============================
import os
import re
import time
import json
import pickle
import tempfile
import requests
import io
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as SA_Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
# ===================================================================


# ============================ НАСТРОЙКИ (ENV) ============================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "")
SHEET_NAME         = os.environ.get("SHEET_NAME", "Лист1")
TRIGGER_TEXT       = os.environ.get("TRIGGER_TEXT", "1")

# Статичные параметры (оставлены в коде)
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

COL_VIDEO = "A"
COL_TITLE = "B"
COL_DESC  = "C"
DELETE_FIRST_ROW_AFTER_SUCCESS = True

SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "service_account.json")
CLIENT_SECRET_FILE   = os.environ.get("CLIENT_SECRET_FILE", "client_secret.json")
TOKEN_FILE           = os.environ.get("TOKEN_FILE", "token.pickle")

YOUTUBE_CATEGORY_ID = "22"
YOUTUBE_DEFAULT_VISIBILITY = "public"
YOUTUBE_MADE_FOR_KIDS = False
YOUTUBE_DEFAULT_TAGS = ["Shorts"]
# ===================================================================


# ---------------------- Утилиты логирования ----------------------
def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} - {msg}")


# ---------------------- Проверка обязательных ENV ----------------------
def ensure_env():
    missing = []
    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not SPREADSHEET_ID:
        missing.append("SPREADSHEET_ID")
    if not SHEET_NAME:
        missing.append("SHEET_NAME")
    if missing:
        for k in missing:
            log(f"❌ Отсутствует переменная окружения: {k}")
        raise SystemExit(1)


# ---------------------- Google Auth & Services ----------------------
SCOPES_YT = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
SCOPES_SA = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _normalize_sheet_id(x: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", x)
    return m.group(1) if m else x
SPREADSHEET_ID = _normalize_sheet_id(SPREADSHEET_ID) if SPREADSHEET_ID else SPREADSHEET_ID

_DRIVE = None
def drive_service():
    global _DRIVE
    if _DRIVE is None:
        creds = SA_Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES_SA)
        _DRIVE = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _DRIVE

def sheets_service():
    creds = SA_Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES_SA)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def youtube_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            creds = pickle.load(f)
    if not creds or not getattr(creds, "valid", False):
        if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES_YT)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(creds, f)
    return build("youtube", "v3", credentials=creds, cache_discovery=False)


# ---------------------- Sheets Helpers ----------------------
def get_first_row(sh):
    rng = f"{SHEET_NAME}!{COL_VIDEO}1:{COL_DESC}1"
    res = sh.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range=rng).execute()
    vals = res.get("values", [])
    if not vals or not vals[0]:
        return None
    row = vals[0]
    v = row[0].strip() if len(row) > 0 else ""
    t = row[1].strip() if len(row) > 1 else ""
    d = row[2].strip() if len(row) > 2 else ""
    if not v:
        return None
    return {"video": v, "title": t, "desc": d}

def get_sheet_id(sh) -> int:
    meta = sh.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == SHEET_NAME:
            return props.get("sheetId")
    raise ValueError(f"Sheet '{SHEET_NAME}' not found")

def delete_first_row(sh):
    sid = get_sheet_id(sh)
    body = {"requests": [{"deleteDimension": {"range": {
        "sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1
    }}}]}
    sh.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()


# ---------------------- Источник видео (Drive-safe) ----------------------
DRIVE_ID_RX = re.compile(r"(?:https?://)?(?:drive\.google\.com)/(?:file/d/|open\?id=|uc\?id=)([A-Za-z0-9_-]+)")

def _save_stream_to_tmp(resp) -> str:
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        if chunk:
            tmp.write(chunk)
    tmp.flush(); tmp.close()
    return tmp.name

def _ensure_valid_video(path: str):
    size = os.path.getsize(path)
    if size < 200 * 1024:
        raise ValueError(f"Скачан слишком маленький файл ({size} байт)")
    with open(path, "rb") as f:
        head = f.read(65536)
    if head.lstrip().lower().startswith(b"<!doctype html") or b"<html" in head.lower():
        raise ValueError("Получен HTML вместо видео (страница Google Drive)")
    return path

def gdrive_download_via_api(file_id: str) -> str:
    svc = drive_service()
    req = svc.files().get_media(fileId=file_id)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
    fh = io.FileIO(tmp.name, "wb")
    downloader = MediaIoBaseDownload(fh, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()
    return _ensure_valid_video(tmp.name)

def gdrive_download_public(file_id: str) -> str:
    URL = "https://drive.google.com/uc?export=download"
    with requests.Session() as s:
        params = {"id": file_id}
        r = s.get(URL, params=params, stream=True, timeout=180)
        if "text/html" in (r.headers.get("Content-Type") or ""):
            token = None
            m = re.search(r"confirm=([0-9A-Za-z_]+)", r.text)
            if m:
                token = m.group(1)
            else:
                for k, v in r.cookies.items():
                    if k.startswith("download_warning"):
                        token = v
                        break
            if token:
                params["confirm"] = token
                r = s.get(URL, params=params, stream=True, timeout=180)
        path = _save_stream_to_tmp(r)
    return _ensure_valid_video(path)

def url_to_tempfile(url: str) -> str:
    r = requests.get(url, stream=True, timeout=300)
    path = _save_stream_to_tmp(r)
    return _ensure_valid_video(path)

def resolve_video_source(src: str):
    src = src.strip().strip('"').strip("'")
    src = os.path.expanduser(os.path.expandvars(src))

    if os.path.isfile(src):
        return src, False

    m = DRIVE_ID_RX.search(src)
    if m:
        file_id = m.group(1)
        try:
            return gdrive_download_via_api(file_id), True
        except Exception:
            return gdrive_download_public(file_id), True

    if src.startswith("http://") or src.startswith("https://"):
        return url_to_tempfile(src), True

    raise FileNotFoundError(f"Источник видео не найден: {src}")


# ---------------------- Загрузка на YouTube ----------------------
class UploadLimitExceeded(Exception):
    pass

def _is_upload_limit_error(err: Exception) -> bool:
    try:
        if isinstance(err, HttpError):
            content = (err.content or b"").decode("utf-8", "ignore")
            if "uploadLimitExceeded" in content or "exceeded the number of videos they may upload" in content:
                return True
    except Exception:
        pass
    return False

def upload_video(yt, file_path: str, title: str, description: str) -> str:
    snippet = {
        "title": (title or os.path.basename(file_path))[:100],
        "description": description or "",
        "categoryId": YOUTUBE_CATEGORY_ID,
    }
    if YOUTUBE_DEFAULT_TAGS:
        snippet["tags"] = YOUTUBE_DEFAULT_TAGS
    status = {
        "privacyStatus": YOUTUBE_DEFAULT_VISIBILITY,
        "selfDeclaredMadeForKids": bool(YOUTUBE_MADE_FOR_KIDS),
    }
    media = MediaFileUpload(file_path, chunksize=8*1024*1024, resumable=True, mimetype="video/*")
    request = yt.videos().insert(part="snippet,status", body={"snippet": snippet, "status": status}, media_body=media)
    response = None
    while response is None:
        try:
            _, response = request.next_chunk()
        except Exception as e:
            if _is_upload_limit_error(e):
                raise UploadLimitExceeded("Лимит отправки видео на YouTube")
            log(f"Повторная попытка загрузки: {e}")
            time.sleep(3)
            continue
    return response.get("id")


# ---------------------- Telegram Bot API ----------------------
def tg_delete_webhook(drop=True):
    try:
        requests.post(f"{TELEGRAM_API}/deleteWebhook", json={"drop_pending_updates": bool(drop)}, timeout=20)
    except Exception:
        pass

def tg_send(chat_id: int, text: str):
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=30)
    except Exception as e:
        log(f"❌ Ошибка: отправка сообщения в Telegram: {e}")

def tg_get_updates(offset: int = None, timeout_s: int = 60):
    params = {"timeout": timeout_s}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(f"{TELEGRAM_API}/getUpdates", params=params, timeout=timeout_s + 10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"❌ Ошибка: получение апдейтов Telegram: {e}")
        return {"ok": False, "result": []}


# ---------------------- Выполнение задачи ----------------------
def process_once():
    try:
        sh = sheets_service()
        row = get_first_row(sh)
    except Exception as e:
        return {"status": "SHEETS_ACCESS_ERROR", "error": str(e)}

    # Если первая строка пустая — удаляем её
    if not row:
        try:
            delete_first_row(sh)
        except Exception as e:
            return {"status": "ROW_DELETE_ERROR", "error": str(e)}
        return {"status": "EMPTY_SHEET", "error": "нет данных в таблице"}

    src, title, desc = row["video"], row["title"], row["desc"]
    try:
        local_path, is_temp = resolve_video_source(src)
    except Exception as e:
        return {"status": "DOWNLOAD_ERROR", "error": str(e)}

    try:
        try:
            yt = youtube_service()
        except Exception as e:
            return {"status": "YOUTUBE_AUTH_ERROR", "error": str(e)}
        vid = upload_video(yt, local_path, title, desc)
    except UploadLimitExceeded as e:
        return {"status": "UPLOAD_LIMIT", "error": str(e)}
    finally:
        if 'is_temp' in locals() and is_temp and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass

    if DELETE_FIRST_ROW_AFTER_SUCCESS:
        try:
            delete_first_row(sh)
        except Exception as e:
            return {"status": "ROW_DELETE_ERROR", "video_id": vid, "error": str(e)}

    return {"status": "OK", "video_id": vid}


# ---------------------- Главный цикл ----------------------
def main():
    ensure_env()
    tg_delete_webhook(drop=True)
    log("Сценарий запущен")
    last_update_id = None
    while True:
        data = tg_get_updates(last_update_id + 1 if last_update_id else None, timeout_s=60)
        if not data.get("ok"):
            time.sleep(1)
            continue
        for upd in data.get("result", []):
            log("Новый запрос")
            log(f"update: {upd}")
            last_update_id = upd["update_id"]
            try:
                msg = upd.get("message") or upd.get("channel_post") or {}
                chat = msg.get("chat", {})
                chat_id = chat.get("id")
                text = msg.get("text")
                if chat_id is None:
                    log("❌ Ошибка: отсутствует chat_id")
                    log("")
                    continue
                if text is None or text.strip() == "":
                    tg_send(chat_id, "❌ Ошибка: пустая строка")
                    log("❌ Ошибка: пустая строка")
                    log("")
                    continue
                if text.strip() != TRIGGER_TEXT:
                    tg_send(chat_id, "Код ничего не активирует")
                    log("Код ничего не активирует")
                    log("")
                    continue

                tg_send(chat_id, "Старт публикации…")
                rep = process_once()
                status = rep.get("status")

                if status == "OK":
                    vid = rep["video_id"]
                    tg_send(chat_id, f"Создано видео, ID: {vid}")
                    log(f"Создано видео, ID: {vid}")
                    log("")
                elif status == "UPLOAD_LIMIT":
                    tg_send(chat_id, "Лимит отправки видео на YouTube")
                    log("Лимит отправки видео на YouTube")
                    log("")
                elif status == "EMPTY_SHEET":
                    tg_send(chat_id, "❌ Ошибка: нет данных в таблице")
                    log("❌ Ошибка: нет данных в таблице")
                    log("")
                elif status == "SHEETS_ACCESS_ERROR":
                    tg_send(chat_id, f"❌ Ошибка: доступ к таблице: {rep.get('error')}")
                    log(f"❌ Ошибка: доступ к таблице: {rep.get('error')}")
                    log("")
                elif status == "DOWNLOAD_ERROR":
                    tg_send(chat_id, f"❌ Ошибка: загрузка видео: {rep.get('error')}")
                    log(f"❌ Ошибка: загрузка видео: {rep.get('error')}")
                    log("")
                elif status == "YOUTUBE_AUTH_ERROR":
                    tg_send(chat_id, f"❌ Ошибка: авторизация YouTube: {rep.get('error')}")
                    log(f"❌ Ошибка: авторизация YouTube: {rep.get('error')}")
                    log("")
                elif status == "ROW_DELETE_ERROR":
                    vid = rep.get("video_id", "")
                    tg_send(chat_id, f"❌ Ошибка: удаление строки: {rep.get('error')} (Видео загружено: {vid})")
                    log(f"❌ Ошибка: удаление строки: {rep.get('error')} (Видео загружено: {vid})")
                    log("")
                else:
                    tg_send(chat_id, f"❌ Ошибка: неизвестный статус: {status}")
                    log(f"❌ Ошибка: неизвестный статус: {status}")
                    log("")
            except Exception as e:
                try:
                    chat_id = (upd.get("message") or upd.get("channel_post") or {}).get("chat", {}).get("id")
                    if chat_id is not None:
                        tg_send(chat_id, f"❌ Ошибка: {e}")
                except Exception:
                    pass
                log(f"❌ Ошибка: {e}")
                log("")
        time.sleep(0.5)

if __name__ == "__main__":
    main()
