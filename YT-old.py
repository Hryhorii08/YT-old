import os, re, time, json, pickle, tempfile, requests, io, base64
from datetime import datetime

from flask import Flask, request, jsonify

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials as SA_Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# ============================ ENV ============================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_TOKEN      = os.environ.get("WEBHOOK_TOKEN", "")
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "")
SHEET_NAME         = os.environ.get("SHEET_NAME", "Лист1")
TRIGGER_TEXT       = os.environ.get("TRIGGER_TEXT", "1")

# Эти две переменные содержат ИМЕННО JSON строкой
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "").strip()
CLIENT_SECRET_FILE   = os.environ.get("CLIENT_SECRET_FILE", "").strip()

# token.pickle — путь на диске
TOKEN_FILE         = os.environ.get("TOKEN_FILE", "/opt/render/project/src/token.pickle")
YOUTUBE_TOKEN_B64  = os.environ.get("YOUTUBE_TOKEN_B64", "")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Статичные настройки
COL_VIDEO = "A"
COL_TITLE = "B"
COL_DESC  = "C"
DELETE_FIRST_ROW_AFTER_SUCCESS = True

YOUTUBE_CATEGORY_ID = "22"
YOUTUBE_DEFAULT_VISIBILITY = "public"
YOUTUBE_MADE_FOR_KIDS = False
YOUTUBE_DEFAULT_TAGS = ["Shorts"]
# ============================================================

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} - {msg}", flush=True)

def ensure_env():
    miss = []
    for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "WEBHOOK_TOKEN": WEBHOOK_TOKEN,
        "SPREADSHEET_ID": SPREADSHEET_ID,
        "SHEET_NAME": SHEET_NAME,
        "SERVICE_ACCOUNT_FILE": SERVICE_ACCOUNT_FILE,
        "CLIENT_SECRET_FILE": CLIENT_SECRET_FILE,
    }.items():
        if not v:
            miss.append(k)
    if miss:
        for k in miss:
            log(f"❌ Отсутствует переменная окружения: {k}")
        raise SystemExit(1)

# Восстановить token.pickle из base64 при старте (если надо)
def maybe_restore_token():
    if YOUTUBE_TOKEN_B64 and not os.path.exists(TOKEN_FILE):
        try:
            os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
        except Exception:
            pass
        with open(TOKEN_FILE, "wb") as f:
            f.write(base64.b64decode(YOUTUBE_TOKEN_B64))
        log(f"Создан token.pickle по пути: {TOKEN_FILE}")

# ====================== Google Auth helpers ======================
SCOPES_YT = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
]
SCOPES_SA = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

def _sa_credentials():
    # SERVICE_ACCOUNT_FILE содержит JSON одной строкой
    if SERVICE_ACCOUNT_FILE.startswith("{"):
        return SA_Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_FILE), scopes=SCOPES_SA)
    # на всякий случай поддержим путь к файлу
    return SA_Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES_SA)

def _oauth_flow():
    # CLIENT_SECRET_FILE содержит JSON одной строкой
    if CLIENT_SECRET_FILE.startswith("{"):
        return InstalledAppFlow.from_client_config(json.loads(CLIENT_SECRET_FILE), SCOPES_YT)
    # на всякий случай поддержим путь к файлу
    return InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES_YT)

_DRIVE = None
def drive_service():
    global _DRIVE
    if _DRIVE is None:
        _DRIVE = build("drive", "v3", credentials=_sa_credentials(), cache_discovery=False)
    return _DRIVE

def sheets_service():
    return build("sheets", "v4", credentials=_sa_credentials(), cache_discovery=False)

def youtube_service():
    # На сервере не запускаем интерактивную авторизацию — ожидаем готовый token.pickle
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("Отсутствует token.pickle. Задайте YOUTUBE_TOKEN_B64 или загрузите файл в Secret Files и укажите TOKEN_FILE.")
    with open(TOKEN_FILE, "rb") as f:
        creds = pickle.load(f)

    if not getattr(creds, "valid", False):
        if getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
            creds.refresh(Request())
            with open(TOKEN_FILE, "wb") as f:
                pickle.dump(creds, f)
        else:
            # как fallback ДЛЯ ЛОКАЛЬНОГО ЗАПУСКА можно раскомментировать 2 строки ниже,
            # но на сервере Render это не сработает:
            # flow = _oauth_flow()
            # creds = flow.run_local_server(port=0)
            raise RuntimeError("Невозможно обновить OAuth токен. Переавторизуйтесь локально и обновите YOUTUBE_TOKEN_B64.")
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

# ====================== Sheets helpers ======================
def _normalize_sheet_id(x: str) -> str:
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", x)
    return m.group(1) if m else x

SPREADSHEET_ID = _normalize_sheet_id(SPREADSHEET_ID)

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

# =================== Video source (Drive-safe) ===================
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

# =================== YouTube upload ===================
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

# =================== Telegram helpers ===================
def tg_send(chat_id: int, text: str):
    try:
        requests.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=30)
    except Exception as e:
        log(f"❌ Ошибка: отправка сообщения в Telegram: {e}")

# =================== Core job ===================
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

# =================== Flask app (webhook) ===================
app = Flask(__name__)

@app.route("/", methods=["GET"])
def root():
    return "ok", 200

@app.route("/webhook/<token>", methods=["POST"])
def webhook(token):
    if token != WEBHOOK_TOKEN:
        return "not found", 404

    upd = request.get_json(silent=True) or {}
    log("Новый запрос")
    log(f"update: {upd}")

    msg = upd.get("message") or upd.get("channel_post") or {}
    chat = msg.get("chat", {}) or {}
    chat_id = chat.get("id")
    text = msg.get("text")

    if chat_id is None:
        log("❌ Ошибка: отсутствует chat_id"); log("")
        return jsonify({"ok": True}), 200

    if text is None or text.strip() == "":
        tg_send(chat_id, "❌ Ошибка: пустая строка")
        log("❌ Ошибка: пустая строка"); log("")
        return jsonify({"ok": True}), 200

    if text.strip() != TRIGGER_TEXT:
        tg_send(chat_id, "Код ничего не активирует")
        log("Код ничего не активирует"); log("")
        return jsonify({"ok": True}), 200

    tg_send(chat_id, "Старт публикации…")
    rep = process_once()
    status = rep.get("status")

    if status == "OK":
        vid = rep["video_id"]
        tg_send(chat_id, f"Создано видео, ID: {vid}")
        log(f"Создано видео, ID: {vid}"); log("")
    elif status == "UPLOAD_LIMIT":
        tg_send(chat_id, "Лимит отправки видео на YouTube")
        log("Лимит отправки видео на YouTube"); log("")
    elif status == "EMPTY_SHEET":
        tg_send(chat_id, "❌ Ошибка: нет данных в таблице")
        log("❌ Ошибка: нет данных в таблице"); log("")
    elif status == "SHEETS_ACCESS_ERROR":
        tg_send(chat_id, f"❌ Ошибка: доступ к таблице: {rep.get('error')}")
        log(f"❌ Ошибка: доступ к таблице: {rep.get('error')}"); log("")
    elif status == "DOWNLOAD_ERROR":
        tg_send(chat_id, f"❌ Ошибка: загрузка видео: {rep.get('error')}")
        log(f"❌ Ошибка: загрузка видео: {rep.get('error')}"); log("")
    elif status == "YOUTUBE_AUTH_ERROR":
        tg_send(chat_id, f"❌ Ошибка: авторизация YouTube: {rep.get('error')}")
        log(f"❌ Ошибка: авторизация YouTube: {rep.get('error')}"); log("")
    elif status == "ROW_DELETE_ERROR":
        vid = rep.get("video_id", "")
        tg_send(chat_id, f"❌ Ошибка: удаление строки: {rep.get('error')} (Видео загружено: {vid})")
        log(f"❌ Ошибка: удаление строки: {rep.get('error')} (Видео загружено: {vid})"); log("")
    else:
        tg_send(chat_id, f"❌ Ошибка: неизвестный статус: {status}")
        log(f"❌ Ошибка: неизвестный статус: {status}"); log("")

    return jsonify({"ok": True}), 200

if __name__ == "__main__":
    ensure_env()
    maybe_restore_token()
    log("Сценарий запущен")
    # Render сам выставит PORT; fallback 10000
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
