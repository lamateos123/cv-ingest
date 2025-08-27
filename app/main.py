import io, os, json, uuid, hashlib, datetime as dt
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient

app = FastAPI()

ALLOWED = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}

def need_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise HTTPException(500, f"Server missing required env var: {name}")
    return v

def get_blob_container():
    conn = need_env("BLOB_CONN_STR")
    container_name = os.getenv("BLOB_CONTAINER", "images")
    svc = BlobServiceClient.from_connection_string(conn)
    return svc.get_container_client(container_name)

def get_cosmos_container():
    ep = need_env("COSMOS_ENDPOINT")
    key = need_env("COSMOS_KEY")
    dbname = os.getenv("COSMOS_DB", "cv")
    cname = os.getenv("COSMOS_CONTAINER", "ingest")
    client = CosmosClient(ep, key)
    db = client.get_database_client(dbname)
    return db.get_container_client(cname)

@app.get("/healthz")
def healthz():
    # no Azure calls here; always up if the server runs
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    file: UploadFile = File(...),
    meta: str = Form(...),
    authorization: str | None = Header(default=None),
):
    api_token = os.getenv("API_TOKEN")
    if api_token:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "missing bearer")
        if authorization.split(" ", 1)[1] != api_token:
            raise HTTPException(403, "bad token")

    if file.content_type not in ALLOWED:
        raise HTTPException(415, "unsupported content type")

    try:
        meta_obj = json.loads(meta)
    except Exception:
        raise HTTPException(400, "meta must be JSON")

    data = await file.read()
    if not data:
        raise HTTPException(400, "empty file")

    ext = ALLOWED[file.content_type]
    md5 = hashlib.md5(data).hexdigest()
    path = f"ingest/{dt.datetime.utcnow():%Y/%m/%d}/{uuid.uuid4().hex}{ext}"

    # do the Azure work only here
    blob_container = get_blob_container()
    blob_container.upload_blob(
        name=path,
        data=io.BytesIO(data),
        overwrite=False,
        content_settings=ContentSettings(content_type=file.content_type),
    )

    cosmos_container = get_cosmos_container()
    doc = {
        "id": uuid.uuid4().hex,
        "blob_account": os.getenv("BLOB_ACCOUNT"),
        "blob_container": os.getenv("BLOB_CONTAINER", "images"),
        "blob_path": path,
        "size": len(data),
        "content_type": file.content_type,
        "md5": md5,
        "meta": meta_obj,
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "camera_id": str(meta_obj.get("camera_id", "unknown")),
    }
    cosmos_container.upsert_item(doc)
    return JSONResponse({"ok": True, "blob_path": path, "doc_id": doc["id"], "md5": md5})
