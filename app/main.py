import io, os, json, uuid, hashlib, datetime as dt
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient

app = FastAPI()
API_TOKEN = os.getenv("API_TOKEN")

blob = BlobServiceClient.from_connection_string(os.environ["BLOB_CONN_STR"])
container = blob.get_container_client(os.environ.get("BLOB_CONTAINER", "images"))

cosmos = CosmosClient(os.environ["COSMOS_ENDPOINT"], os.environ["COSMOS_KEY"])
db = cosmos.get_database_client(os.environ.get("COSMOS_DB", "cv"))
coll = db.get_container_client(os.environ.get("COSMOS_CONTAINER", "ingest"))

ALLOWED = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp"}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/ingest")
async def ingest(
    file: UploadFile = File(...),
    meta: str = Form(...),
    authorization: str | None = Header(default=None),
):
    if API_TOKEN:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "missing bearer")
        if authorization.split(" ", 1)[1] != API_TOKEN:
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

    md5 = hashlib.md5(data).hexdigest()
    path = f"ingest/{dt.datetime.utcnow():%Y/%m/%d}/{uuid.uuid4().hex}{ALLOWED[file.content_type]}"

    container.upload_blob(
        name=path,
        data=io.BytesIO(data),
        overwrite=False,
        content_settings=ContentSettings(content_type=file.content_type),
    )

    doc = {
        "id": uuid.uuid4().hex,
        "blob_account": os.environ.get("BLOB_ACCOUNT"),
        "blob_container": os.environ.get("BLOB_CONTAINER", "images"),
        "blob_path": path,
        "size": len(data),
        "content_type": file.content_type,
        "md5": md5,
        "meta": meta_obj,
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "camera_id": str(meta_obj.get("camera_id", "unknown")),
    }
    coll.upsert_item(doc)
    return JSONResponse({"ok": True, "blob_path": path, "doc_id": doc["id"], "md5": md5})

