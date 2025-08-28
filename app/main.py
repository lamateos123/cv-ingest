import os, json, uuid
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header, Depends
from fastapi.responses import JSONResponse
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.cosmos import CosmosClient, PartitionKey

app = FastAPI()

def get_api_token() -> str:
    tok = os.environ.get("API_TOKEN")
    if not tok:
        raise RuntimeError("API_TOKEN not set")
    return tok

def bearer_auth(authorization: str | None = Header(default=None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing bearer")
    supplied = authorization.split(" ", 1)[1]
    if supplied != get_api_token():
        raise HTTPException(status_code=403, detail="bad token")

def get_blob_clients():
    # Prefer explicit connection string
    conn = os.environ.get("BLOB_CONN_STR") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn:
        raise RuntimeError("Blob connection string missing (BLOB_CONN_STR or AZURE_STORAGE_CONNECTION_STRING)")
    svc = BlobServiceClient.from_connection_string(conn)
    container_name = os.environ.get("BLOB_CONTAINER", "images")
    container = svc.get_container_client(container_name)
    try:
        container.create_container()  # idempotent
    except Exception:
        pass
    return container

def get_cosmos_container():
    ep = os.environ.get("COSMOS_ENDPOINT")
    key = os.environ.get("COSMOS_KEY")
    if not ep or not key:
        return None
    client = CosmosClient(ep, key)
    db = client.create_database_if_not_exists(id=os.environ.get("COSMOS_DB", "cv"))
    cont = db.create_container_if_not_exists(
        id=os.environ.get("COSMOS_CONTAINER", "ingest"),
        partition_key=PartitionKey(path="/barcode"),
        offer_throughput=400,
    )
    return cont

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.post("/ingest", dependencies=[Depends(bearer_auth)])
async def ingest(file: UploadFile = File(...), meta: str = Form(...)):
    # Parse meta JSON (expects camera_id, ts (ISO), barcode)
    try:
        m = json.loads(meta)
    except Exception:
        raise HTTPException(status_code=400, detail="meta must be JSON")

    camera = (m.get("camera_id") or "unknown").strip()
    ts_raw = (m.get("ts") or "").strip()
    barcode = (m.get("barcode") or "").strip()

    # sanitize for name
    ext = Path(file.filename or "").suffix or ".bin"
    safe_ts = ts_raw.replace(":", "").replace("/", "-").replace(" ", "T")
    blob_name = f"{camera}-{safe_ts}-{barcode}-{uuid.uuid4().hex}{ext}"

    data = await file.read()

    # Upload to Blob
    container = get_blob_clients()
    container.upload_blob(
        name=blob_name,
        data=data,
        overwrite=True,
        content_settings=ContentSettings(content_type=file.content_type or "application/octet-stream"),
    )

    # Write metadata to Cosmos (best-effort; blob already uploaded)
    cont = get_cosmos_container()
    doc = {
        "id": str(uuid.uuid4()),
        "camera_id": camera,
        "ts": ts_raw or datetime.utcnow().isoformat() + "Z",
        "barcode": barcode or "unknown",
        "blob": blob_name,
        "size": len(data),
    }
    if cont:
        cont.upsert_item(doc)

    return JSONResponse({"ok": True, "blob": blob_name, "doc": doc})
