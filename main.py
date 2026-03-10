"""
oneaether.ai Agent Server
- SQLite database for persistent storage
- Live customer/product/order sync from SellnChill
- WhatsApp intent detection + order creation
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("oneaether")

# ─── Database ─────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "/app/oneaether.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    db = get_db()
    # Migrate: drop old tables if schema changed (detected by missing columns)
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(customers)").fetchall()]
        if "price_tier" not in cols or "outstanding_amount" not in cols:
            logger.info("Migrating customers table to new schema...")
            db.execute("DROP TABLE IF EXISTS customers")
            db.commit()
    except Exception as e:
        logger.warning("Migration check failed: %s", e)
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(orders)").fetchall()]
        if "item_info" not in cols or "paid_status" not in cols:
            logger.info("Migrating orders table to new schema...")
            db.execute("DROP TABLE IF EXISTS orders")
            db.commit()
    except Exception as e:
        logger.warning("Orders migration check failed: %s", e)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS credentials (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            uts   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS customers (
            customer_id   TEXT PRIMARY KEY,
            store_id      TEXT,
            customer_name TEXT,
            phone         TEXT,
            email         TEXT,
            store         TEXT,
            branch_id     TEXT,
            branch_name   TEXT,
            address       TEXT,
            credit_term   TEXT,
            status        TEXT DEFAULT 'Active',
            raw           TEXT,
            synced_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS products (
            item_id     TEXT PRIMARY KEY,
            sku         TEXT,
            name        TEXT,
            uom         TEXT,
            uom_id      TEXT,
            price       REAL DEFAULT 0,
            tax_rate    REAL DEFAULT 9,
            tax_code    TEXT DEFAULT 'SR9',
            tax_code_id TEXT,
            status      TEXT DEFAULT 'Active',
            raw         TEXT,
            synced_at   TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id      TEXT PRIMARY KEY,
            order_number  TEXT UNIQUE,
            customer_id   TEXT,
            store_id      TEXT,
            status        TEXT,
            total_amount  REAL DEFAULT 0,
            delivery_date TEXT,
            chat_id       TEXT,
            raw           TEXT,
            synced_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS chat_assignments (
            chat_id     TEXT PRIMARY KEY,
            customer_id TEXT,
            customer    TEXT,
            store_id    TEXT,
            branch_id   TEXT,
            branch      TEXT,
            updated_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS conversation_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id    TEXT,
            role       TEXT,
            message    TEXT,
            intent     TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    db.commit()
    db.close()
    logger.info("Database initialised at %s", DB_PATH)

def db_set_credential(key: str, value: str):
    db = get_db()
    db.execute("INSERT OR REPLACE INTO credentials(key,value,uts) VALUES(?,?,datetime('now'))", (key, value))
    db.commit(); db.close()

# SNC account constants (from Postman collection)
SNC_ENC_USERNAME = "gAAAAABoqEPZGYCgChT1WhZBGw27VZsOAPIfR4GuBCjd0YsYl5115GocjXjPKYWEQjZVvbfdL5Ibsf5_7KomKdn5Xqm3A9vmYXIoudeREWERO8-D_1OkoSc="
SNC_ENC_PASSWORD = "gAAAAABoqEPZbvRypzhBV1omAh9YOWFUjCZJRvJ8Ub2Nnjzv0jCMDFWrZNiFGPPdRQA6Vs1LbPTIvN-EE_HcXm8UXia5u2w93w=="
SNC_HARDCODED_USER_ID  = "1qxcb0ssTRGPQaKogvtkMw"
SNC_HARDCODED_USERNAME = "admin@mindmastersg.com"
SNC_HARDCODED_COMPANY  = "mindmasters"

async def auto_refresh_token() -> bool:
    """Login to SNC using stored encrypted credentials to get a fresh token."""
    api_url = credentials["snc"].get("api_url", "https://enterprise.sellnchill.com/api")
    enc_user = os.getenv("SNC_ENC_USERNAME", SNC_ENC_USERNAME)
    enc_pass = os.getenv("SNC_ENC_PASSWORD", SNC_ENC_PASSWORD)
    logger.info("Auto-refreshing SNC token...")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(f"{api_url}/login", data={
                "username":     enc_user,
                "password":     enc_pass,
                "grant_type":   "password",
                "notification": "false",
            })
            logger.info("Auto-login response: HTTP %s", resp.status_code)
            if resp.status_code in (200, 201):
                data = resp.json()
                token = data.get("access_token", "")
                if token:
                    # SNC login response keys vary — log to find user_id location
                    logger.info("Login response keys: %s", list(data.keys()))
                    logger.info("Login response snippet: %s", str(data)[:300])
                    # Try all known locations for user_id
                    # JWT and login response don't contain user_id — use known constant
                    uid = SNC_HARDCODED_USER_ID
                    credentials["snc"]["access_token"] = token
                    credentials["snc"]["user_id"]      = uid
                    credentials["snc"]["username"]     = data.get("username","") or credentials["snc"].get("username","")
                    db_set_credential("snc_token",    token)
                    db_set_credential("snc_user_id",  uid)
                    db_set_credential("snc_username", credentials["snc"]["username"])
                    logger.info("Token refreshed! user_id=%s username=%s", uid, credentials["snc"]["username"])
                    return True
            logger.error("Auto-login failed: %s %s", resp.status_code, resp.text[:200])
            return False
    except Exception as e:
        logger.error("Auto-login exception: %s", e)
        return False

def db_get_credential(key: str) -> Optional[str]:
    db = get_db()
    row = db.execute("SELECT value FROM credentials WHERE key=?", (key,)).fetchone()
    db.close()
    return row["value"] if row else None

def db_load_all_credentials() -> Dict:
    db = get_db()
    rows = db.execute("SELECT key,value FROM credentials").fetchall()
    db.close()
    return {r["key"]: r["value"] for r in rows}

def db_save_assignment(chat_id: str, data: Dict):
    db = get_db()
    db.execute("""INSERT OR REPLACE INTO chat_assignments
        (chat_id,customer_id,customer,store_id,branch_id,branch,updated_at)
        VALUES(?,?,?,?,?,?,datetime('now'))""",
        (chat_id, data.get("customer_id",""), data.get("customer",""),
         data.get("store_id",""), data.get("branch_id",""), data.get("branch","")))
    db.commit(); db.close()

def db_get_assignment(chat_id: str) -> Optional[Dict]:
    db = get_db()
    row = db.execute("SELECT * FROM chat_assignments WHERE chat_id=?", (chat_id,)).fetchone()
    db.close()
    return dict(row) if row else None

def db_get_all_assignments() -> Dict:
    db = get_db()
    rows = db.execute("SELECT * FROM chat_assignments").fetchall()
    db.close()
    return {r["chat_id"]: dict(r) for r in rows}

def db_log_message(chat_id: str, role: str, message: str, intent: str = ""):
    db = get_db()
    db.execute("INSERT INTO conversation_log(chat_id,role,message,intent) VALUES(?,?,?,?)",
               (chat_id, role, message, intent))
    db.commit(); db.close()

def db_upsert_customers(customers: List[Dict]):
    db = get_db()
    for c in customers:
        # customer_id field is display code (e.g. "121212")
        # Use it as primary key since it's the stable identifier
        cid   = c.get("customer_id","")
        if not cid: continue
        name  = c.get("customer_name","")
        b2b   = c.get("b2b_settings") or {}
        stores = b2b.get("stores") or []

        # Extract first store/branch
        store_id    = (stores[0].get("store_id","")    if stores else "") or c.get("store_id","")
        branch_id   = (stores[0].get("branch_id","")   if stores else "") or c.get("branch_id","")
        branch_name = (stores[0].get("branch_name","") if stores else "") or c.get("branch_name","")

        # Credit term
        ct_obj      = b2b.get("credit_term") or {}
        credit_term = ct_obj.get("credit_term","") or c.get("credit_term","")
        credit_term_id = ct_obj.get("credit_term_id","")

        # Price tier
        pt_obj      = b2b.get("price_tier") or {}
        price_tier  = pt_obj.get("name","")

        # Financial
        outstanding = b2b.get("outstanding_amount", 0) or 0
        max_credit  = b2b.get("max_credit_limit", 0) or 0
        min_order   = b2b.get("min_order_amount", 0) or 0

        # Tax / currency
        tax_obj     = b2b.get("tax_code") or {}
        tax_code    = tax_obj.get("tax_code","") if isinstance(tax_obj,dict) else str(tax_obj)
        currency    = b2b.get("currency","SGD") or "SGD"
        if isinstance(currency, dict): currency = currency.get("currency","SGD")

        # Payment / sales
        pm_obj       = b2b.get("payment_mode") or {}
        payment_mode = pm_obj.get("payment_mode","") if isinstance(pm_obj,dict) else ""
        sp_obj       = b2b.get("sales_person") or {}
        sales_person = sp_obj.get("username","") if isinstance(sp_obj,dict) else ""

        # Contact info
        phone = c.get("customer_phone","") or c.get("phone","")
        email = c.get("customer_email","") or c.get("email","")

        db.execute("""INSERT OR REPLACE INTO customers (
            customer_id, customer_code, customer_name, customer_type,
            phone, email, uen,
            store_id, store, branch_id, branch_name,
            address, credit_term, credit_term_id, price_tier,
            outstanding_amount, max_credit_limit, min_order_amount,
            tax_code, currency, status, billing_id, payment_mode, sales_person,
            b2b_stores, raw, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
        (
            cid,
            cid,  # customer_code same as customer_id display code
            name,
            c.get("customer_type",""),
            phone, email,
            c.get("customer_uen",""),
            store_id, name, branch_id, branch_name,
            json.dumps(c.get("address",{})),
            credit_term, credit_term_id, price_tier,
            outstanding, max_credit, min_order,
            tax_code, currency,
            c.get("status","Active"),
            c.get("billing_id",""),
            payment_mode, sales_person,
            json.dumps(stores),
            json.dumps(c),
        ))
    db.commit(); db.close()
    logger.info("Upserted %d customers", len(customers))

def db_upsert_products(products: List[Dict]):
    db = get_db()
    for p in products:
        prices = p.get("prices", [])
        price  = prices[0].get("price", 0) if prices else 0
        db.execute("""INSERT OR REPLACE INTO products
            (item_id,sku,name,uom,uom_id,price,tax_rate,tax_code,tax_code_id,status,raw,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
            (p.get("item_id",""), p.get("sku",""), p.get("name",""),
             p.get("uom",""), p.get("uom_id",""), price,
             p.get("tax_rate",9), p.get("tax_code","SR9"),
             p.get("tax_code_id",""), p.get("status","Active"), json.dumps(p)))
    db.commit(); db.close()
    logger.info("Upserted %d products", len(products))

def db_upsert_orders(orders: List[Dict]):
    db = get_db()
    for o in orders:
        db.execute("""INSERT OR REPLACE INTO orders (
            order_id, order_number, customer_id, store_id, store,
            branch_id, branch_name, status, paid_status,
            delivery_date, delivery_type,
            item_total, tax_amount, total_amount, items_count,
            item_info, chat_id, platform, source, created_by,
            raw, synced_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
        (
            o.get("order_id","") or o.get("_id",""),
            o.get("order_number",""),
            o.get("customer_id","") or o.get("store_id",""),
            o.get("store_id",""),
            o.get("store",""),
            o.get("branch_id",""),
            o.get("branch_name",""),
            o.get("order_status",""),
            o.get("paid_status",""),
            o.get("delivery_date",""),
            o.get("delivery_type","Delivery"),
            o.get("item_total",0),
            o.get("tax_amount",0),
            o.get("total_amount",0),
            o.get("items_count",0),
            json.dumps(o.get("item_info",[])),
            o.get("whatsapp_contact_id","") or o.get("chat_id",""),
            o.get("platform",""),
            o.get("source",""),
            o.get("created_by",""),
            json.dumps(o),
        ))
    db.commit(); db.close()

# ─── Global state ─────────────────────────────────────────────────────────────
credentials: Dict[str, Any] = {
    "snc": {
        "api_url":      os.getenv("SNC_API_URL", "https://enterprise.sellnchill.com/api"),
        "access_token": os.getenv("SNC_TOKEN", ""),
        "user_id":      os.getenv("SNC_USER_ID", ""),
        "username":     os.getenv("SNC_USERNAME", ""),
        "company_id":   os.getenv("SNC_COMPANY_ID", ""),
        "timezone":     os.getenv("SNC_TIMEZONE", "Asia/Singapore"),
    },
    "whapi": {
        "base_url":   os.getenv("WHAPI_BASE_URL", "https://gate.whapi.cloud"),
        "token":      os.getenv("WHAPI_TOKEN", ""),
        "channel_id": os.getenv("WHAPI_CHANNEL_ID", ""),
    },
    "agent": {
        "enabled":       os.getenv("AGENT_ENABLED", "true").lower() == "true",
        "anthropic_key": os.getenv("ANTHROPIC_API_KEY", ""),
        "openai_key":    os.getenv("OPENAI_API_KEY", ""),
    }
}

conversation_memory: Dict[str, List[Dict]] = {}

def load_credentials_from_db():
    """Load persisted credentials from DB into memory on startup."""
    try:
        saved = db_load_all_credentials()
        if saved.get("snc_api_url"):      credentials["snc"]["api_url"]      = saved["snc_api_url"]
        if saved.get("snc_token"):        credentials["snc"]["access_token"]  = saved["snc_token"]
        if saved.get("snc_user_id"):      credentials["snc"]["user_id"]       = saved["snc_user_id"]
        if saved.get("snc_username"):     credentials["snc"]["username"]      = saved["snc_username"]
        if saved.get("snc_company_id"):   credentials["snc"]["company_id"]    = saved["snc_company_id"]
        if saved.get("whapi_token"):      credentials["whapi"]["token"]       = saved["whapi_token"]
        if saved.get("whapi_base_url"):   credentials["whapi"]["base_url"]    = saved["whapi_base_url"]
        if saved.get("whapi_channel_id"): credentials["whapi"]["channel_id"]  = saved["whapi_channel_id"]
        if saved.get("anthropic_key"):    credentials["agent"]["anthropic_key"]= saved["anthropic_key"]
        if saved.get("openai_key"):       credentials["agent"]["openai_key"]  = saved["openai_key"]
        logger.info("Credentials loaded from DB")
    except Exception as e:
        logger.warning("Could not load credentials from DB: %s", e)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    load_credentials_from_db()
    logger.info("oneaether.ai starting on port %s", os.getenv("PORT", 8000))
    # Auto-refresh token on startup
    if not credentials["snc"].get("access_token"):
        await auto_refresh_token()
    else:
        # Verify token is still valid, refresh if not
        asyncio.create_task(_verify_and_refresh_token())
    logger.info("Agent: %s | AI: %s",
        "ON" if credentials["agent"]["enabled"] else "OFF",
        "Claude" if credentials["agent"]["anthropic_key"] else
        "OpenAI" if credentials["agent"]["openai_key"] else "rule-based")
    yield

async def _verify_and_refresh_token():
    """Check if current token is valid, refresh if expired."""
    api_url = credentials["snc"].get("api_url","")
    token   = credentials["snc"].get("access_token","")
    if not api_url or not token:
        await auto_refresh_token()
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{api_url}/health",
                headers={"Authorization": f"Bearer {token}"})
            if resp.status_code == 401:
                logger.info("Token expired on startup — refreshing")
                await auto_refresh_token()
    except Exception:
        pass

app = FastAPI(title="oneaether.ai", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# ─── Pydantic models ──────────────────────────────────────────────────────────
class CredentialsPayload(BaseModel):
    snc:   Optional[Dict[str, str]] = None
    whapi: Optional[Dict[str, str]] = None
    agent: Optional[Dict[str, str]] = None

class SendMessagePayload(BaseModel):
    contact_id:   str
    body:         str
    type:         str = "text"
    phone_number: Optional[str] = None

# ─── SNC helpers ──────────────────────────────────────────────────────────────
def snc_headers():
    return {"Authorization": f"Bearer {credentials['snc'].get('access_token','')}", "Content-Type": "application/json"}

def snc_base():
    # user_id is required by SNC — decode from JWT token if not set
    user_id  = credentials["snc"].get("user_id","") or os.getenv("SNC_USER_ID","") or SNC_HARDCODED_USER_ID
    username = credentials["snc"].get("username","") or os.getenv("SNC_USERNAME","") or SNC_HARDCODED_USERNAME
    if not credentials["snc"].get("user_id",""):
        try:
            import base64, json as _json
            token = credentials["snc"].get("access_token","")
            payload = token.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            decoded = _json.loads(base64.b64decode(payload))
            username = username or decoded.get("username","")
            # SNC JWT doesn't include user_id directly — extract from username
            logger.warning("user_id missing from credentials — JWT: %s", decoded)
        except Exception:
            pass
    return {
        "company_id":   credentials["snc"].get("company_id",""),
        "user_id":      user_id,
        "username":     username,
        "timezone":     credentials["snc"].get("timezone","Asia/Singapore"),
        "request_from": "WEB",
    }

def whapi_headers():
    return {"Authorization": f"Bearer {credentials['whapi'].get('token','')}", "Content-Type": "application/json"}

async def poll_job(job_id: str, timeout: int = 25) -> Optional[Dict]:
    api_url = credentials["snc"].get("api_url","")
    if not api_url: return None
    deadline = time.time() + timeout
    attempt  = 0
    while time.time() < deadline:
        attempt += 1
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.get(f"{api_url}/job/{job_id}", headers=snc_headers())
                data = resp.json()
            process_status = data.get("process_status","")
            logger.info("poll_job %s attempt=%d status=%s", job_id, attempt, process_status)
            is_processed = process_status in ("processed","completed","done","success")
            has_result   = data.get("result") is not None
            has_order    = bool((data.get("result") or {}).get("order_number"))
            if is_processed or has_result or has_order:
                return data
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning("poll_job %s error: %s", job_id, e)
            await asyncio.sleep(2)
    logger.error("poll_job %s timed out after %ds", job_id, timeout)
    return None

async def snc_call(endpoint: str, body: Dict) -> Optional[Dict]:
    api_url = credentials["snc"].get("api_url","")
    token   = credentials["snc"].get("access_token","")
    if not api_url or not token:
        logger.error("snc_call %s: missing api_url or token", endpoint)
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{api_url}{endpoint}",
                json={**snc_base(), **body},
                headers=snc_headers()
            )
            logger.info("snc_call %s → HTTP %s", endpoint, resp.status_code)
            if resp.status_code == 401:
                logger.warning("snc_call %s got 401 — attempting token refresh", endpoint)
                refreshed = await auto_refresh_token()
                if refreshed:
                    # Retry with new token
                    resp = await client.post(
                        f"{api_url}{endpoint}",
                        json={**snc_base(), **body},
                        headers=snc_headers()
                    )
                    logger.info("snc_call %s retry → HTTP %s", endpoint, resp.status_code)
                else:
                    logger.error("Token refresh failed — cannot retry")
                    return None
            if resp.status_code not in (200, 201):
                logger.error("snc_call %s failed: %s", endpoint, resp.text[:300])
                return None
            data = resp.json()
            status = data.get("status", {})
            logger.info("snc_call %s status=%s job_id=%s", endpoint, status, data.get("job_id"))
            # SNC returns job_id for async operations — poll for result
            job_id = data.get("job_id")
            if job_id:
                result = await poll_job(job_id)
                logger.info("snc_call %s poll result keys=%s snippet=%s",
                            endpoint, list(result.keys()) if result else None,
                            str(result)[:300] if result else None)
                return result
            # Some endpoints return result directly
            if data.get("result") is not None:
                return data
            # If status.success is False, log and return None
            if status and not status.get("success", True):
                logger.error("snc_call %s: success=false msg=%s", endpoint, status.get("msg",""))
                return None
            return data
    except Exception as e:
        logger.error("snc_call %s exception: %s", endpoint, e)
        return None

async def whapi_send(to: str, body: str) -> bool:
    token    = credentials["whapi"].get("token","")
    base_url = credentials["whapi"].get("base_url","https://gate.whapi.cloud")
    if not token:
        logger.error("whapi_send: no token")
        return False
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(f"{base_url}/messages/text",
                json={"to": to, "body": body}, headers=whapi_headers())
            ok = resp.status_code in (200,201) or resp.json().get("sent") or bool(resp.json().get("id"))
            logger.info("whapi_send to=%s ok=%s", to, ok)
            return ok
    except Exception as e:
        logger.error("whapi_send: %s", e)
        return False

# ─── Sync functions ───────────────────────────────────────────────────────────
async def sync_customers_from_snc(page: int = 1, per_page: int = 100) -> int:
    """Pull all customers from SNC and store in DB."""
    result = await snc_call("/customers/get", {
        "custom": True,
        "data": {"filter_by": {
            "company_id": SNC_HARDCODED_COMPANY,
            "search_on": ["customer_name","customer_phone","customer_email"],
            "search_text": "", "exact_match": False,
            "pagination": {"page_no": page, "no_of_recs": per_page, "sort_by": "cts", "order_by": False},
        }}
    })
    if not result:
        logger.error("sync_customers: snc_call returned None")
        return 0
    # SNC returns: result.metadata.CustomersList
    r = result.get("result", {})
    metadata = r.get("metadata", {})
    customers = (
        metadata.get("CustomersList") or
        metadata.get("customers") or
        r.get("data", {}).get("CustomersList") or
        r.get("CustomersList") or
        (r.get("data") if isinstance(r.get("data"), list) else []) or
        []
    )
    logger.info("sync_customers page=%d found=%d total=%s", page, len(customers), metadata.get("total_count"))
    if customers:
        db_upsert_customers(customers)
    return len(customers)

async def sync_products_from_snc(page: int = 1, per_page: int = 100) -> int:
    """Pull all products from SNC and store in DB."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [],
        "search_on": ["name","sku","product_code"],
        "confidence": 0.5,
        "search_text": [],
        "exact_match": False,
        "pagination": {"page_no": page, "no_of_recs": per_page, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": ["short_description","description","sku","short_name","name",
            "parent_sku","product_code","category","sub_category","brand",
            "uom","base_uom","prices","images","status","is_sellable","is_edited",
            "b2b_enabled","pos_enabled","is_primary","merged_id","merged_skus",
            "bom_type","linked_stores","inventory_type","quantity",
            "uom_id","config","attribute_set"],
        "merged": True, "bundles": False,
    }}})
    if not result:
        logger.error("sync_products: snc_call returned None")
        return 0
    r = result.get("result", {})
    meta = r.get("metadata", {})
    # Try all known SNC response paths
    products = (
        meta.get("ProductList") or meta.get("products") or meta.get("Items") or
        r.get("data", {}).get("ProductList") if isinstance(r.get("data"), dict) else None or
        (r.get("data") if isinstance(r.get("data"), list) else None) or
        r.get("ProductList") or r.get("products") or []
    )
    if not isinstance(products, list): products = []
    logger.info("sync_products page=%d found=%d meta_keys=%s", page, len(products), list(meta.keys()))
    if products:
        db_upsert_products(products)
    return len(products)

async def sync_orders_from_snc(days: int = 30) -> int:
    """Pull recent orders from SNC and store in DB."""
    today     = datetime.now().strftime("%d-%m-%Y")
    from_date = (datetime.now() - timedelta(days=days)).strftime("%d-%m-%Y")
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source": "B2B", "platform": "Whatsapp",
        "store_id": [], "branch_id": [], "status": "All",
        "date_range": [from_date, today],
        "search_on": ["order_number"], "search_text": "",
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 200, "sort_by": "order_cts", "order_by": False},
        "whatsapp_contact_id": [], "review_required": False,
    }}})
    if not result: return 0
    orders = result.get("result", {}).get("data", [])
    if orders:
        db_upsert_orders(orders)
    return len(orders)

# ─── Intent Detection ─────────────────────────────────────────────────────────
INTENT_KEYWORDS = {
    "NEW_ORDER":    ["order","i want","i need","can i get","can i order","please send","like to order","want to order","purchase"],
    "AMEND_ORDER":  ["change","amend","modify","update","instead","replace","add more","reduce","increase","decrease"],
    "CANCEL_ORDER": ["cancel","don't want","dont want","stop order","remove order","no longer need","nevermind"],
    "ORDER_STATUS": ["status","where is my order","my order","tracking","when will","check order","delivered yet"],
    "ENQUIRY":      ["price","how much","available","stock","do you have","what is","info","details"],
    "GREETING":     ["hi","hello","hey","good morning","good afternoon","good evening","morning","afternoon"],
}

def detect_intent(text: str) -> Tuple[str, float]:
    t = text.lower()
    scores: Dict[str, int] = {}
    for intent, kws in INTENT_KEYWORDS.items():
        hits = sum(1 for kw in kws if kw in t)
        if hits: scores[intent] = hits
    if not scores: return "UNKNOWN", 0.3
    best = max(scores, key=scores.get)
    return best, min(scores[best] / 3.0, 1.0)

def extract_items(text: str) -> List[Dict]:
    items = []
    pattern = r'(\d+)\s*(boxes?|pcs?|packets?|pkts?|kgs?|units?|bags?|cans?|bottles?)?\s+(?:of\s+)?([a-zA-Z][a-zA-Z\s]{2,30}?)(?=\s+and\b|\s*,|\s*$)'
    for m in re.finditer(pattern, text, re.IGNORECASE):
        qty, uom, name = m.groups()
        name = name.strip().rstrip('.,')
        if name: items.append({"name": name, "qty": int(qty), "uom": uom or "unit"})
    return items

def extract_order_number(text: str) -> Optional[str]:
    m = re.search(r'[A-Z]{2,5}[-/]\d{6,12}[-/]\d{1,5}', text.upper())
    return m.group(0) if m else None

# ─── AI Analysis ──────────────────────────────────────────────────────────────
async def analyze_with_ai(text: str, sender: str, customer: Optional[str], history: List[Dict]) -> Dict:
    anthropic_key = credentials["agent"].get("anthropic_key","")
    openai_key    = credentials["agent"].get("openai_key","")
    intent, confidence = detect_intent(text)
    base = {"intent": intent, "confidence": confidence, "items": extract_items(text),
            "order_number": extract_order_number(text), "reply": None, "method": "rule-based"}
    if not anthropic_key and not openai_key: return base

    ctx = "\n".join([f"{m['role'].upper()}: {m['text']}" for m in history[-4:]]) if history else "None"
    system = """You are a WhatsApp ordering assistant for a B2B food distribution company.
Return ONLY valid JSON — no markdown.
Intent: NEW_ORDER, AMEND_ORDER, CANCEL_ORDER, ORDER_STATUS, ENQUIRY, GREETING, UNKNOWN
{
  "intent": "NEW_ORDER", "confidence": 0.95,
  "items": [{"name": "chicken", "qty": 5, "uom": "unit"}],
  "order_number": null,
  "reply": "friendly reply confirming what you understood"
}"""
    user = f"Sender: {sender}\nCustomer: {customer or 'Unknown'}\nHistory:\n{ctx}\nMessage: \"{text}\""

    if anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post("https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": anthropic_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": "claude-haiku-4-5-20251001", "max_tokens": 400, "system": system,
                          "messages": [{"role": "user", "content": user}]})
                raw = re.sub(r'^```json\s*|\s*```$', '', r.json()["content"][0]["text"].strip())
                result = json.loads(raw)
                result["method"] = "claude"
                return result
        except Exception as e:
            logger.error("Claude error: %s", e)

    if openai_key:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post("https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                    json={"model": "gpt-4o-mini", "max_tokens": 400, "temperature": 0.1,
                          "response_format": {"type": "json_object"},
                          "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
                result = json.loads(r.json()["choices"][0]["message"]["content"])
                result["method"] = "openai"
                return result
        except Exception as e:
            logger.error("OpenAI error: %s", e)
    return base

# ─── Order helpers ────────────────────────────────────────────────────────────
async def lookup_products_for_items(items: List[Dict]) -> List[Dict]:
    enriched = []
    for item in items:
        # First check local DB
        db = get_db()
        row = db.execute("SELECT * FROM products WHERE name LIKE ? OR sku LIKE ? LIMIT 1",
                         (f"%{item['name']}%", f"%{item['name']}%")).fetchone()
        db.close()
        if row:
            enriched.append({**item, "item_id": row["item_id"], "sku": row["sku"],
                "full_name": row["name"], "uom": row["uom"], "uom_id": row["uom_id"],
                "base_uom": row["uom"], "base_uom_id": row["uom_id"],
                "price": row["price"], "tax_rate": row["tax_rate"],
                "tax_code": row["tax_code"], "tax_code_id": row["tax_code_id"], "found": True})
            continue
        # Fall back to SNC API
        result = await snc_call("/products/list", {"data": {"filter_by": {
            "search_text": [item["name"]], "search_on": ["name","sku"],
            "confidence": 0.5, "exact_match": False,
            "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
            "view": "individual", "status": "Active",
            "include_columns": ["name","sku","prices","uom","uom_id","item_id","tax_code","tax_code_id","tax_rate"],
            "merged": True, "bundles": False,
        }}})
        if result and result.get("result",{}).get("data"):
            p = result["result"]["data"][0]
            prices = p.get("prices",[])
            price  = prices[0].get("price",0) if prices else 0
            enriched.append({**item, "item_id": p.get("item_id",""), "sku": p.get("sku",""),
                "full_name": p.get("name",item["name"]), "uom": p.get("uom",item.get("uom","unit")),
                "uom_id": p.get("uom_id",""), "base_uom": p.get("uom","unit"),
                "base_uom_id": p.get("uom_id",""), "price": price,
                "tax_rate": p.get("tax_rate",9), "tax_code": p.get("tax_code","SR9"),
                "tax_code_id": p.get("tax_code_id",""), "found": True})
        else:
            enriched.append({**item, "found": False})
    return enriched

async def push_order_to_snc(items: List[Dict], chat_id: str, delivery_date: str, sender_name: str) -> Optional[str]:
    assignment  = db_get_assignment(chat_id) or {}
    store_id    = assignment.get("store_id","")
    store       = assignment.get("customer","")
    branch_id   = assignment.get("branch_id","") or ""
    branch_name = assignment.get("branch","Main Branch")
    username    = credentials["snc"].get("username","") or SNC_HARDCODED_USERNAME
    user_id     = credentials["snc"].get("user_id","") or SNC_HARDCODED_USER_ID
    company_id  = credentials["snc"].get("company_id","") or SNC_HARDCODED_COMPANY
    delivery    = delivery_date or (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    # Clean branch_id — "main" is a placeholder, not a real SNC ID
    if branch_id in ("main", "Main Branch", ""):
        branch_id = ""
        # Try to get real branch from customers DB
        if store_id:
            db = get_db()
            row = db.execute("SELECT raw FROM customers WHERE customer_id=? OR store_id=?",
                             (store_id, store_id)).fetchone()
            db.close()
            if row:
                try:
                    raw = json.loads(row["raw"])
                    b2b = raw.get("b2b_settings") or {}
                    stores = b2b.get("stores") or []
                    if stores:
                        branch_id   = stores[0].get("branch_id","")
                        branch_name = stores[0].get("branch_name", branch_name)
                        logger.info("Resolved branch from DB: branch_id=%s", branch_id)
                except Exception as e:
                    logger.warning("Branch resolve error: %s", e)

    item_info  = []
    item_total = 0.0
    tax_total  = 0.0

    for it in items:
        qty        = int(it.get("qty", 1) or 1)
        price      = float(it.get("price", 0) or 0)
        tax_rate   = float(it.get("tax_rate", 9) or 9)
        name       = it.get("full_name") or it.get("name") or it.get("sku","Unknown")
        sku        = it.get("sku") or it.get("name","")
        item_id    = it.get("item_id","")
        uom        = it.get("uom","unit")
        uom_id     = it.get("uom_id","")
        tax_code   = it.get("tax_code","SR9")
        tax_code_id= it.get("tax_code_id","")

        line_total = round(qty * price, 5)
        tax_amt    = round(line_total * tax_rate / 100, 5)
        item_total += line_total
        tax_total  += tax_amt

        item_info.append({
            "item_id": item_id, "name": name, "sku": sku,
            "tax_code": tax_code, "tax_code_id": tax_code_id,
            "tax_rate": tax_rate, "tax_amount": tax_amt,
            "uom": uom, "uom_id": uom_id,
            "base_uom_id": uom_id, "base_uom": uom,
            "conversion_rate": 1, "qty": qty, "new_qty": qty, "actual_qty": qty,
            "total_order_qty": 0, "price": price,
            "special_price_enabled": False, "promotion_enabled": False,
            "promotion_info": {"promotion_id":None,"name":None,"discount_type":None,"discount_value":None,"promotional_stock":None,"purchase_limit":None,"discount_amount":None},
            "discount_info": {"discount_type":"% Discount","discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"discount":0,"discount_amount":0,"coupon_code":0},
            "item_total": line_total, "adjusted_qty": 0, "available_qty": 0, "free": 0,
            "is_offer_item": False, "actual_is_offer_item": False,
            "movement_info": [], "new_movement_info": [],
            "exchange": False, "damaged": False, "deleted": False,
            "auto_assign": False, "picked": False, "packed": False,
            "packed_qty": 0, "picked_qty": 0, "has_catch_weight": False,
            "cw_conversion": 1, "cw_price": price, "cw_qty": qty,
            "actual_sku": sku, "actual_name": name,
            "actual_uom": uom, "actual_uom_id": uom_id,
            "actual_base_uom_id": uom_id, "actual_base_uom": uom,
            "actual_conversion_rate": 1, "actual_qty_for_return": qty,
            "actual_price": price, "drop_shipping": False,
            "drop_shipping_from_product": "No Drop Ship", "tags": [],
        })

    if not item_info:
        logger.error("push_order: no items to send")
        return None

    slot_info = {
        "slot_id": None, "delivery_type": "Delivery",
        "cut_off_name": "Morning - 9:00 am", "cut_off_period": "09:00",
        "start_period": None, "end_period": None, "default": True,
    }

    order_payload = {
        "company_id": company_id, "source": "B2B", "platform": "Whatsapp",
        "store_id": store_id, "store": store,
        "branch_id": branch_id, "branch_name": branch_name,
        "slot_info": slot_info,
        "currency": "SGD", "items_count": len(item_info),
        "order_status": "Pending", "paid_status": "Pending",
        "delivery_type": "Delivery", "delivery_date": delivery,
        "item_total": round(item_total, 5),
        "tax_amount": round(tax_total, 5),
        "total_amount": round(item_total + tax_total, 5),
        "created_by": username, "updated_by": username,
        "created_by_id": user_id, "user_id": user_id, "user_name": username,
        "item_info": item_info,
        "discount_info": {"discount_type":"% Discount","discount":0,"discount_amount":0,"discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
        "coupon_info":   {"discount_type":"% Discount","discount":0,"discount_amount":0,"discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
        "free_shipping": False, "payment_info": [], "applied_credit_notes": [],
        "advance_amount": 0, "delivery_charges": 0, "tax_inclusive": False,
        "installation_info": {"installation":False,"installation_date":None,"installation_charges":0},
        "whatsapp_contact_id": chat_id,
    }

    logger.info("push_order: store_id=%s branch_id=%s items=%d total=%.2f",
                store_id, branch_id, len(item_info), round(item_total+tax_total,2))

    try:
        result = await snc_call("/b2b/order/save", {"data": order_payload})
    except Exception as e:
        logger.error("push_order snc_call exception: %s", e)
        return None

    if not result:
        logger.error("push_order: snc_call returned None")
        return None

    r = result.get("result") or {}
    meta = r.get("metadata") or r
    order_num = (meta.get("order_number") or r.get("order_number") or
                 result.get("order_number") or
                 (r.get("data") or {}).get("order_number"))
    logger.info("push_order result: order_num=%s result_keys=%s meta_keys=%s",
                order_num, list(r.keys()), list(meta.keys()) if isinstance(meta,dict) else "n/a")
    return order_num


async def process_incoming_message(message: Dict, contact: Dict, company_id: str, chat_id: str):
    if not credentials["agent"]["enabled"]: return
    text = ""
    msg_type = message.get("type","")
    if msg_type == "text":
        txt_field = message.get("text","")
        if isinstance(txt_field, dict):
            text = txt_field.get("body","").strip()
        elif isinstance(txt_field, str):
            text = txt_field.strip()
        # Some Whapi formats send body at top level
        if not text:
            text = message.get("body","").strip()
    elif msg_type in ("image","document"):
        media = message.get(msg_type) or {}
        if isinstance(media, dict):
            text = media.get("caption","") or f"[{msg_type} received]"
        else:
            text = f"[{msg_type} received]"
    logger.info("[AGENT] Received type=%s text=%r from chat=%s", msg_type, text[:80] if text else "", message.get("chat_id",""))
    if not text: return

    sender = message.get("from_name") or contact.get("name") or "there"
    db = get_db()
    customer_row = db.execute("SELECT customer_name FROM customers WHERE phone LIKE ?",
                              (f"%{chat_id.replace('@s.whatsapp.net','').replace('@g.us','')}%",)).fetchone()
    db.close()
    customer = customer_row["customer_name"] if customer_row else None
    history  = conversation_memory.get(chat_id, [])

    # Handle CONFIRM
    if text.strip().upper() in ("CONFIRM","YES","YEP","OK","CONFIRMED"):
        mem     = conversation_memory.get(chat_id,[])
        pending = next((m.get("pending_order") for m in reversed(mem) if m.get("pending_order")), None)
        if pending:
            await whapi_send(chat_id, f"⏳ Placing your order now...")
            order_num = await push_order_to_snc(
                items=pending["items"], chat_id=chat_id,
                delivery_date=pending.get("delivery_date",""), sender_name=sender)
            if order_num:
                reply = f"✅ Order placed! *{order_num}*\n📅 Delivery: {pending.get('delivery_date','as requested')}"
                for m in mem: m.pop("pending_order",None)
            else:
                reply = f"⚠️ Could not create order automatically. Please contact your sales rep."
        else:
            reply = f"No pending order found. Please place a new order first."
        await whapi_send(chat_id, reply)
        db_log_message(chat_id, "agent", reply, "CONFIRM")
        return

    analysis = await analyze_with_ai(text, sender, customer, history)
    intent   = analysis.get("intent","UNKNOWN")
    logger.info("[AGENT] %s | %s | %.0f%%", intent, sender, analysis.get("confidence",0)*100)

    # For new orders, enrich with product lookup
    if intent == "NEW_ORDER" and analysis.get("items"):
        enriched = await lookup_products_for_items(analysis["items"])
        found    = [i for i in enriched if i.get("found")]
        missing  = [i for i in enriched if not i.get("found")]
        if found:
            lines = "\n".join([f"  • *{i['full_name']}* × {i['qty']} {i['uom']}" +
                               (f" @ ${i['price']:.2f}" if i.get("price") else "") for i in found])
            total = sum(i.get("price",0)*i.get("qty",1) for i in found)
            reply = f"Got it {sender}! 🛒\n\n{lines}"
            if missing: reply += f"\n\n⚠️ Not found: {', '.join(i['name'] for i in missing)}"
            if total > 0: reply += f"\n\n💰 Est. total: *${total:.2f}* (excl. tax)"
            reply += f"\n\n✅ Reply *CONFIRM* to place order\n📅 What delivery date?"
            # Store pending order
            conversation_memory.setdefault(chat_id,[]).append({"role":"agent","text":reply,
                "pending_order":{"items":enriched,"delivery_date":""}})
        else:
            reply = f"I couldn't find those products. Please check names and try again."
    else:
        reply = build_reply(analysis, sender)

    await whapi_send(chat_id, reply)
    conversation_memory.setdefault(chat_id,[]).append({"role":"customer","text":text})
    conversation_memory[chat_id].append({"role":"agent","text":reply})
    conversation_memory[chat_id] = conversation_memory[chat_id][-10:]
    db_log_message(chat_id, "customer", text, intent)
    db_log_message(chat_id, "agent", reply)

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_file = Path(__file__).parent / "oneaether.html"
    if html_file.exists(): return HTMLResponse(content=html_file.read_text())
    return HTMLResponse(content="<h2>✓ oneaether.ai running</h2>")

@app.get("/debug/customers-with-branches")
async def debug_customers_with_branches():
    """Show all customers that have a valid store_id and branch_id."""
    db = get_db()
    rows = db.execute("SELECT customer_id, customer_name, store_id, branch_id, branch_name, raw FROM customers").fetchall()
    db.close()
    with_branches = []
    without = []
    for r in rows:
        try:
            raw = json.loads(r["raw"] or "{}")
            b2b = raw.get("b2b_settings",{})
            stores = b2b.get("stores",[])
            has_store = bool(r["store_id"] or stores)
            entry = {
                "customer_id":   r["customer_id"],
                "customer_name": r["customer_name"],
                "store_id":      r["store_id"],
                "branch_id":     r["branch_id"],
                "stores_in_b2b": len(stores),
            }
            if has_store:
                with_branches.append(entry)
            else:
                without.append(r["customer_name"])
        except:
            pass
    return {
        "with_branches_count": len(with_branches),
        "without_branches_count": len(without),
        "customers_with_branches": with_branches[:20],
        "customers_without": without[:10],
    }


@app.get("/debug/customer-raw/{customer_id}")
async def debug_customer_raw(customer_id: str):
    """Show raw customer record from our DB."""
    db = get_db()
    row = db.execute("SELECT * FROM customers WHERE customer_id=?", (customer_id,)).fetchone()
    db.close()
    if not row:
        return {"error": "Not found in DB"}
    raw = json.loads(dict(row).get("raw","{}"))
    b2b = raw.get("b2b_settings",{})
    stores = b2b.get("stores",[])
    return {
        "customer_id":  row["customer_id"],
        "customer_name":row["customer_name"],
        "store_id":     row["store_id"],
        "branch_id":    row["branch_id"],
        "branch_name":  row["branch_name"],
        "stores_count": len(stores),
        "stores":       stores,
        "b2b_keys":     list(b2b.keys()),
    }


@app.get("/debug/find-branch/{customer_id}")
async def debug_find_branch(customer_id: str):
    """Try every possible SNC endpoint to find branch_id for a customer."""
    results = {}
    # Try customers/get with include_columns for stores
    r1 = await snc_call("/customers/get", {"data": {"filter_by": {
        "search_on": ["customer_id"], "search_text": customer_id,
        "exact_match": True,
        "pagination": {"page_no":1,"no_of_recs":1,"sort_by":"cts","order_by":False},
        "include_columns": ["stores","branches","outlets","b2b_settings","customer_id","customer_name"],
    }}})
    if r1:
        meta = r1.get("result",{}).get("metadata",{})
        cl = meta.get("CustomersList",[])
        if cl:
            c = cl[0]
            b2b = c.get("b2b_settings",{})
            results["customer_stores"] = c.get("stores",[])
            results["b2b_stores"] = b2b.get("stores",[])
            results["b2b_branches"] = b2b.get("branches",[])
            results["all_b2b_keys"] = list(b2b.keys())
            results["raw_customer"] = str(c)[:600]

    # Try dedicated branch endpoints
    for ep, body in [
        ("/b2b/branches/get",  {"data": {"customer_id": customer_id}}),
        ("/branch/list",       {"data": {"customer_id": customer_id}}),
        ("/customer/branches", {"data": {"customer_id": customer_id}}),
        ("/b2b/store/get",     {"data": {"customer_id": customer_id}}),
    ]:
        r = await snc_call(ep, body)
        results[ep] = str(r)[:200] if r else "None/404"

    return results


@app.get("/debug/customer-branches/{customer_id}")
async def debug_customer_branches(customer_id: str):
    """Try all known SNC endpoints to find branches for a customer."""
    results = {}

    # Try 1: b2b/customer/branches
    for ep in ["/b2b/customer/branches", "/customers/branches", "/branch/get", "/branches/get"]:
        r = await snc_call(ep, {"data": {"customer_id": customer_id}})
        results[ep] = str(r)[:200] if r else "None"

    # Try 2: Get store list
    r2 = await snc_call("/store/get", {"data": {"filter_by": {
        "customer_id": customer_id,
        "pagination": {"page_no":1,"no_of_recs":20,"sort_by":"cts","order_by":False}
    }}})
    results["/store/get"] = str(r2)[:300] if r2 else "None"

    # Try 3: outlets
    r3 = await snc_call("/outlets/get", {"data": {"customer_id": customer_id}})
    results["/outlets/get"] = str(r3)[:200] if r3 else "None"

    return results


@app.get("/debug/customer-lookup/{name}")
async def debug_customer_lookup(name: str):
    """Find real store_id and branch_id for a customer by name."""
    result = await snc_call("/customers/get", {"data": {"filter_by": {
        "search_on": ["customer_name"],
        "search_text": name,
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "cts", "order_by": False},
    }}})
    if not result:
        return {"error": "snc_call failed"}
    r      = result.get("result", {})
    meta   = r.get("metadata", {})
    customers = meta.get("CustomersList", [])
    out = []
    for c in customers:
        b2b    = c.get("b2b_settings") or {}
        stores = b2b.get("stores") or []
        out.append({
            "customer_id":   c.get("customer_id"),
            "customer_name": c.get("customer_name"),
            "stores":        stores,
            "raw_b2b_keys":  list(b2b.keys()),
        })
    return {"customers": out, "raw_snippet": str(customers[0])[:500] if customers else None}


@app.get("/debug/login-response")
async def debug_login_response():
    """Show raw SNC login response to find user_id field."""
    api_url  = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    enc_user = os.getenv("SNC_ENC_USERNAME", SNC_ENC_USERNAME)
    enc_pass = os.getenv("SNC_ENC_PASSWORD", SNC_ENC_PASSWORD)
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(f"{api_url}/login", data={
                "username": enc_user, "password": enc_pass,
                "grant_type": "password", "notification": "false",
            })
            data = resp.json()
            return {
                "http_status": resp.status_code,
                "keys":        list(data.keys()),
                "user_id":     data.get("user_id"),
                "id":          data.get("id"),
                "username_field": data.get("username"),
                "full_response": str(data)[:1000],
            }
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/products-test")
async def debug_products_test():
    """Test SNC products fetch — see raw response structure."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "search_text": [], "search_on": ["name","sku"],
        "confidence": 0.0, "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 3, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "Active",
        "include_columns": ["name","sku","prices","uom","uom_id","item_id"],
        "merged": True, "bundles": False,
    }}})
    if not result:
        return {"error": "snc_call returned None — check token/user_id"}
    r = result.get("result", {})
    meta = r.get("metadata", {})
    return {
        "result_keys":  list(r.keys()),
        "metadata_keys": list(meta.keys()),
        "data_type":    str(type(r.get("data"))),
        "sample":       str(result)[:800],
    }


@app.get("/debug/test-order")
async def debug_test_order():
    """Test order with real iSTEAKS IDs fetched from SNC."""
    # Step 1: Get real internal IDs for iSTEAKS
    result = await snc_call("/customers/get", {
        "custom": True,
        "data": {"filter_by": {"customer_id": "121212", "company_id": SNC_HARDCODED_COMPANY}}
    })
    store_id  = "121212"
    branch_id = ""
    branch_name = "Main Branch"
    if result:
        r    = result.get("result",{})
        meta = r.get("metadata",{})
        cl   = meta.get("CustomersList",[])
        if cl:
            c       = cl[0]
            store_id    = c.get("_id") or c.get("id") or c.get("customer_id","121212")
            b2b         = c.get("b2b_settings",{})
            stores      = b2b.get("stores",[])
            if stores:
                branch_id   = stores[0].get("branch_id","")
                branch_name = stores[0].get("branch_name","Main Branch")
            logger.info("iSTEAKS real IDs: store_id=%s branch_id=%s raw_keys=%s", store_id, branch_id, list(c.keys()))

    user_id    = SNC_HARDCODED_USER_ID
    username   = SNC_HARDCODED_USERNAME
    company_id = SNC_HARDCODED_COMPANY
    delivery   = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    api_url    = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token      = credentials["snc"].get("access_token","")

    payload = {
        "company_id": company_id, "user_id": user_id, "username": username,
        "timezone": "Asia/Singapore", "request_from": "WEB",
        "data": {
            "company_id": company_id, "source": "B2B", "platform": "Whatsapp",
            "store_id": store_id, "store": "iSTEAKS",
            "branch_id": branch_id, "branch_name": branch_name,
            "slot_info": {"slot_id":None,"delivery_type":"Delivery","cut_off_name":"Morning - 9:00 am","cut_off_period":"09:00","start_period":None,"end_period":None,"default":True},
            "currency": "SGD", "items_count": 1,
            "order_status": "Pending", "paid_status": "Pending",
            "delivery_type": "Delivery", "delivery_date": delivery,
            "item_total": 13.0, "tax_amount": 1.17, "total_amount": 14.17,
            "created_by": username, "updated_by": username,
            "created_by_id": user_id, "user_id": user_id, "user_name": username,
            "tax_code": {"tax_code":"SR9","tax_rate":9,"tax_code_id":"r4L4SqU4QSmMqU3EsOYJtg"},
            "tax_inclusive": False,
            "item_info": [{
                "item_id": "LaEjED0hQ0GEvlDSfMk2pA",
                "name": "THAILAND FROZEN CHICKEN KARAAGE 1KG",
                "sku": "ig-chicken-karaage",
                "tax_code": "SR9", "tax_code_id": "r4L4SqU4QSmMqU3EsOYJtg",
                "tax_rate": 9, "tax_amount": 1.17,
                "uom": "PKT", "uom_id": "Zye6i55vQCSVQbGdZL6aOQ",
                "base_uom_id": "Zye6i55vQCSVQbGdZL6aOQ", "base_uom": "PKT",
                "conversion_rate": 1, "qty": 1, "new_qty": 1, "actual_qty": 1,
                "total_order_qty": 0, "price": 13.0,
                "special_price_enabled": False, "promotion_enabled": False,
                "promotion_info": {"promotion_id":None,"name":None,"discount_type":None,"discount_value":None,"promotional_stock":None,"purchase_limit":None,"discount_amount":None},
                "discount_info": {"discount_type":"% Discount","discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"discount":0,"discount_amount":0,"coupon_code":0},
                "item_total": 13.0, "adjusted_qty": 0, "available_qty": 0, "free": 0,
                "is_offer_item": False, "actual_is_offer_item": False,
                "movement_info": [], "new_movement_info": [],
                "exchange": False, "damaged": False, "deleted": False,
                "auto_assign": False, "picked": False, "packed": False,
                "packed_qty": 0, "picked_qty": 0, "has_catch_weight": False,
                "cw_conversion": 1, "cw_price": 13.0, "cw_qty": 1,
                "actual_sku": "ig-chicken-karaage",
                "actual_name": "THAILAND FROZEN CHICKEN KARAAGE 1KG",
                "actual_uom": "PKT", "actual_uom_id": "Zye6i55vQCSVQbGdZL6aOQ",
                "actual_base_uom_id": "Zye6i55vQCSVQbGdZL6aOQ", "actual_base_uom": "PKT",
                "actual_conversion_rate": 1, "actual_qty_for_return": 1,
                "actual_price": 13.0, "drop_shipping": False,
                "drop_shipping_from_product": "No Drop Ship", "tags": [],
            }],
            "discount_info": {"discount_type":"% Discount","discount":0,"discount_amount":0,"discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
            "coupon_info":   {"discount_type":"% Discount","discount":0,"discount_amount":0,"discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
            "free_shipping": False, "payment_info": [], "applied_credit_notes": [],
            "advance_amount": 0, "delivery_charges": 0,
            "installation_info": {"installation":False,"installation_date":None,"installation_charges":0},
            "whatsapp_contact_id": "6587264539@s.whatsapp.net",
        }
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}/b2b/order/save", json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
            resp_data = resp.json()
            job_id = resp_data.get("job_id")
            poll_result = await poll_job(job_id) if job_id else None
            return {
                "store_id_used":  store_id,
                "branch_id_used": branch_id,
                "step1_http":     resp.status_code,
                "step1_response": resp_data,
                "step3_poll":     poll_result,
            }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/debug/order-test/{chat_id:path}")
async def debug_order_test(chat_id: str):
    """Test all prerequisites for order creation."""
    assignment = db_get_assignment(chat_id) or {}
    db = get_db()
    cust_count = db.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    prod_count = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    db.close()
    return {
        "chat_id":        chat_id,
        "assignment":     assignment,
        "snc_token":      bool(credentials["snc"].get("access_token")),
        "snc_user_id":    credentials["snc"].get("user_id","") or "1qxcb0ssTRGPQaKogvtkMw (fallback)",
        "snc_company_id": credentials["snc"].get("company_id",""),
        "snc_api_url":    credentials["snc"].get("api_url",""),
        "db_customers":   cust_count,
        "db_products":    prod_count,
        "ready":          bool(assignment.get("customer_id") or assignment.get("store_id")),
    }


@app.get("/resolve-user")
async def resolve_user():
    """Fetch user_id from SNC using the current token — call this after setting token."""
    api_url = credentials["snc"].get("api_url","")
    token   = credentials["snc"].get("access_token","")
    company = credentials["snc"].get("company_id","")
    if not token:
        raise HTTPException(status_code=400, detail="No token set")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Try to get user profile
            resp = await client.get(f"{api_url}/user/profile",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
            logger.info("resolve_user profile → HTTP %s", resp.status_code)
            if resp.status_code == 200:
                data = resp.json()
                user_id  = data.get("user_id") or data.get("id") or data.get("_id","")
                username = data.get("username") or data.get("email","")
                if user_id:
                    credentials["snc"]["user_id"]  = user_id
                    credentials["snc"]["username"] = username
                    db_set_credential("snc_user_id",  user_id)
                    db_set_credential("snc_username", username)
                    return {"user_id": user_id, "username": username, "source": "profile"}

            # Fallback: try whoami or me endpoint
            for ep in ["/whoami", "/me", "/user/me"]:
                resp2 = await client.get(f"{api_url}{ep}",
                    headers={"Authorization": f"Bearer {token}"})
                if resp2.status_code == 200:
                    data2 = resp2.json()
                    user_id = data2.get("user_id") or data2.get("id","")
                    if user_id:
                        credentials["snc"]["user_id"] = user_id
                        db_set_credential("snc_user_id", user_id)
                        return {"user_id": user_id, "source": ep}

            # Last resort: decode JWT
            try:
                import base64 as _b64, json as _json
                parts = token.split(".")
                pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
                decoded = _json.loads(_b64.b64decode(pad))
                logger.info("JWT payload: %s", decoded)
                return {"jwt_payload": decoded, "note": "user_id not in JWT — set manually"}
            except Exception as e:
                return {"error": str(e)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/refresh-token")
@app.post("/refresh-token")
async def refresh_token():
    """Manually trigger token refresh."""
    ok = await auto_refresh_token()
    return {
        "success": ok,
        "has_token": bool(credentials["snc"].get("access_token")),
        "user_id": credentials["snc"].get("user_id",""),
    }


@app.get("/health")
async def health():
    db = get_db()
    counts = {
        "customers": db.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
        "products":  db.execute("SELECT COUNT(*) FROM products").fetchone()[0],
        "orders":    db.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
    }
    db.close()
    return {"status":"ok","timestamp":datetime.utcnow().isoformat(),
            "snc_configured":bool(credentials["snc"].get("access_token")),
            "whapi_configured":bool(credentials["whapi"].get("token")),
            "agent_enabled":credentials["agent"]["enabled"],
            "db_counts":counts}

@app.post("/credentials")
async def save_credentials(payload: CredentialsPayload):
    if payload.snc:
        for k,v in payload.snc.items():
            if v: credentials["snc"][k] = v
        if payload.snc.get("api_url"):      db_set_credential("snc_api_url",    payload.snc["api_url"])
        if payload.snc.get("company_id"):   db_set_credential("snc_company_id", payload.snc["company_id"])
        if payload.snc.get("username"):     db_set_credential("snc_username",   payload.snc["username"])
        if payload.snc.get("user_id"):
            db_set_credential("snc_user_id", payload.snc["user_id"])
        # Auto-extract username from JWT if not provided
        token = payload.snc.get("access_token","")
        if token:
            db_set_credential("snc_token", token)
            try:
                import base64 as _b64, json as _json
                parts = token.split(".")
                if len(parts) == 3:
                    pad = parts[1] + "=" * (4 - len(parts[1]) % 4)
                    decoded = _json.loads(_b64.b64decode(pad))
                    if decoded.get("username") and not credentials["snc"].get("username"):
                        credentials["snc"]["username"] = decoded["username"]
                        db_set_credential("snc_username", decoded["username"])
                    logger.info("JWT decoded: username=%s company=%s", decoded.get("username"), decoded.get("company_id"))
            except Exception as e:
                logger.warning("JWT decode failed: %s", e)
    if payload.whapi:
        for k,v in payload.whapi.items():
            if v: credentials["whapi"][k] = v
        if payload.whapi.get("token"):      db_set_credential("whapi_token",      payload.whapi["token"])
        if payload.whapi.get("base_url"):   db_set_credential("whapi_base_url",   payload.whapi["base_url"])
        if payload.whapi.get("channel_id"): db_set_credential("whapi_channel_id", payload.whapi["channel_id"])
    if payload.agent:
        if payload.agent.get("anthropic_key"): db_set_credential("anthropic_key", payload.agent["anthropic_key"])
        if payload.agent.get("openai_key"):    db_set_credential("openai_key",    payload.agent["openai_key"])
    return {"status":"ok"}

@app.get("/credentials/status")
async def credentials_status():
    return {
        "snc":   {"api_url":credentials["snc"].get("api_url",""),"company_id":credentials["snc"].get("company_id",""),"authenticated":bool(credentials["snc"].get("access_token"))},
        "whapi": {"base_url":credentials["whapi"].get("base_url",""),"configured":bool(credentials["whapi"].get("token"))},
        "agent": {"enabled":credentials["agent"]["enabled"],"ai_powered":bool(credentials["agent"]["anthropic_key"] or credentials["agent"]["openai_key"])},
    }

# ─── Sync routes ──────────────────────────────────────────────────────────────
@app.get("/debug/sync-test")
async def debug_sync_test():
    """Test SNC connection and customer fetch."""
    api_url  = credentials["snc"].get("api_url","")
    token    = credentials["snc"].get("access_token","")
    company  = credentials["snc"].get("company_id","") or "mindmasters"
    user_id  = credentials["snc"].get("user_id","")  or "1qxcb0ssTRGPQaKogvtkMw"
    username = credentials["snc"].get("username","") or "admin@mindmastersg.com"

    if not token:
        return {"error": "No SNC token — paste Bearer token in Data Sources first"}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            payload = {
                "company_id":   company,
                "user_id":      user_id,
                "username":     username,
                "timezone":     "Asia/Singapore",
                "request_from": "WEB",
                "data": {"filter_by": {
                    "search_on": ["customer_name"],
                    "search_text": "",
                    "exact_match": False,
                    "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "cts", "order_by": False},
                }}
            }
            resp = await client.post(
                f"{api_url}/customers/get",
                json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
            data = resp.json()
            job_id = data.get("job_id")
            poll_result = None
            if job_id:
                poll_result = await poll_job(job_id)

            return {
                "step1_http":      resp.status_code,
                "step1_success":   data.get("status",{}).get("success"),
                "step1_job_id":    job_id,
                "step2_poll":      str(poll_result)[:500] if poll_result else None,
                "step2_has_data":  bool(poll_result and poll_result.get("result")),
                "credentials_used": {"company_id": company, "user_id": user_id, "username": username},
            }
    except Exception as e:
        return {"error": str(e)}


@app.post("/sync/customers")
async def sync_customers(background_tasks: BackgroundTasks):
    background_tasks.add_task(_sync_customers_task)
    return {"status":"syncing","message":"Customer sync started in background"}

async def _sync_customers_task():
    page=1; total=0
    while True:
        count = await sync_customers_from_snc(page=page, per_page=100)
        total += count
        logger.info("Sync progress: page=%d got=%d running_total=%d", page, count, total)
        if count < 100: break
        page += 1
    logger.info("Customer sync COMPLETE: %d customers stored", total)

@app.post("/sync/products")
async def sync_products(background_tasks: BackgroundTasks):
    background_tasks.add_task(_sync_products_task)
    return {"status":"syncing","message":"Product sync started in background"}

async def _sync_products_task():
    page=1; total=0
    while True:
        count = await sync_products_from_snc(page=page, per_page=100)
        total += count
        if count < 100: break
        page += 1
    logger.info("Product sync complete: %d total", total)

@app.post("/sync/orders")
async def sync_orders(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_orders_from_snc)
    return {"status":"syncing","message":"Order sync started in background"}

@app.post("/sync/all")
async def sync_all(background_tasks: BackgroundTasks):
    background_tasks.add_task(_sync_customers_task)
    background_tasks.add_task(_sync_products_task)
    background_tasks.add_task(sync_orders_from_snc)
    return {"status":"syncing","message":"Full sync started"}

# ─── Data routes ──────────────────────────────────────────────────────────────
@app.get("/data/customers")
async def get_customers(page: int = 1, per_page: int = 50, search: str = ""):
    db  = get_db()
    if search:
        rows  = db.execute("SELECT * FROM customers WHERE customer_name LIKE ? OR phone LIKE ? OR email LIKE ? LIMIT ? OFFSET ?",
                           (f"%{search}%",f"%{search}%",f"%{search}%", per_page, (page-1)*per_page)).fetchall()
        total = db.execute("SELECT COUNT(*) FROM customers WHERE customer_name LIKE ? OR phone LIKE ? OR email LIKE ?",
                           (f"%{search}%",f"%{search}%",f"%{search}%")).fetchone()[0]
    else:
        rows  = db.execute("SELECT * FROM customers LIMIT ? OFFSET ?", (per_page, (page-1)*per_page)).fetchall()
        total = db.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    db.close()
    return {"customers":[dict(r) for r in rows],"total":total,"page":page,"per_page":per_page}

@app.get("/data/products")
async def get_products(page: int = 1, per_page: int = 50, search: str = ""):
    db = get_db()
    if search:
        rows  = db.execute("SELECT * FROM products WHERE name LIKE ? OR sku LIKE ? LIMIT ? OFFSET ?",
                           (f"%{search}%",f"%{search}%", per_page, (page-1)*per_page)).fetchall()
        total = db.execute("SELECT COUNT(*) FROM products WHERE name LIKE ? OR sku LIKE ?",
                           (f"%{search}%",f"%{search}%")).fetchone()[0]
    else:
        rows  = db.execute("SELECT * FROM products LIMIT ? OFFSET ?", (per_page, (page-1)*per_page)).fetchall()
        total = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    db.close()
    return {"products":[dict(r) for r in rows],"total":total,"page":page,"per_page":per_page}

@app.get("/data/orders")
async def get_orders(page: int = 1, per_page: int = 50, search: str = "", status: str = ""):
    db = get_db()
    where = "WHERE 1=1"
    params: list = []
    if search: where += " AND (order_number LIKE ? OR customer_id LIKE ?)"; params += [f"%{search}%",f"%{search}%"]
    if status: where += " AND status=?"; params.append(status)
    rows  = db.execute(f"SELECT * FROM orders {where} ORDER BY synced_at DESC LIMIT ? OFFSET ?",
                       params+[per_page,(page-1)*per_page]).fetchall()
    total = db.execute(f"SELECT COUNT(*) FROM orders {where}", params).fetchone()[0]
    db.close()
    return {"orders":[dict(r) for r in rows],"total":total,"page":page,"per_page":per_page}

@app.get("/data/stats")
async def get_stats():
    db = get_db()
    stats = {
        "customers": db.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
        "products":  db.execute("SELECT COUNT(*) FROM products").fetchone()[0],
        "orders":    db.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
        "pending":   db.execute("SELECT COUNT(*) FROM orders WHERE status='Pending'").fetchone()[0],
        "conversations": db.execute("SELECT COUNT(DISTINCT chat_id) FROM conversation_log").fetchone()[0],
        "messages":  db.execute("SELECT COUNT(*) FROM conversation_log").fetchone()[0],
    }
    db.close()
    return stats

# ─── Assignments ──────────────────────────────────────────────────────────────
@app.get("/data/customer/{customer_id}/orders")
async def get_customer_orders(customer_id: str, page: int = 1, per_page: int = 20):
    """Get order history for a customer."""
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM orders WHERE customer_id=? OR store_id=?",
                       (customer_id, customer_id)).fetchone()[0]
    rows = db.execute("""SELECT order_id, order_number, store, branch_name, status, paid_status,
        delivery_date, item_total, tax_amount, total_amount, items_count, item_info,
        platform, created_by, synced_at
        FROM orders WHERE customer_id=? OR store_id=?
        ORDER BY delivery_date DESC LIMIT ? OFFSET ?""",
        (customer_id, customer_id, per_page, (page-1)*per_page)).fetchall()
    db.close()
    orders = []
    for r in rows:
        d = dict(r)
        try: d["item_info"] = json.loads(d.get("item_info") or "[]")
        except: d["item_info"] = []
        orders.append(d)

    # Also try to sync latest orders from SNC
    if total == 0:
        asyncio.create_task(_sync_orders_for_customer(customer_id))

    return {"orders": orders, "total": total, "page": page, "per_page": per_page}


async def _sync_orders_for_customer(customer_id: str):
    """Background sync of orders for a specific customer."""
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source": "B2B", "platform": "Whatsapp",
        "store_id": [customer_id], "branch_id": [], "status": "All",
        "date_range": [
            (datetime.now() - timedelta(days=365)).strftime("%d-%m-%Y"),
            datetime.now().strftime("%d-%m-%Y"),
        ],
        "search_on": ["order_number","store"], "search_text": "",
        "exact_match": False,
        "pagination": {"page_no":1,"no_of_recs":50,"sort_by":"order_cts","order_by":False},
        "packer_id":"","sales_person_id":"","date_range_by":"order_cts",
        "warehouse_id":"All","tags":[],"whatsapp_contact_id":[],"review_required":False,
    }}})
    if result:
        r    = result.get("result",{})
        meta = r.get("metadata",{})
        orders = meta.get("OrderList") or meta.get("orders") or []
        if orders:
            db_upsert_orders(orders)
            logger.info("Synced %d orders for customer %s", len(orders), customer_id)


@app.get("/data/customer/{customer_id}/branches")
async def get_customer_branches(customer_id: str):
    """Get branches for a customer from DB or SNC."""
    # Check DB first
    db = get_db()
    row = db.execute("SELECT raw FROM customers WHERE customer_id=?", (customer_id,)).fetchone()
    db.close()
    if row:
        try:
            raw = json.loads(row["raw"])
            b2b = raw.get("b2b_settings") or {}
            stores = b2b.get("stores") or raw.get("stores") or []
            if stores:
                branches = []
                for s in stores:
                    for br in s.get("branches") or [s] if s.get("branch_id") else []:
                        branches.append({
                            "branch_id":   br.get("branch_id", s.get("branch_id","")),
                            "branch_name": br.get("branch_name", s.get("branch_name","Main Branch")),
                            "store_id":    s.get("store_id",""),
                            "store_name":  s.get("store_name", s.get("store","")),
                        })
                if branches:
                    return {"branches": branches}
        except Exception as e:
            logger.warning("Branch parse error: %s", e)

    # Fallback: fetch from SNC
    result = await snc_call("/customers/get", {"data": {"filter_by": {
        "search_on": ["customer_id"],
        "search_text": customer_id,
        "exact_match": True,
        "pagination": {"page_no": 1, "no_of_recs": 1, "sort_by": "cts", "order_by": False},
    }}})
    if result:
        r = result.get("result", {})
        meta = r.get("metadata", {})
        customers = meta.get("CustomersList", [])
        if customers:
            c = customers[0]
            b2b = c.get("b2b_settings") or {}
            stores = b2b.get("stores") or []
            branches = []
            for s in stores:
                for br in s.get("branches") or [{"branch_id": s.get("branch_id",""), "branch_name": s.get("branch_name","Main Branch")}]:
                    branches.append({
                        "branch_id":   br.get("branch_id",""),
                        "branch_name": br.get("branch_name","Main Branch"),
                        "store_id":    s.get("store_id",""),
                        "store_name":  s.get("store_name",""),
                    })
            return {"branches": branches or [{"branch_id":"main","branch_name":"Main Branch","store_id":"","store_name":""}]}

    return {"branches": [{"branch_id": "main", "branch_name": "Main Branch", "store_id": "", "store_name": ""}]}


@app.post("/assignments")
async def save_assignments(request: Request):
    body = await request.json()
    for chat_id, data in body.get("assignments",{}).items():
        db_save_assignment(chat_id, data)
    return {"status":"ok","saved":len(body.get("assignments",{}))}

@app.get("/assignments")
async def get_assignments():
    return {"assignments": db_get_all_assignments()}

# ─── Webhook ──────────────────────────────────────────────────────────────────
@app.post("/process-message")
async def process_message(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    messages   = body.get("messages",[])
    contact    = body.get("contacts",{})
    company_id = body.get("company_id", credentials["snc"].get("company_id",""))

    # Auto-assign customer from contacts payload (matches Postman webhook format)
    # contacts.customers[0] contains customer_id and customer_branch_id
    if contact and isinstance(contact, dict):
        wa_customers = contact.get("customers", [])
        if wa_customers:
            for message in messages:
                chat_id = message.get("chat_id","")
                if chat_id and not db_get_assignment(chat_id):
                    wc = wa_customers[0]
                    auto_cid    = wc.get("customer_id","")
                    auto_branch = wc.get("customer_branch_id","")
                    auto_bname  = wc.get("customer_branch_name","Main Branch")
                    auto_cname  = wc.get("customer_name","")
                    if auto_cid:
                        db_save_assignment(chat_id, {
                            "customer_id": auto_cid,
                            "customer":    auto_cname,
                            "store_id":    auto_cid,
                            "branch_id":   auto_branch,
                            "branch":      auto_bname,
                        })
                        logger.info("Auto-assigned %s → %s / %s", chat_id, auto_cname, auto_branch)

    for message in messages:
        if message.get("from_me"): continue
        if message.get("type") not in ("text","image","document","audio","voice"): continue
        background_tasks.add_task(process_incoming_message,
            message=message, contact=contact,
            company_id=company_id, chat_id=message.get("chat_id",""))
    return JSONResponse({"status":"ok","received":len(messages)})

# ─── Proxy routes ─────────────────────────────────────────────────────────────
@app.get("/proxy/whapi/chats")
async def proxy_whapi_chats(page: int=1, count: int=20):
    base = credentials["whapi"].get("base_url","https://gate.whapi.cloud")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{base}/chats", headers=whapi_headers(), params={"page":page,"count":count})
            return resp.json()
        except Exception as e: raise HTTPException(status_code=502, detail=str(e))

@app.get("/proxy/whapi/messages/{chat_id}")
async def proxy_whapi_messages(chat_id: str, page: int=1, count: int=40):
    base = credentials["whapi"].get("base_url","https://gate.whapi.cloud")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{base}/messages/list/{chat_id}", headers=whapi_headers(), params={"page":page,"count":count})
            return resp.json()
        except Exception as e: raise HTTPException(status_code=502, detail=str(e))

@app.get("/proxy/whapi/group/{chat_id:path}")
async def proxy_whapi_group(chat_id: str):
    """Fetch group participants from Whapi."""
    base  = credentials["whapi"].get("base_url", "https://gate.whapi.cloud")
    token = credentials["whapi"].get("token", "")
    if not token:
        return {"participants": [], "error": "No Whapi token"}
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{base}/groups/{chat_id}", headers=whapi_headers())
            logger.info("Whapi group %s → HTTP %s", chat_id, resp.status_code)
            if resp.status_code == 200:
                data = resp.json()
                participants = data.get("participants", data.get("members", []))
                return {"participants": participants, "raw": data}
            resp2 = await client.get(f"{base}/chats/{chat_id}", headers=whapi_headers())
            if resp2.status_code == 200:
                data2 = resp2.json()
                return {"participants": data2.get("participants", data2.get("members", [])), "raw": data2}
            return {"participants": [], "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            logger.error("Whapi group error: %s", e)
            return {"participants": [], "error": str(e)}

@app.post("/proxy/whapi/send")
async def proxy_whapi_send(payload: SendMessagePayload):
    ok = await whapi_send(payload.contact_id, payload.body)
    if not ok: raise HTTPException(status_code=502, detail="Failed to send")
    return {"status":"ok","sent":True}

@app.post("/proxy/snc/login")
async def proxy_snc_login(request: Request):
    body     = await request.json()
    api_url  = body.get("api_url","").rstrip("/")
    username = body.get("username","")
    password = body.get("password","")
    if not api_url or not username or not password:
        raise HTTPException(status_code=400, detail="api_url, username and password required")
    if not api_url.endswith("/api"):
        api_url = api_url + "/api" if "/api" not in api_url else api_url.split("/api")[0]+"/api"
    logger.info("Proxying SNC login → %s/login", api_url)
    fd = {"username": username, "password": password, "grant_type": "password", "notification": "false"}
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(f"{api_url}/login", data=fd)
            logger.info("SNC login: %s | %s", resp.status_code, resp.text[:300])
            data = resp.json()
            if resp.status_code not in (200,201):
                raise HTTPException(status_code=resp.status_code,
                    detail=data.get("detail") or data.get("message") or f"SNC returned {resp.status_code}")
            if data.get("access_token"):
                credentials["snc"]["api_url"]      = api_url
                credentials["snc"]["access_token"]  = data["access_token"]
                credentials["snc"]["user_id"]       = data.get("user_id","")
                credentials["snc"]["username"]      = username
            return data
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

@app.post("/proxy/snc/customers")
async def proxy_snc_customers(request: Request):
    body = await request.json()
    result = await snc_call("/customers/get", {"data": {"filter_by": {
        "search_on": ["customer_id","customer_name","customer_phone","customer_email"],
        "exact_match": False,
        "pagination": {"page_no":1,"no_of_recs":100,"sort_by":"cts","order_by":False},
        **body.get("filter_by",{})
    }}})
    if result is None: raise HTTPException(status_code=502, detail="SNC API failed")
    return result

@app.post("/proxy/snc/products")
async def proxy_snc_products(request: Request):
    body = await request.json()
    search_text = body.get("search_text",[])
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "search_text": search_text if isinstance(search_text,list) else [search_text],
        "search_on": ["name","sku","product_code"],
        "confidence": 0.5, "exact_match": False,
        "pagination": {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False},
        "view":"individual","status":"All",
        "include_columns":["name","sku","prices","uom","quantity","images"],
        "merged":True,"bundles":False
    }}})
    if result is None: raise HTTPException(status_code=502, detail="SNC API failed")
    return result

@app.post("/proxy/snc/orders")
async def proxy_snc_orders(request: Request):
    body = await request.json()
    today     = datetime.now().strftime("%d-%m-%Y")
    from_date = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
    date_range = body.get("date_range",[from_date, today])
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source":"B2B","platform":"Whatsapp","store_id":[],"branch_id":[],
        "status":body.get("status","All"),"date_range":date_range,
        "search_on":["order_number","reference","store","tags"],
        "search_text":"","exact_match":False,
        "pagination":{"page_no":1,"no_of_recs":40,"sort_by":"order_cts","order_by":False},
        "whatsapp_contact_id":body.get("whatsapp_contact_id",[]),
        "review_required":False
    }}})
    if result is None: raise HTTPException(status_code=502, detail="SNC API failed")
    return result

@app.post("/orders/create")
async def create_order_manual(request: Request):
    """Create order manually from the UI Create Order button."""
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    chat_id       = body.get("chat_id", "")
    items_raw     = body.get("items", [])
    delivery_date = body.get("delivery_date", "")
    sender_name   = body.get("sender_name", "")

    logger.info("create_order: chat=%s items=%s delivery=%s", chat_id, items_raw, delivery_date)

    if not items_raw:
        raise HTTPException(status_code=400, detail="No items provided")

    items = [{"name": i["name"], "qty": i.get("qty",1), "uom": i.get("uom","unit")}
             for i in items_raw if i.get("name","").strip()]

    # Look up products in SNC
    enriched = await lookup_products_for_items(items)
    found    = [i for i in enriched if i.get("found")]
    missing  = [i for i in enriched if not i.get("found")]

    logger.info("create_order: found=%d missing=%d", len(found), len(missing))

    # If no products found in DB/SNC, create order with basic item info anyway
    if not found:
        logger.warning("create_order: no products matched in SNC — using raw items")
        # Build minimal items without SNC product lookup
        found = [{
            "item_id": "", "sku": i["name"], "full_name": i["name"],
            "uom": i.get("uom","unit"), "uom_id": "", "base_uom": i.get("uom","unit"),
            "base_uom_id": "", "price": 0, "tax_rate": 9,
            "tax_code": "SR9", "tax_code_id": "", "qty": i["qty"], "found": True,
        } for i in items]

    try:
        order_num = await push_order_to_snc(
            items=found, chat_id=chat_id,
            delivery_date=delivery_date, sender_name=sender_name,
        )
    except Exception as e:
        logger.error("push_order_to_snc exception: %s", e)
        raise HTTPException(status_code=502, detail=f"Order creation error: {str(e)}")

    if not order_num:
        assignment = db_get_assignment(chat_id) or {}
        raise HTTPException(status_code=502, detail=(
            f"Order creation failed. "
            f"store_id={'✓' if assignment.get('store_id') else '✗ MISSING'} "
            f"branch_id={'✓' if assignment.get('branch_id') else '✗ MISSING'} "
            f"token={'✓' if credentials['snc'].get('access_token') else '✗ MISSING'}"
        ))

    item_lines = ", ".join([f"{i['full_name']} x{i['qty']}" for i in found])
    msg = f"✅ Order *{order_num}* placed!\n📦 {item_lines}\n📅 Delivery: {delivery_date or 'as scheduled'}"
    await whapi_send(chat_id, msg)

    return {"order_number": order_num, "items": len(found), "missing": [i["name"] for i in missing], "status": "created"}


@app.get("/agent/status")
async def agent_status():
    return {"enabled":credentials["agent"]["enabled"],
            "ai_powered":bool(credentials["agent"]["anthropic_key"] or credentials["agent"]["openai_key"]),
            "model":"claude" if credentials["agent"]["anthropic_key"] else "openai" if credentials["agent"]["openai_key"] else "rule-based",
            "active_chats":len(conversation_memory)}

@app.post("/agent/toggle")
async def toggle_agent(request: Request):
    body = await request.json()
    credentials["agent"]["enabled"] = body.get("enabled",True)
    return {"enabled":credentials["agent"]["enabled"]}

@app.post("/agent/test")
async def test_agent(request: Request):
    body   = await request.json()
    text   = body.get("message","I want 5 boxes chicken karaage")
    sender = body.get("sender","Test User")
    analysis = await analyze_with_ai(text, sender, None, [])
    reply    = build_reply(analysis, sender)
    return {"input":text,"intent":analysis.get("intent"),"confidence":analysis.get("confidence"),
            "items":analysis.get("items",[]),"method":analysis.get("method"),"reply":reply}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT",8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
