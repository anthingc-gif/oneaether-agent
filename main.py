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
    # Drop tables if columns are missing (schema migration)
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(customers)").fetchall()]
        if "customer_code" not in cols or "price_tier" not in cols:
            db.execute("DROP TABLE IF EXISTS customers"); db.commit()
    except: pass
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(products)").fetchall()]
        if "base_uom" not in cols or "short_name" not in cols:
            db.execute("DROP TABLE IF EXISTS products"); db.commit()
    except: pass
    try:
        cols = [r[1] for r in db.execute("PRAGMA table_info(orders)").fetchall()]
        if "paid_status" not in cols or "item_info" not in cols:
            db.execute("DROP TABLE IF EXISTS orders"); db.commit()
    except: pass

    db.executescript("""
        CREATE TABLE IF NOT EXISTS credentials (
            key TEXT PRIMARY KEY, value TEXT NOT NULL,
            uts TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS customers (
            customer_id        TEXT PRIMARY KEY,
            customer_code      TEXT,
            customer_name      TEXT,
            customer_type      TEXT,
            phone              TEXT,
            email              TEXT,
            uen                TEXT,
            store_id           TEXT,
            store              TEXT,
            branch_id          TEXT,
            branch_name        TEXT,
            address            TEXT,
            credit_term        TEXT,
            credit_term_id     TEXT,
            price_tier         TEXT,
            outstanding_amount REAL DEFAULT 0,
            max_credit_limit   REAL DEFAULT 0,
            min_order_amount   REAL DEFAULT 0,
            tax_code           TEXT,
            currency           TEXT DEFAULT 'SGD',
            status             TEXT DEFAULT 'Active',
            billing_id         TEXT,
            payment_mode       TEXT,
            sales_person       TEXT,
            b2b_stores         TEXT,
            raw                TEXT,
            synced_at          TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS products (
            item_id      TEXT PRIMARY KEY,
            sku          TEXT,
            name         TEXT,
            short_name   TEXT,
            uom          TEXT,
            uom_id       TEXT,
            base_uom     TEXT,
            base_uom_id  TEXT,
            price        REAL DEFAULT 0,
            tax_code     TEXT DEFAULT 'SR9',
            tax_code_id  TEXT,
            tax_rate     REAL DEFAULT 9,
            product_code TEXT,
            category     TEXT,
            quantity     REAL DEFAULT 0,
            status       TEXT DEFAULT 'Active',
            raw          TEXT,
            synced_at    TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id      TEXT PRIMARY KEY,
            order_number  TEXT,
            customer_id   TEXT,
            store_id      TEXT,
            store         TEXT,
            branch_id     TEXT,
            branch_name   TEXT,
            status        TEXT,
            paid_status   TEXT,
            delivery_date TEXT,
            delivery_type TEXT,
            item_total    REAL DEFAULT 0,
            tax_amount    REAL DEFAULT 0,
            total_amount  REAL DEFAULT 0,
            items_count   INTEGER DEFAULT 0,
            item_info     TEXT,
            chat_id       TEXT,
            platform      TEXT,
            source        TEXT,
            created_by    TEXT,
            raw           TEXT,
            synced_at     TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS chat_assignments (
            chat_id     TEXT PRIMARY KEY,
            customer_id TEXT, customer TEXT,
            store_id    TEXT, branch_id TEXT, branch TEXT,
            updated_at  TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS conversation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT, role TEXT, message TEXT, intent TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    db.commit(); db.close()
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

        # branch_list has real branch IDs (different from b2b stores)
        branch_list = c.get("branch_list") or []
        store_id    = (stores[0].get("store_id","")    if stores else "") or c.get("store_id","")
        branch_id   = (branch_list[0].get("branch_id","") if branch_list else "") or                       (stores[0].get("branch_id","")   if stores else "") or c.get("branch_id","")
        branch_name = (branch_list[0].get("branch_name","") if branch_list else "") or                       (stores[0].get("branch_name","") if stores else "") or c.get("branch_name","")

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

        # Contact info — from contacts_list
        contacts_list = c.get("contacts_list") or []
        first_contact = contacts_list[0] if contacts_list else {}
        phone = c.get("customer_phone","") or c.get("phone","") or first_contact.get("phone","")
        email = c.get("customer_email","") or c.get("email","") or first_contact.get("email","")
        status = "Active" if c.get("is_active", True) else "Inactive"

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
            cid,
            name,
            c.get("customer_type",""),
            phone, email,
            c.get("customer_uen",""),
            store_id, name, branch_id, branch_name,
            json.dumps(c.get("address",{})),
            credit_term, credit_term_id, price_tier,
            outstanding, max_credit, min_order,
            tax_code, currency,
            status,
            c.get("billing_id",""),
            payment_mode, sales_person,
            json.dumps({"stores": stores, "branch_list": branch_list}),
            json.dumps(c),
        ))
    db.commit(); db.close()
    logger.info("Upserted %d customers", len(customers))

def db_upsert_products(products: List[Dict]):
    db = get_db()
    # Add missing columns if needed
    existing = [r[1] for r in db.execute("PRAGMA table_info(products)").fetchall()]
    for col, defn in [
        ("short_name","TEXT"), ("base_uom","TEXT"), ("base_uom_id","TEXT"),
        ("product_code","TEXT"), ("category","TEXT"), ("quantity","REAL"),
    ]:
        if col not in existing:
            db.execute(f"ALTER TABLE products ADD COLUMN {col} {defn}")
    db.commit()

    for p in products:
        item_id = p.get("item_id") or p.get("sku","")
        if not item_id: continue

        # Pick best price: prefer "Default Price" tier, then "B2B", then first non-zero
        prices = p.get("prices") or []
        price  = 0.0
        for tier_name in ["Default Price","B2B","POS"]:
            t = next((x for x in prices if x.get("name") == tier_name), None)
            if t and float(t.get("price",0) or 0) > 0:
                price = float(t["price"])
                break
        if price == 0.0:
            for t in prices:
                v = float(t.get("price",0) or 0)
                if v > 0:
                    price = v
                    break

        tc_raw      = p.get("tax_code") or {}
        tax_code    = tc_raw.get("tax_code","SR9")    if isinstance(tc_raw,dict) else str(tc_raw) or "SR9"
        tax_code_id = tc_raw.get("tax_code_id","")    if isinstance(tc_raw,dict) else p.get("tax_code_id","")
        tax_rate    = float(tc_raw.get("tax_rate",9)  if isinstance(tc_raw,dict) else p.get("tax_rate",9) or 9)

        uom_id = p.get("uom_id","")
        if isinstance(uom_id, dict): uom_id = uom_id.get("uom_id","")

        base_uom    = p.get("base_uom","") or p.get("uom","")
        base_uom_id = p.get("base_uom_id","") or uom_id

        db.execute("""INSERT OR REPLACE INTO products
            (item_id,sku,name,short_name,uom,uom_id,base_uom,base_uom_id,
             price,tax_rate,tax_code,tax_code_id,
             product_code,category,quantity,status,raw,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
            (
                item_id,
                p.get("sku","") or item_id,
                p.get("name",""),
                p.get("short_name","") or p.get("name",""),
                p.get("uom",""), uom_id, base_uom, base_uom_id,
                price, tax_rate, tax_code, tax_code_id,
                p.get("product_code",""), p.get("category",""),
                float(p.get("quantity",0) or 0),
                p.get("status","Active"), json.dumps(p),
            ))
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
    await auto_refresh_token()
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
    # Postman: relation_id is a customer's customer_id, used to filter by customer
    # Without relation_id, pass empty string to get all products
    # relation_id in Postman is "11001058" — an ERP store/relation ID stored in raw customer data
    # Try to find it from raw customer data (erp_id or store reference number)
    relation_id = ""
    try:
        _db = get_db()
        rows = _db.execute("SELECT customer_id, raw FROM customers WHERE raw IS NOT NULL LIMIT 20").fetchall()
        _db.close()
        for row in rows:
            try:
                raw = json.loads(row["raw"])
                # Check erp_id, erp_tenant_name, and nested store IDs
                erp = raw.get("erp_id") or raw.get("erp_tenant_name")
                if erp and str(erp).isdigit():
                    relation_id = str(erp)
                    break
                # Check branch_list for netsuite_internal_id
                bl = raw.get("branch_list") or []
                for b in bl:
                    nid = b.get("netsuite_internal_id")
                    if nid and str(nid).isdigit():
                        relation_id = str(nid)
                        break
                if relation_id:
                    break
                # Fall back to numeric customer_id
                cid = row["customer_id"]
                if cid and cid.replace("-","").isdigit():
                    relation_id = cid
                    break
            except: pass
    except: pass
    # Final fallback: use iSTEAKS known ID
    if not relation_id:
        relation_id = "121212"
    logger.info("sync_products: using relation_id=%s", relation_id)

    result = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [],
        "relation_id": relation_id,
        "search_on": ["name","sku","product_code"],
        "confidence": 0.5,
        "search_text": [],
        "exact_match": False,
        "pagination": {"page_no": page, "no_of_recs": per_page, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": [
            "short_description","description","sku","short_name","name",
            "parent_sku","product_code","category","sub_category","brand",
            "uom","base_uom","prices","images","status","is_sellable","is_edited",
            "b2b_enabled","pos_enabled","is_primary","merged_id","merged_skus",
            "bom_type","linked_stores","inventory_type","quantity","uom_id","config","attribute_set"
        ],
        "merged": True, "bundles": False,
    }}})
    if not result:
        logger.error("sync_products: snc_call returned None")
        return 0
    r    = result.get("result", {})
    meta = r.get("metadata", {})
    # SNC returns products under various keys — try all
    # SNC returns products under metadata.products
    products = meta.get("products") or meta.get("ProductList") or meta.get("Items") or []
    if not isinstance(products, list):
        products = []
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
    """Extract order items from text. Handles qty+uom+name and name+qty+uom formats."""
    items = []
    seen: set = set()
    text = text.replace("\r", "").replace("\n", ", ")
    UOM = r"(?:kg|kgs|grams?|g|pcs?|pkt|pkts?|packets?|boxes?|bags?|units?|cans?|bottles?|cartons?|nos?)"
    # Pattern A: qty [uom] name  →  "5kg chicken", "3 boxes beef"
    pA = re.compile(
        r"(\d+(?:\.\d+)?)\s*(" + UOM + r")?\s+(?:of\s+)?([a-zA-Z][a-zA-Z0-9\s\-]{1,40}?)"
        r"(?=\s*(?:,|\band\b|&|$))", re.IGNORECASE)
    # Pattern B: name qty uom  →  "chicken 5kg", "beef cubes 3pcs"
    pB = re.compile(
        r"([a-zA-Z][a-zA-Z0-9\s\-]{1,40}?)\s+(\d+(?:\.\d+)?)\s*(" + UOM + r")"
        r"(?=\s*(?:,|\band\b|&|$))", re.IGNORECASE)
    for m in pA.finditer(text):
        qty, uom, name = m.groups()
        name = name.strip().rstrip(".,- ")
        key = name.lower()
        if len(name) > 1 and key not in seen:
            seen.add(key)
            items.append({"name": name, "qty": float(qty), "uom": (uom or "unit").lower()})
    if not items:
        for m in pB.finditer(text):
            name, qty, uom = m.groups()
            name = name.strip().rstrip(".,- ")
            key = name.lower()
            if len(name) > 1 and key not in seen:
                seen.add(key)
                items.append({"name": name, "qty": float(qty), "uom": (uom or "unit").lower()})
    return items

def extract_order_number(text: str) -> Optional[str]:
    m = re.search(r'[A-Z]{2,5}[-/]\d{6,12}[-/]\d{1,5}', text.upper())
    return m.group(0) if m else None

# ─── AI Analysis ──────────────────────────────────────────────────────────────
async def analyze_with_ai(text: str, sender: str, customer: Optional[str], history: List[Dict]) -> Dict:
    anthropic_key = credentials["agent"].get("anthropic_key","") or os.getenv("ANTHROPIC_API_KEY","")
    openai_key    = credentials["agent"].get("openai_key","") or os.getenv("OPENAI_API_KEY","") or os.getenv("OPENAI_API_KEY","")
    intent, confidence = detect_intent(text)
    base = {"intent": intent, "confidence": confidence, "items": extract_items(text),
            "order_number": extract_order_number(text), "reply": None, "method": "rule-based"}
    if not anthropic_key and not openai_key:
        logger.warning("No AI key set — using rule-based detection. Set Anthropic key in Settings.")
        return base

    ctx = "\n".join([f"{m['role'].upper()}: {m['text']}" for m in history[-4:]]) if history else "None"
    system = """You are a WhatsApp ordering assistant for a B2B food distribution company.
Return ONLY valid JSON — no markdown, no extra text.
Intent options: NEW_ORDER, AMEND_ORDER, CANCEL_ORDER, ORDER_STATUS, ENQUIRY, GREETING, UNKNOWN

IMPORTANT: Extract ALL items from the message. Messages may have multiple items separated by commas, "and", or newlines.
Example: "5kg chicken and 3kg beef" → 2 items. "chicken 5kg, pork 2kg, fish 3pcs" → 3 items.

Response format:
{
  "intent": "NEW_ORDER",
  "confidence": 0.95,
  "items": [
    {"name": "chicken curry cut", "qty": 5, "uom": "kg"},
    {"name": "beef cubes", "qty": 3, "uom": "kg"}
  ],
  "order_number": null,
  "reply": "Got it! I have: chicken curry cut 5kg, beef cubes 3kg. Reply CONFIRM to place order."
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
            # item_id may be null — use sku as fallback
            iid = row["item_id"] or row["sku"] or ""
            uid = row["uom_id"] or ""
            enriched.append({**item,
                "item_id": iid, "sku": row["sku"] or "",
                "full_name": row["name"], "uom": row["uom"] or "unit",
                "uom_id": uid, "base_uom": row["base_uom"] or row["uom"] or "unit",
                "base_uom_id": row["base_uom_id"] or uid,
                "price": float(row["price"] or 0),
                "tax_rate": float(row["tax_rate"] or 9),
                "tax_code": row["tax_code"] or "SR9",
                "tax_code_id": row["tax_code_id"] or "r4L4SqU4QSmMqU3EsOYJtg",
                "found": True})
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

    # Resolve real branch_id from customers DB
    if branch_id in ("main", "Main Branch", "", None):
        branch_id = ""
    if not branch_id and store_id:
        db = get_db()
        row = db.execute(
            "SELECT raw, branch_id, branch_name FROM customers WHERE customer_id=? OR store_id=?",
            (store_id, store_id)).fetchone()
        db.close()
        if row:
            # Try branch_id column first
            if row["branch_id"]:
                branch_id   = row["branch_id"]
                branch_name = row["branch_name"] or branch_name
                logger.info("Resolved branch from DB column: branch_id=%s", branch_id)
            else:
                # Parse raw to get branch_list
                try:
                    raw_data = json.loads(row["raw"] or "{}")
                    bl = raw_data.get("branch_list") or []
                    if bl:
                        branch_id   = bl[0].get("branch_id","")
                        branch_name = bl[0].get("branch_name", branch_name)
                        logger.info("Resolved branch from branch_list: branch_id=%s", branch_id)
                except Exception as e:
                    logger.warning("Branch resolve error: %s", e)

    # Get credit_term from customer DB (required by Postman payload)
    credit_term    = {}
    delivery_addr  = {}
    billing_addr   = {}
    if store_id:
        db = get_db()
        crow = db.execute("SELECT raw FROM customers WHERE customer_id=?", (store_id,)).fetchone()
        db.close()
        if crow:
            try:
                craw = json.loads(crow["raw"] or "{}")
                b2b  = craw.get("b2b_settings") or {}
                ct   = b2b.get("credit_term") or {}
                if ct: credit_term = ct
                # Get address from branch_list
                bl = craw.get("branch_list") or []
                if bl:
                    ba = bl[0].get("billing_address") or {}
                    sa = bl[0].get("shipping_address") or {}
                    if ba:
                        billing_addr = {
                            "address1": ba.get("address1",""), "address2": ba.get("address2"),
                            "city": ba.get("city"), "state": ba.get("state"),
                            "country": ba.get("country","Singapore"),
                            "postal_code": ba.get("postal_code",""),
                        }
                    if sa:
                        delivery_addr = {
                            "address1": sa.get("address1",""), "address2": sa.get("address2"),
                            "city": sa.get("city"), "state": sa.get("state"),
                            "country": sa.get("country","Singapore"),
                            "postal_code": sa.get("postal_code",""),
                        }
            except: pass

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

    # Pull full customer data for credit_term + address
    cust_raw = {}
    try:
        _db = get_db()
        _row = _db.execute("SELECT raw FROM customers WHERE customer_id=?", (store_id,)).fetchone()
        _db.close()
        if _row: cust_raw = json.loads(_row["raw"] or "{}")
    except: pass

    b2b         = cust_raw.get("b2b_settings") or {}
    ct_obj      = b2b.get("credit_term") or {}
    bl          = cust_raw.get("branch_list") or []
    ship_addr   = bl[0].get("shipping_address",{}) if bl else {}
    bill_addr   = bl[0].get("billing_address",{})  if bl else {}
    ship_addr_id = ship_addr.get("address_id","")
    bill_addr_id = bill_addr.get("address_id","")
    currency_val = b2b.get("currency","SGD")
    if isinstance(currency_val, dict): currency_val = currency_val.get("currency","SGD")

    # Generate temp order number — SNC will assign the real one
    order_number = f"WA-{datetime.now().strftime('%d%m%Y-%H%M%S')}"

    order_payload = {
        "company_id": company_id, "source": "B2B", "platform": "Whatsapp",
        "invoice_number": order_number,
        "store_id": store_id, "store": store,
        "branch_id": branch_id, "branch_name": branch_name,
        "slot_info": slot_info,
        "currency": currency_val or "SGD",
        "items_count": len(item_info),
        "order_status": "Pending", "paid_status": "Pending",
        "credit_term": ct_obj,
        "delivery_type": "Delivery", "delivery_date": delivery,
        "item_total": round(item_total, 5),
        "tax_amount": round(tax_total, 5),
        "total_amount": round(item_total + tax_total, 5),
        "created_by": username, "updated_by": username,
        "created_by_id": user_id, "user_id": user_id, "user_name": username,
        "item_info": item_info,
        "tax_code": {"tax_code":"SR9","tax_rate":9,"tax_code_id":"r4L4SqU4QSmMqU3EsOYJtg",
            "to_date":None,"from_date":"2024-01-01 05:30:00","description":"SR9",
            "is_deleted":False,"_SNCBaseObject__api_version":"TaxCodes","_SNCBaseObject__type":"1.0"},
        "tax_inclusive": False,
        "discount_info": {"discount_type":"% Discount","discount":0,"discount_amount":0,
            "discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
        "coupon_info": {"discount_type":"% Discount","discount":0,"discount_amount":0,
            "discount_id":None,"discount_name":None,"deal_id":None,"deal_name":None,"coupon_code":None},
        "installation_info": {"installation":False,"installation_date":None,"installation_charges":0},
        "free_shipping": False, "payment_info": [], "applied_credit_notes": [],
        "advance_amount": 0, "delivery_charges": 0,
        "delivery_address_id":   ship_addr_id,
        "delivery_address_info": {
            "address1":    ship_addr.get("address1",""),
            "address2":    ship_addr.get("address2",None),
            "city":        ship_addr.get("city",None),
            "state":       ship_addr.get("state",None),
            "country":     ship_addr.get("country","Singapore"),
            "postal_code": ship_addr.get("postal_code",""),
        },
        "billing_address_id":   bill_addr_id,
        "billing_address_info": {
            "address1":    bill_addr.get("address1",""),
            "address2":    bill_addr.get("address2",None),
            "city":        bill_addr.get("city",None),
            "state":       bill_addr.get("state",None),
            "country":     bill_addr.get("country","Singapore"),
            "postal_code": bill_addr.get("postal_code",""),
        },
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


def build_reply(analysis: Dict, sender: str) -> str:
    """Generate a reply based on intent analysis."""
    intent = analysis.get("intent","UNKNOWN")
    items  = analysis.get("items",[])
    order_number = analysis.get("order_number")

    if analysis.get("reply"):
        return analysis["reply"]

    if intent == "GREETING":
        return f"Hi {sender}! 👋 How can I help you today? You can place an order by telling me what you need."

    if intent == "NEW_ORDER":
        if items:
            lines = "\n".join([f"  • {i.get('name','?')} × {i.get('qty',1)} {i.get('uom','unit')}" for i in items])
            return f"Got it {sender}! 🛒\n\n{lines}\n\nReply *CONFIRM* to place this order, or let me know your delivery date."
        return f"Sure {sender}! What would you like to order? Please include product name and quantity."

    if intent == "ORDER_STATUS":
        if order_number:
            return f"Let me check order *{order_number}* for you. Please wait a moment."
        return f"Could you share your order number? It looks like WA-DDMMYYYY-001."

    if intent == "AMEND_ORDER":
        return f"I'll help you amend your order. What changes would you like to make?"

    if intent == "CANCEL_ORDER":
        return f"I'll process your cancellation request. Could you confirm the order number?"

    if intent == "ENQUIRY":
        return f"Happy to help with your enquiry {sender}! What would you like to know?"

    return f"Thanks {sender}! How can I help you today?"


def build_reply(analysis: Dict, sender: str) -> str:
    """Build a reply message based on intent analysis."""
    intent = analysis.get("intent","UNKNOWN")
    ai_reply = analysis.get("reply","")
    if ai_reply:
        return ai_reply
    replies = {
        "GREETING":     f"Hi {sender}! 👋 How can I help you today? You can send me your order and I'll process it right away.",
        "ORDER_STATUS": f"Let me check your order status. Please provide your order number (e.g. B2B-DDMMYYYY-001).",
        "AMEND_ORDER":  f"Sure {sender}, I can help amend your order. Please share the order number and what changes you'd like.",
        "CANCEL_ORDER": f"I'll help you cancel the order. Please provide the order number to proceed.",
        "ENQUIRY":      f"Happy to help with your enquiry! What would you like to know?",
        "UNKNOWN":      f"Hi {sender}! I'm your ordering assistant. Send me your order (e.g. '5kg chicken, 3kg beef') and I'll process it for you.",
    }
    return replies.get(intent, replies["UNKNOWN"])


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
    # Also try rule-based extraction if AI found no items
    if intent == "NEW_ORDER":
        ai_items = analysis.get("items",[])
        # Always try rule-based extraction to supplement AI results
        if not ai_items:
            ai_items = extract_items(text)
            analysis["items"] = ai_items
        elif len(ai_items) == 1:
            # AI may have missed items — check rule-based too
            rule_items = extract_items(text)
            if len(rule_items) > len(ai_items):
                ai_items = rule_items
                analysis["items"] = ai_items
        enriched = await lookup_products_for_items(ai_items) if ai_items else []
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


@app.get("/debug/isteaks-full")
async def debug_isteaks_full():
    """Get complete iSTEAKS record from SNC to find branch_id and internal IDs."""
    # Try custom=True which returns full data including internal IDs
    result = await snc_call("/customers/get", {
        "custom": True,
        "data": {"filter_by": {"customer_id": "121212", "company_id": "mindmasters"}}
    })
    if not result:
        return {"error": "No result"}
    r    = result.get("result", {})
    meta = r.get("metadata", {})
    cl   = meta.get("CustomersList", [])
    if not cl:
        return {"meta_keys": list(meta.keys()), "raw": str(result)[:1000]}
    c   = cl[0]
    b2b = c.get("b2b_settings", {})
    return {
        "all_keys":      list(c.keys()),
        "customer_id":   c.get("customer_id"),
        "_id":           c.get("_id"),
        "id":            c.get("id"),
        "erp_id":        c.get("erp_id"),
        "branch_list":   c.get("branch_list", []),
        "b2b_stores":    b2b.get("stores", []),
        "b2b_keys":      list(b2b.keys()),
        "raw_snippet":   str(c)[:800],
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


@app.get("/debug/products-exact-curl")
async def debug_products_exact_curl():
    """Use exact cURL payload — force fresh token first."""
    # Force token refresh
    await auto_refresh_token()
    token = credentials["snc"].get("access_token","")
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")

    payload = {
        "company_id": "mindmasters",
        "data": {"filter_by": {
            "date_range": [],
            "search_on": ["name","sku","product_code"],
            "search_text": "",
            "exact_match": False,
            "pagination": {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False},
            "view": "individual", "status": "All",
            "include_columns": ["sku","short_name","name","parent_sku","product_code",
                "category","sub_category","brand","uom","base_uom","prices","images",
                "status","is_sellable","is_edited","b2b_enabled","pos_enabled","is_primary",
                "merged_id","merged_skus","bom_type","linked_stores","inventory_type",
                "quantity","uom_id","config","attribute_set",
                "warehouse_type_code","warehouse_type_description"],
            "merged": True, "bundles": False,
        }},
        "user_id": "1qxcb0ssTRGPQaKogvtkMw",
        "username": "admin@mindmastersg.com",
        "timezone": "Asia/Singapore",
        "request_from": "WEB",
    }
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(f"{api_url}/products/list", json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        data = resp.json()
        job_id = data.get("job_id")
        if not job_id:
            return {"step":"no_job_id","http":resp.status_code,"data":data}
        poll = await poll_job(job_id, timeout=40)
        if not poll:
            return {"step":"poll_timeout"}
        meta  = poll.get("result",{}).get("metadata",{})
        prods = meta.get("products",[])
        # Show token prefix so we can compare with browser
        return {
            "token_prefix": token[:40]+"...",
            "count": meta.get("count",0),
            "products_returned": len(prods),
            "poll_success": poll.get("status",{}).get("success"),
            "sample": [{"name":p.get("name"),"sku":p.get("sku"),"item_id":p.get("item_id")} for p in prods[:5]],
        }


@app.get("/debug/products-letter-search")
async def debug_products_exact_curl():
    """Use exact cURL payload from browser — raw response."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    # Exact payload from browser cURL — word for word
    payload = {
        "company_id": "mindmasters",
        "data": {"filter_by": {
            "date_range": [],
            "search_on": ["name","sku","product_code"],
            "search_text": "",
            "exact_match": False,
            "pagination": {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False},
            "view": "individual",
            "status": "All",
            "include_columns": ["sku","short_name","name","parent_sku","product_code",
                "category","sub_category","brand","uom","base_uom","prices","images",
                "status","is_sellable","is_edited","b2b_enabled","pos_enabled","is_primary",
                "merged_id","merged_skus","bom_type","linked_stores","inventory_type",
                "quantity","uom_id","config","attribute_set",
                "warehouse_type_code","warehouse_type_description"],
            "merged": True,
            "bundles": False,
        }},
        "user_id":      "1qxcb0ssTRGPQaKogvtkMw",
        "username":     "admin@mindmastersg.com",
        "timezone":     "Asia/Singapore",
        "request_from": "WEB",
    }
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(f"{api_url}/products/list", json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        data = resp.json()
        job_id = data.get("job_id")
        if not job_id:
            return {"step":"no_job_id","http":resp.status_code,"data":data}
        poll = await poll_job(job_id, timeout=40)
        if not poll:
            return {"step":"poll_timeout","job_id":job_id}
        meta  = poll.get("result",{}).get("metadata",{})
        prods = meta.get("products",[])
        return {
            "http": resp.status_code,
            "job_id": job_id,
            "poll_success": poll.get("status",{}).get("success"),
            "poll_message": poll.get("status",{}).get("message",""),
            "count": meta.get("count",0),
            "products_returned": len(prods),
            "meta_keys": list(meta.keys()),
            "full_poll_snippet": str(poll)[:800],
            "sample": prods[:2] if prods else [],
        }


@app.get("/debug/products-letter-search")
async def debug_products_letter_search():
    """Search products using single letters to bypass empty search_text restriction."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_products = {}

    for letter in ["a","e","i","o","the","chicken","beef","pork","fish","rice","cheese","butter","cake","frozen","fresh"]:
        try:
            payload = {
                "company_id": SNC_HARDCODED_COMPANY,
                "user_id":    SNC_HARDCODED_USER_ID,
                "username":   SNC_HARDCODED_USERNAME,
                "timezone":   "Asia/Singapore",
                "request_from": "WEB",
                "data": {"filter_by": {
                    "date_range": [],
                    "relation_id": "121212",
                    "search_on": ["name","sku","product_code"],
                    "confidence": 0.1,
                    "search_text": [letter],
                    "exact_match": False,
                    "pagination": {"page_no":1,"no_of_recs":50,"sort_by":"cts","order_by":False},
                    "view": "individual", "status": "Active",
                    "include_columns": ["name","sku","uom","uom_id","prices","item_id","status","b2b_enabled"],
                    "merged": True, "bundles": False,
                }}
            }
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(f"{api_url}/products/list", json=payload, headers=headers)
                data = resp.json()
                job_id = data.get("job_id")
                if not job_id: continue
                poll = await poll_job(job_id, timeout=30)
                if not poll: continue
                status = poll.get("status",{})
                if not status.get("success",True): continue
                prods = poll.get("result",{}).get("metadata",{}).get("products",[])
                for p in prods:
                    iid = p.get("item_id","")
                    if iid and iid not in all_products:
                        all_products[iid] = p
        except Exception as e:
            logger.warning("letter search %s failed: %s", letter, e)
            continue

    products = list(all_products.values())
    if products:
        db_upsert_products(products)
        return {"found": len(products), "saved": True,
                "sample": [{"name":p.get("name"),"sku":p.get("sku"),"item_id":p.get("item_id")} for p in products[:10]]}
    return {"found": 0, "message": "No products found with any search term — check SNC product setup"}


@app.get("/debug/save-all-products-v2")
async def debug_save_all_products_v2():
    """Fetch and save ALL 513 products - paginate all pages."""
    await auto_refresh_token()
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    total_saved = 0
    page = 1
    while True:
        payload = {
            "company_id": SNC_HARDCODED_COMPANY,
            "user_id": SNC_HARDCODED_USER_ID,
            "username": SNC_HARDCODED_USERNAME,
            "timezone": "Asia/Singapore",
            "request_from": "WEB",
            "data": {"filter_by": {
                "date_range": [],
                "search_on": ["name","sku","product_code"],
                "search_text": "",
                "exact_match": False,
                "pagination": {"page_no": page, "no_of_recs": 40, "sort_by": "cts", "order_by": False},
                "view": "individual", "status": "All",
                "include_columns": ["sku","short_name","name","product_code","category",
                    "uom","base_uom","prices","status","b2b_enabled","uom_id","quantity"],
                "merged": True, "bundles": False,
            }}
        }
        async with httpx.AsyncClient(timeout=40) as client:
            resp = await client.post(f"{api_url}/products/list", json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
            data = resp.json()
            job_id = data.get("job_id")
            if not job_id:
                break
            poll = await poll_job(job_id, timeout=40)
            if not poll:
                break
            meta  = poll.get("result",{}).get("metadata",{})
            prods = meta.get("products",[])
            if not prods:
                break
            # Save with correct price extraction
            db = get_db()
            saved = 0
            for p in prods:
                item_id = p.get("item_id") or p.get("sku","")
                if not item_id: continue
                prices = p.get("prices") or []
                price = 0.0
                for tier_name in ["Default Price","B2B","POS"]:
                    t = next((x for x in prices if x.get("name") == tier_name), None)
                    if t and float(t.get("price",0) or 0) > 0:
                        price = float(t["price"]); break
                if price == 0.0:
                    for t in prices:
                        v = float(t.get("price",0) or 0)
                        if v > 0: price = v; break
                uom_id = p.get("uom_id","") or ""
                if isinstance(uom_id, dict): uom_id = uom_id.get("uom_id","")
                try:
                    db.execute("""INSERT OR REPLACE INTO products
                        (item_id,sku,name,short_name,uom,uom_id,base_uom,base_uom_id,
                         price,tax_rate,tax_code,product_code,category,status,raw,synced_at)
                        VALUES(?,?,?,?,?,?,?,?,?,9,'SR9',?,?,?,?,datetime('now'))""",
                        (item_id, p.get("sku",""), p.get("name",""), p.get("short_name","") or p.get("name",""),
                         p.get("uom",""), uom_id, p.get("base_uom","") or p.get("uom",""), uom_id,
                         price, p.get("product_code",""), p.get("category",""),
                         p.get("status","Active"), json.dumps(p)))
                    saved += 1
                except Exception as e:
                    logger.warning("product insert error: %s", e)
            db.commit(); db.close()
            total_saved += saved
            if len(prods) < 40:
                break
            page += 1
    db2 = get_db()
    db_total = db2.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    sample = db2.execute("SELECT item_id,sku,name,uom,price FROM products WHERE price > 0 LIMIT 5").fetchall()
    db2.close()
    return {"pages_fetched": page, "total_saved": total_saved, "total_in_db": db_total,
            "sample": [dict(r) for r in sample]}


@app.get("/debug/products-raw-response")
async def debug_products_raw_response():
    """Show complete raw SNC response for products."""
    await auto_refresh_token()
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    # Exact portal payload — search_text="" not [], status=All
    payload = {
        "company_id":   "mindmasters",
        "user_id":      "1qxcb0ssTRGPQaKogvtkMw",
        "username":     "admin@mindmastersg.com",
        "timezone":     "Asia/Singapore",
        "request_from": "WEB",
        "data": {"filter_by": {
            "date_range": [],
            "search_on":  ["name","sku","product_code"],
            "search_text": "",
            "exact_match": False,
            "pagination":  {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False},
            "view":    "individual",
            "status":  "All",
            "include_columns": ["sku","short_name","name","product_code","category","sub_category",
                "brand","uom","base_uom","prices","status","b2b_enabled","quantity","uom_id"],
            "merged":  True,
            "bundles": False,
        }}
    }
    async with httpx.AsyncClient(timeout=40) as client:
        resp = await client.post(f"{api_url}/products/list", json=payload,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        data = resp.json()
        job_id = data.get("job_id")
        if not job_id:
            return {"step": "no_job_id", "response": data}
        poll = await poll_job(job_id, timeout=40)
        if not poll:
            return {"step": "poll_timeout", "job_id": job_id}
        meta  = poll.get("result",{}).get("metadata",{})
        prods = meta.get("products",[])
        saved = 0
        if prods:
            db_upsert_products(prods)
            saved = len(prods)
        return {
            "job_id": job_id,
            "count_in_snc": meta.get("count",0),
            "products_returned": len(prods),
            "saved_to_db": saved,
            "poll_success": poll.get("status",{}).get("success"),
            "sample": [{"name":p.get("name"),"sku":p.get("sku"),"uom":p.get("uom"),"prices":p.get("prices",[])} for p in prods[:5]],
        }


@app.get("/debug/products-portal-sim")
async def debug_products_portal_sim():
    """Simulate exactly how the SNC B2B portal loads products."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    results = {}

    base = {
        "company_id": SNC_HARDCODED_COMPANY,
        "user_id":    SNC_HARDCODED_USER_ID,
        "username":   SNC_HARDCODED_USERNAME,
        "timezone":   "Asia/Singapore",
        "request_from": "WEB",
    }

    async def test(label, extra_filter):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                payload = {**base, "data": {"filter_by": {
                    "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
                    "view": "individual",
                    "include_columns": ["name","sku","uom","uom_id","prices","item_id","b2b_enabled","status"],
                    "merged": True, "bundles": False,
                    **extra_filter
                }}}
                resp = await client.post(f"{api_url}/products/list", json=payload, headers=headers)
                data = resp.json()
                job_id = data.get("job_id")
                if not job_id:
                    results[label] = {"no_job_id": str(data)[:200]}
                    return
                poll = await poll_job(job_id)
                meta = (poll or {}).get("result",{}).get("metadata",{})
                prods = meta.get("products",[])
                results[label] = {
                    "count": meta.get("count",0),
                    "returned": len(prods),
                    "success": (poll or {}).get("status",{}).get("success"),
                    "msg": (poll or {}).get("status",{}).get("message",""),
                    "sample": prods[0].get("name") if prods else None,
                }
        except Exception as e:
            results[label] = {"error": str(e)}

    # No search_text at all, no confidence
    await test("no_search_no_confidence", {"status":"All"})
    # confidence=0 explicitly
    await test("confidence_0", {"status":"All","confidence":0,"search_text":[],"search_on":["name"],"exact_match":False})
    # b2b_enabled=true, no search
    await test("b2b_true_no_search", {"status":"Active","b2b_enabled":True,"confidence":0,"search_text":[],"search_on":["name"],"exact_match":False})
    # relation_id=121212, confidence=0
    await test("relation_121212_conf0", {"status":"All","relation_id":"121212","confidence":0,"search_text":[],"search_on":["name"],"exact_match":False})
    # Try without merged
    await test("no_merged", {"status":"All","merged":False,"confidence":0,"search_text":[],"search_on":["name"],"exact_match":False})
    # Try with date_range last year
    await test("with_date_range", {
        "status":"All","confidence":0,"search_text":[],"search_on":["name"],"exact_match":False,
        "date_range":[(datetime.now()-timedelta(days=365)).strftime("%d-%m-%Y"), datetime.now().strftime("%d-%m-%Y")]
    })

    return results


@app.get("/debug/products-try-all-customers")
async def debug_products_try_all_customers():
    """Try products/list with every customer_id in DB as relation_id."""
    db = get_db()
    customers = db.execute("SELECT customer_id, customer_name FROM customers LIMIT 20").fetchall()
    db.close()
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    results = {}
    for row in customers:
        cid = row["customer_id"]
        payload = {
            "company_id": SNC_HARDCODED_COMPANY,
            "user_id":    SNC_HARDCODED_USER_ID,
            "username":   SNC_HARDCODED_USERNAME,
            "timezone":   "Asia/Singapore",
            "request_from": "WEB",
            "data": {"filter_by": {
                "date_range": [], "relation_id": cid,
                "search_on": ["name","sku"], "confidence": 0.5,
                "search_text": [], "exact_match": False,
                "pagination": {"page_no":1,"no_of_recs":3,"sort_by":"cts","order_by":False},
                "view": "individual", "status": "All",
                "include_columns": ["name","sku","uom","uom_id","prices","item_id","b2b_enabled"],
                "merged": True, "bundles": False,
            }}
        }
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(f"{api_url}/products/list", json=payload, headers=headers)
                data = resp.json()
                job_id = data.get("job_id")
                if job_id:
                    poll = await poll_job(job_id)
                    meta = (poll or {}).get("result",{}).get("metadata",{})
                    prods = meta.get("products",[])
                    count = meta.get("count",0)
                    if count > 0:
                        results[cid] = {"name": row["customer_name"], "count": count, "sample": prods[0].get("name") if prods else None}
                    # Only store non-zero results to keep response clean
        except Exception as e:
            pass
    if not results:
        return {"message": "No customer returned products — products may need B2B enabling in SNC", "customers_tried": [dict(r) for r in customers]}
    return {"working_relation_ids": results}


@app.get("/sync/products-from-orders")
@app.post("/sync/products-from-orders")
async def sync_products_from_orders():
    """Extract products from past B2B orders and save to DB — workaround for B2B products API."""
    today     = datetime.now().strftime("%d-%m-%Y")
    from_date = (datetime.now() - timedelta(days=365)).strftime("%d-%m-%Y")
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source": "B2B", "platform": "Whatsapp",
        "store_id": [], "branch_id": [], "status": "All",
        "date_range": [from_date, today],
        "search_on": ["order_number"], "search_text": "", "exact_match": False,
        "pagination": {"page_no":1,"no_of_recs":200,"sort_by":"order_cts","order_by":False},
        "whatsapp_contact_id": [], "review_required": False,
        "packer_id":"","sales_person_id":"","date_range_by":"order_cts","warehouse_id":"All","tags":[],
    }}})
    if not result:
        return {"error": "Could not fetch orders from SNC"}
    r      = result.get("result",{})
    meta   = r.get("metadata",{})
    orders = meta.get("OrderList") or meta.get("orders") or []
    seen   = {}
    for o in orders:
        for item in (o.get("item_info") or []):
            iid = item.get("item_id","")
            if iid and iid not in seen:
                seen[iid] = {
                    "item_id":     iid,
                    "name":        item.get("name",""),
                    "sku":         item.get("sku",""),
                    "uom":         item.get("uom",""),
                    "uom_id":      item.get("uom_id",""),
                    "base_uom":    item.get("base_uom","") or item.get("uom",""),
                    "base_uom_id": item.get("base_uom_id","") or item.get("uom_id",""),
                    "price":       float(item.get("price",0) or 0),
                    "tax_code":    item.get("tax_code","SR9"),
                    "tax_code_id": item.get("tax_code_id",""),
                    "tax_rate":    float(item.get("tax_rate",9) or 9),
                    "status":      "Active",
                }
    products = list(seen.values())
    if products:
        db_upsert_products(products)
    return {"orders_scanned": len(orders), "products_saved": len(products),
            "products": [{"item_id":p["item_id"],"name":p["name"],"sku":p["sku"],"uom":p["uom"],"price":p["price"]} for p in products]}


@app.get("/debug/products-from-orders")
async def debug_products_from_orders():
    """Extract unique products from past SNC orders — workaround for products API."""
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source": "B2B", "platform": "Whatsapp",
        "store_id": [], "branch_id": [], "status": "All",
        "date_range": [
            (datetime.now() - timedelta(days=365)).strftime("%d-%m-%Y"),
            datetime.now().strftime("%d-%m-%Y"),
        ],
        "search_on": ["order_number"], "search_text": "", "exact_match": False,
        "pagination": {"page_no":1,"no_of_recs":100,"sort_by":"order_cts","order_by":False},
        "whatsapp_contact_id": [], "review_required": False,
        "packer_id":"","sales_person_id":"","date_range_by":"order_cts","warehouse_id":"All","tags":[],
    }}})
    if not result:
        return {"error": "No orders found"}
    r     = result.get("result",{})
    meta  = r.get("metadata",{})
    orders = meta.get("OrderList") or meta.get("orders") or []
    
    # Extract unique products from order items
    seen = {}
    for o in orders:
        for item in (o.get("item_info") or []):
            iid = item.get("item_id","")
            if iid and iid not in seen:
                seen[iid] = {
                    "item_id":    iid,
                    "name":       item.get("name",""),
                    "sku":        item.get("sku",""),
                    "uom":        item.get("uom",""),
                    "uom_id":     item.get("uom_id",""),
                    "base_uom":   item.get("base_uom",""),
                    "base_uom_id":item.get("base_uom_id",""),
                    "price":      item.get("price",0),
                    "tax_code":   item.get("tax_code","SR9"),
                    "tax_code_id":item.get("tax_code_id",""),
                    "tax_rate":   item.get("tax_rate",9),
                    "status":     "Active",
                }
    products = list(seen.values())
    if products:
        db_upsert_products(products)
    return {
        "orders_scanned": len(orders),
        "unique_products_found": len(products),
        "products": products[:10],
    }


@app.get("/debug/products-b2b")
async def debug_products_b2b():
    """Try different product endpoints and params to find what works."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    results = {}

    base = {
        "company_id": SNC_HARDCODED_COMPANY,
        "user_id":    SNC_HARDCODED_USER_ID,
        "username":   SNC_HARDCODED_USERNAME,
        "timezone":   "Asia/Singapore",
        "request_from": "WEB",
    }

    async def try_payload(label, payload):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(f"{api_url}/products/list", json=payload, headers=headers)
                data = resp.json()
                job_id = data.get("job_id")
                if job_id:
                    poll = await poll_job(job_id)
                    r    = (poll or {}).get("result",{})
                    meta = r.get("metadata",{})
                    prods = meta.get("products",[])
                    results[label] = {
                        "count": meta.get("count",0),
                        "returned": len(prods),
                        "success": (poll or {}).get("status",{}).get("success"),
                        "error": (poll or {}).get("status",{}).get("message",""),
                        "sample": prods[0].get("name") if prods else None,
                    }
                else:
                    results[label] = {"error": "no job_id", "resp": str(data)[:200]}
        except Exception as e:
            results[label] = {"error": str(e)}

    # Test 1: status=Active, no search, no relation_id
    await try_payload("active_no_filter", {**base, "data": {"filter_by": {
        "search_text": [], "search_on": ["name"], "confidence": 0,
        "exact_match": False, "merged": True, "bundles": False,
        "view": "individual", "status": "Active",
        "include_columns": ["name","sku","uom","uom_id","prices","item_id"],
        "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
    }}})

    # Test 2: status=All
    await try_payload("all_status", {**base, "data": {"filter_by": {
        "search_text": [], "search_on": ["name"], "confidence": 0,
        "exact_match": False, "merged": True, "bundles": False,
        "view": "individual", "status": "All",
        "include_columns": ["name","sku","uom","uom_id","prices","item_id"],
        "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
    }}})

    # Test 3: b2b_enabled filter
    await try_payload("b2b_enabled", {**base, "data": {"filter_by": {
        "search_text": [], "search_on": ["name"], "confidence": 0,
        "exact_match": False, "merged": True, "bundles": False,
        "view": "individual", "status": "All", "b2b_enabled": True,
        "include_columns": ["name","sku","uom","uom_id","prices","item_id","b2b_enabled"],
        "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
    }}})

    # Test 4: with iSTEAKS branch_id
    await try_payload("with_branch_id", {**base, "data": {"filter_by": {
        "search_text": [], "search_on": ["name"], "confidence": 0,
        "exact_match": False, "merged": True, "bundles": False,
        "view": "individual", "status": "All",
        "store_id": "121212", "branch_id": "QUtl0kOMQRaTDgMuGGf16A",
        "include_columns": ["name","sku","uom","uom_id","prices","item_id"],
        "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
    }}})

    # Test 5: pos endpoint instead
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}/products/get", json={**base, "data": {"filter_by": {
                "search_text": [], "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
                "status": "Active",
            }}}, headers=headers)
            results["products_get_endpoint"] = {"status": resp.status_code, "snippet": resp.text[:200]}
    except Exception as e:
        results["products_get_endpoint"] = {"error": str(e)}

    return results


@app.get("/debug/products-all")
async def debug_products_all():
    """Get all products with no search_text filter."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    # Key insight: search_text must be [] (empty), no relation_id, confidence=0
    payload = {
        "company_id": SNC_HARDCODED_COMPANY,
        "user_id":    SNC_HARDCODED_USER_ID,
        "username":   SNC_HARDCODED_USERNAME,
        "timezone":   "Asia/Singapore",
        "request_from": "WEB",
        "data": {"filter_by": {
            "date_range": [],
            "search_on": ["name","sku"],
            "confidence": 0,
            "search_text": [],
            "exact_match": False,
            "pagination": {"page_no":1,"no_of_recs":10,"sort_by":"cts","order_by":False},
            "view": "individual", "status": "Active",
            "include_columns": ["name","sku","uom","uom_id","prices","item_id","status","b2b_enabled","quantity"],
            "merged": True, "bundles": False,
        }}
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}/products/list", json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
            data = resp.json()
            job_id = data.get("job_id")
            if not job_id:
                return {"error": "no job_id", "response": data}
            poll = await poll_job(job_id)
            if not poll:
                return {"error": "poll timed out"}
            r    = poll.get("result",{})
            meta = r.get("metadata",{})
            prods = meta.get("products",[])
            return {
                "success": poll.get("status",{}).get("success"),
                "count": meta.get("count",0),
                "products_returned": len(prods),
                "meta_keys": list(meta.keys()),
                "sample": [{k: p.get(k) for k in ["item_id","name","sku","uom","prices","b2b_enabled"]} for p in prods[:3]],
                "error_msg": poll.get("status",{}).get("message",""),
            }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/debug/products-direct")
async def debug_products_direct():
    """Hit products API directly without snc_call wrapper to see raw response."""
    api_url = credentials["snc"].get("api_url","https://enterprise.sellnchill.com/api")
    token   = credentials["snc"].get("access_token","")
    payload = {
        "company_id": SNC_HARDCODED_COMPANY,
        "user_id":    SNC_HARDCODED_USER_ID,
        "username":   SNC_HARDCODED_USERNAME,
        "timezone":   "Asia/Singapore",
        "request_from": "WEB",
        "data": {"filter_by": {
            "date_range": [],
            "relation_id": "121212",
            "search_on": ["name","sku"],
            "confidence": 0.5,
            "search_text": ["chicken"],
            "exact_match": False,
            "pagination": {"page_no":1,"no_of_recs":5,"sort_by":"cts","order_by":False},
            "view": "individual", "status": "All",
            "include_columns": ["name","sku","uom","uom_id","prices","item_id","status"],
            "merged": True, "bundles": False,
        }}
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{api_url}/products/list",
                json=payload,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
            raw = resp.text
            data = resp.json()
            job_id = data.get("job_id")
            poll_result = None
            if job_id:
                poll_result = await poll_job(job_id)
            return {
                "http_status": resp.status_code,
                "has_job_id": bool(job_id),
                "job_id": job_id,
                "direct_keys": list(data.keys()),
                "direct_snippet": raw[:300],
                "poll_result_keys": list(poll_result.keys()) if poll_result else None,
                "poll_snippet": str(poll_result)[:500] if poll_result else None,
            }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/debug/products-search-test")
async def debug_products_search_test():
    """Test products with search_text as Postman does."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [],
        "relation_id": "121212",
        "search_on": ["name","sku","product_code"],
        "confidence": 0.5,
        "search_text": ["chicken","beef","fish","pork","vegetable"],
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 10, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": ["sku","short_name","name","uom","base_uom","prices","status","b2b_enabled","uom_id","quantity","item_id"],
        "merged": True, "bundles": False,
    }}})
    if not result:
        return {"error": "snc_call returned None"}
    r    = result.get("result",{})
    meta = r.get("metadata",{})
    prods = meta.get("products",[])

    # Also try with empty search_text but different params
    result2 = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [],
        "search_on": ["name"],
        "confidence": 0.0,
        "search_text": ["a"],
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 10, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": ["sku","name","uom","prices","uom_id","item_id"],
        "merged": True, "bundles": False,
    }}})
    meta2 = (result2 or {}).get("result",{}).get("metadata",{})
    prods2 = meta2.get("products",[])

    return {
        "search_chicken_count": len(prods),
        "search_a_count": len(prods2),
        "meta_keys": list(meta.keys()),
        "first": {k: prods[0].get(k) for k in ["item_id","name","sku","uom","prices"]} if prods else None,
        "first2": {k: prods2[0].get(k) for k in ["item_id","name","sku","uom","prices"]} if prods2 else None,
    }


@app.get("/debug/products-raw-test")
async def debug_products_raw_test():
    """Test products API with exact Postman payload — show raw response."""
    # Try with iSTEAKS customer_id = 121212
    tests = ["121212", "11001058", ""]
    results = {}
    for rid in tests:
        result = await snc_call("/products/list", {"data": {"filter_by": {
            "date_range": [],
            "relation_id": rid,
            "search_on": ["name","sku","product_code"],
            "confidence": 0.5,
            "search_text": [],
            "exact_match": False,
            "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "cts", "order_by": False},
            "view": "individual", "status": "All",
            "include_columns": ["short_description","description","sku","short_name","name",
                "parent_sku","product_code","category","sub_category","brand","uom","base_uom",
                "prices","images","status","is_sellable","b2b_enabled","uom_id","quantity"],
            "merged": True, "bundles": False,
        }}})
        if result:
            r    = result.get("result",{})
            meta = r.get("metadata",{})
            prods = meta.get("products",[])
            results[rid or "EMPTY"] = {
                "count": meta.get("count",0),
                "meta_keys": list(meta.keys()),
                "product_count": len(prods),
                "first_product": {k: prods[0].get(k) for k in ["item_id","name","sku","uom","prices","b2b_enabled"]} if prods else None,
            }
        else:
            results[rid or "EMPTY"] = {"error": "snc_call returned None"}

    # Also show what customer IDs are in DB
    db = get_db()
    rows = db.execute("SELECT customer_id, customer_name, branch_id FROM customers LIMIT 10").fetchall()
    db.close()
    return {
        "product_tests": results,
        "customers_in_db": [dict(r) for r in rows],
    }


@app.get("/debug/sync-products-now")
async def debug_sync_products_now():
    """Sync 100 products and show result."""
    try:
        _db2 = get_db()
        _rr = _db2.execute("SELECT customer_id FROM customers WHERE customer_id NOT LIKE '%-%-%' AND branch_id != '' LIMIT 1").fetchone()
        _db2.close()
        relation_id_used = _rr["customer_id"] if _rr else "NONE"

        page=1; total=0
        while True:
            count = await sync_products_from_snc(page=page, per_page=40)
            total += count
            if count < 40: break
            page += 1
        db = get_db()
        db_total = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        sample = db.execute("SELECT item_id,sku,name,uom,price,status FROM products WHERE price > 0 LIMIT 5").fetchall()
        db.close()
        return {
            "synced": total,
            "total_in_db": db_total,
            "sample": [dict(r) for r in sample],
        }
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/debug/products-relation-ids")
async def debug_products_relation_ids():
    """Show what relation IDs we have and test each format."""
    db = get_db()
    rows = db.execute("SELECT customer_id, customer_name, raw FROM customers LIMIT 10").fetchall()
    db.close()
    results = []
    for r in rows:
        raw = {}
        try: raw = json.loads(r["raw"] or "{}")
        except: pass
        bl = raw.get("branch_list") or []
        results.append({
            "customer_id":    r["customer_id"],
            "customer_name":  r["customer_name"],
            "erp_id":         raw.get("erp_id"),
            "erp_tenant_name":raw.get("erp_tenant_name"),
            "netsuite_ids":   [b.get("netsuite_internal_id") for b in bl],
        })
    return {"customers": results}


@app.get("/debug/products-with-relation/{relation_id}")
async def debug_products_with_relation(relation_id: str):
    """Test products fetch with a specific relation_id (customer_id like 121212)."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [],
        "relation_id": relation_id,
        "search_on": ["name","sku","product_code"],
        "confidence": 0.5, "search_text": [],
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": ["short_description","description","sku","short_name","name",
            "parent_sku","product_code","category","sub_category","brand",
            "uom","base_uom","prices","images","status","is_sellable",
            "b2b_enabled","uom_id","config","attribute_set"],
        "merged": True, "bundles": False,
    }}})
    if not result:
        return {"error": "snc_call returned None"}
    r    = result.get("result", {})
    meta = r.get("metadata", {})
    prods = meta.get("products", [])
    return {
        "count":      meta.get("count", 0),
        "meta_keys":  list(meta.keys()),
        "first_product_keys": list(prods[0].keys()) if prods else [],
        "sample":     [{k: p.get(k) for k in ["item_id","name","sku","uom","uom_id","prices","b2b_enabled"]} for p in prods[:3]],
    }


@app.get("/debug/products-test")
async def debug_products_test():
    """Show raw SNC products response to find correct key."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "date_range": [], "search_on": ["name","sku","product_code"],
        "confidence": 0.5, "search_text": [],
        "exact_match": False,
        "pagination": {"page_no": 1, "no_of_recs": 3, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "All",
        "include_columns": ["name","sku","prices","uom","base_uom","uom_id","item_id","tax_code","tax_code_id","tax_rate","b2b_enabled","status"],
        "merged": True, "bundles": False,
    }}})
    if not result:
        return {"error": "snc_call returned None"}
    r    = result.get("result", {})
    meta = r.get("metadata", {})
    return {
        "result_keys":    list(r.keys()),
        "metadata_keys":  list(meta.keys()),
        "meta_sample":    str(meta)[:1000],
        "full_snippet":   str(result)[:1500],
    }


@app.get("/debug/test-order")
async def debug_test_order():
    """Test order with real iSTEAKS IDs fetched from SNC."""
    # Step 1: Get real internal IDs for iSTEAKS
    result = await snc_call("/customers/get", {
        "custom": True,
        "data": {"filter_by": {"customer_id": "121212", "company_id": SNC_HARDCODED_COMPANY}}
    })
    store_id    = "121212"
    branch_id   = ""
    branch_name = "Main Branch"
    if result:
        r    = result.get("result",{})
        meta = r.get("metadata",{})
        cl   = meta.get("CustomersList",[])
        if cl:
            c  = cl[0]
            # branch_id is in branch_list, not b2b_settings.stores
            bl = c.get("branch_list",[])
            if bl:
                branch_id   = bl[0].get("branch_id","")
                branch_name = bl[0].get("branch_name","Main Branch")
            store_id = c.get("customer_id","121212")
            logger.info("iSTEAKS: store_id=%s branch_id=%s", store_id, branch_id)

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
@app.get("/debug/reset-db")
async def debug_reset_db():
    """Force drop and recreate customers and orders tables with new schema."""
    try:
        db = get_db()
        db.execute("DROP TABLE IF EXISTS customers")
        db.execute("DROP TABLE IF EXISTS orders")
        db.execute("DROP TABLE IF EXISTS products")
        db.commit()

        # Recreate with full new schema inline
        db.execute("""CREATE TABLE IF NOT EXISTS customers (
            customer_id   TEXT PRIMARY KEY,
            customer_code TEXT,
            customer_name TEXT,
            customer_type TEXT,
            phone         TEXT,
            email         TEXT,
            uen           TEXT,
            store_id      TEXT,
            store         TEXT,
            branch_id     TEXT,
            branch_name   TEXT,
            address       TEXT,
            credit_term   TEXT,
            credit_term_id TEXT,
            price_tier    TEXT,
            outstanding_amount REAL DEFAULT 0,
            max_credit_limit   REAL DEFAULT 0,
            min_order_amount   REAL DEFAULT 0,
            tax_code      TEXT,
            currency      TEXT,
            status        TEXT,
            billing_id    TEXT,
            payment_mode  TEXT,
            sales_person  TEXT,
            b2b_stores    TEXT,
            raw           TEXT,
            synced_at     TEXT
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id      TEXT PRIMARY KEY,
            order_number  TEXT,
            customer_id   TEXT,
            store_id      TEXT,
            store         TEXT,
            branch_id     TEXT,
            branch_name   TEXT,
            status        TEXT,
            paid_status   TEXT,
            delivery_date TEXT,
            delivery_type TEXT,
            item_total    REAL DEFAULT 0,
            tax_amount    REAL DEFAULT 0,
            total_amount  REAL DEFAULT 0,
            items_count   INTEGER DEFAULT 0,
            item_info     TEXT,
            chat_id       TEXT,
            platform      TEXT,
            source        TEXT,
            created_by    TEXT,
            raw           TEXT,
            synced_at     TEXT
        )""")
        db.execute("""CREATE TABLE IF NOT EXISTS products (
            item_id      TEXT PRIMARY KEY,
            sku          TEXT,
            name         TEXT,
            short_name   TEXT,
            uom          TEXT,
            uom_id       TEXT,
            base_uom     TEXT,
            base_uom_id  TEXT,
            price        REAL DEFAULT 0,
            tax_code     TEXT DEFAULT 'SR9',
            tax_code_id  TEXT,
            tax_rate     REAL DEFAULT 9,
            product_code TEXT,
            category     TEXT,
            quantity     REAL DEFAULT 0,
            status       TEXT DEFAULT 'Active',
            raw          TEXT,
            synced_at    TEXT
        )""")
        db.commit()
        db.close()

        # Verify columns
        db2 = get_db()
        cust_cols = [r[1] for r in db2.execute("PRAGMA table_info(customers)").fetchall()]
        prod_cols = [r[1] for r in db2.execute("PRAGMA table_info(products)").fetchall()]
        db2.close()
        return {"success": True, "customer_columns": cust_cols, "product_columns": prod_cols}
    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.get("/debug/sync-now")
async def debug_sync_now():
    """Run customer sync synchronously and return exact error."""
    try:
        result = await snc_call("/customers/get", {
            "custom": True,
            "data": {"filter_by": {
                "company_id": SNC_HARDCODED_COMPANY,
                "search_on": ["customer_name"],
                "search_text": "", "exact_match": False,
                "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "cts", "order_by": False},
            }}
        })
        if not result:
            return {"error": "snc_call returned None"}

        r    = result.get("result", {})
        meta = r.get("metadata", {})
        customers = meta.get("CustomersList", [])

        if not customers:
            return {"error": "No CustomersList in response", "meta_keys": list(meta.keys()), "result_keys": list(r.keys())}

        # Try upserting first customer only
        c = customers[0]
        import traceback as tb
        try:
            db_upsert_customers([c])
            upsert_ok = True
            upsert_error = None
        except Exception as e:
            upsert_ok = False
            upsert_error = tb.format_exc()

        return {
            "snc_returned": len(customers),
            "first_customer_keys": list(c.keys()),
            "first_customer_id": c.get("customer_id"),
            "first_customer_name": c.get("customer_name"),
            "b2b_keys": list((c.get("b2b_settings") or {}).keys()),
            "upsert_ok": upsert_ok,
            "upsert_error": upsert_error,
        }
    except Exception as e:
        import traceback as tb
        return {"error": str(e), "traceback": tb.format_exc()}


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


@app.get("/sync/customers")
@app.post("/sync/customers")
async def sync_customers():
    """Sync customers from SNC synchronously and return count."""
    try:
        page=1; total=0
        while True:
            count = await sync_customers_from_snc(page=page, per_page=100)
            total += count
            if count < 100: break
            page += 1
        return {"status":"ok","synced":total}
    except Exception as e:
        import traceback
        return {"status":"error","error":str(e),"traceback":traceback.format_exc()}

async def _sync_customers_task():
    page=1; total=0
    while True:
        count = await sync_customers_from_snc(page=page, per_page=100)
        total += count
        logger.info("Sync progress: page=%d got=%d running_total=%d", page, count, total)
        if count < 100: break
        page += 1
    logger.info("Customer sync COMPLETE: %d customers stored", total)

# ─── Product sync endpoint ───────────────────────────────────────────────────
@app.get("/sync/products")
@app.post("/sync/products")
async def sync_products_endpoint():
    """Sync ALL products from SNC - paginate through all pages."""
    try:
        await auto_refresh_token()
        page = 1; total = 0
        while True:
            count = await sync_products_from_snc(page=page, per_page=40)
            total += count
            if count < 40: break
            page += 1
        db = get_db()
        db_total = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        db.close()
        return {"status": "ok", "synced": total, "total_in_db": db_total}
    except Exception as e:
        import traceback as tb
        return {"status": "error", "error": str(e), "traceback": tb.format_exc()}


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

@app.post("/products/manual")
async def add_product_manual(request: Request):
    """Manually add a product to the local DB."""
    body = await request.json()
    item_id = body.get("item_id") or body.get("sku","")
    if not item_id:
        raise HTTPException(400, "item_id or sku required")
    db_upsert_products([{
        "item_id":     item_id,
        "name":        body.get("name",""),
        "sku":         body.get("sku",""),
        "uom":         body.get("uom","unit"),
        "uom_id":      body.get("uom_id",""),
        "base_uom":    body.get("uom","unit"),
        "base_uom_id": body.get("uom_id",""),
        "price":       float(body.get("price",0)),
        "tax_code":    body.get("tax_code","SR9"),
        "tax_code_id": body.get("tax_code_id","r4L4SqU4QSmMqU3EsOYJtg"),
        "tax_rate":    float(body.get("tax_rate",9)),
        "status":      "Active",
    }])
    return {"status": "ok", "item_id": item_id}


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
