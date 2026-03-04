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
        db.execute("""INSERT OR REPLACE INTO customers
            (customer_id,store_id,customer_name,phone,email,store,branch_id,branch_name,address,credit_term,status,raw,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
            (c.get("customer_id",""), c.get("store_id",""), c.get("customer_name",""),
             c.get("phone",""), c.get("email",""), c.get("store",""),
             c.get("branch_id",""), c.get("branch_name",""),
             json.dumps(c.get("address",{})), c.get("credit_term",""),
             c.get("status","Active"), json.dumps(c)))
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
        db.execute("""INSERT OR REPLACE INTO orders
            (order_id,order_number,customer_id,store_id,status,total_amount,delivery_date,chat_id,raw,synced_at)
            VALUES(?,?,?,?,?,?,?,?,?,datetime('now'))""",
            (o.get("order_id",""), o.get("order_number",""),
             o.get("customer_id",""), o.get("store_id",""),
             o.get("order_status",""), o.get("total_amount",0),
             o.get("delivery_date",""), o.get("whatsapp_contact_id",""),
             json.dumps(o)))
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
    logger.info("Agent: %s | AI: %s",
        "ON" if credentials["agent"]["enabled"] else "OFF",
        "Claude" if credentials["agent"]["anthropic_key"] else
        "OpenAI" if credentials["agent"]["openai_key"] else "rule-based")
    yield

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
    user_id  = credentials["snc"].get("user_id","") or "1qxcb0ssTRGPQaKogvtkMw"
    username = credentials["snc"].get("username","") or "admin@mindmastersg.com"
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

async def poll_job(job_id: str, timeout: int = 30) -> Optional[Dict]:
    api_url = credentials["snc"].get("api_url","")
    if not api_url: return None
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=10) as client:
        attempt = 0
        while time.time() < deadline:
            attempt += 1
            try:
                resp = await client.get(f"{api_url}/job/{job_id}", headers=snc_headers())
                data = resp.json()
                process_status = data.get("process_status","")
                logger.info("poll_job %s attempt=%d status=%s", job_id, attempt, process_status)
                if process_status == "processed" or data.get("result") is not None:
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
                logger.info("snc_call %s poll result keys=%s", endpoint, list(result.keys()) if result else None)
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
    result = await snc_call("/customers/get", {"data": {"filter_by": {
        "search_on": ["customer_name","customer_phone","customer_email"],
        "search_text": "", "exact_match": False,
        "pagination": {"page_no": page, "no_of_recs": per_page, "sort_by": "cts", "order_by": False},
    }}})
    if not result:
        logger.error("sync_customers: snc_call returned None")
        return 0
    logger.info("sync_customers raw result keys: %s", list(result.keys()))
    # Handle various response structures
    r = result.get("result", result)
    if isinstance(r, dict):
        customers = r.get("data", r.get("customers", []))
    elif isinstance(r, list):
        customers = r
    else:
        customers = []
    logger.info("sync_customers page=%d found=%d", page, len(customers))
    if customers:
        db_upsert_customers(customers)
    return len(customers)

async def sync_products_from_snc(page: int = 1, per_page: int = 100) -> int:
    """Pull all products from SNC and store in DB."""
    result = await snc_call("/products/list", {"data": {"filter_by": {
        "search_text": [], "search_on": ["name","sku"],
        "confidence": 0.0, "exact_match": False,
        "pagination": {"page_no": page, "no_of_recs": per_page, "sort_by": "cts", "order_by": False},
        "view": "individual", "status": "Active",
        "include_columns": ["name","sku","prices","uom","uom_id","item_id","tax_code","tax_code_id","tax_rate"],
        "merged": True, "bundles": False,
    }}})
    if not result: return 0
    products = result.get("result", {}).get("data", [])
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
    assignment = db_get_assignment(chat_id) or {}
    store_id   = assignment.get("store_id","")
    store      = assignment.get("customer","")
    branch_id  = assignment.get("branch_id","")
    branch_name= assignment.get("branch","Main Branch")
    username   = credentials["snc"].get("username","")
    user_id    = credentials["snc"].get("user_id","")
    company_id = credentials["snc"].get("company_id","mindmasters")
    delivery   = delivery_date or (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    item_info  = []
    item_total = 0.0
    tax_total  = 0.0
    for it in items:
        if not it.get("found") or not it.get("item_id"): continue
        qty        = it.get("qty",1)
        price      = it.get("price",0)
        tax_rate   = it.get("tax_rate",9)
        line_total = round(qty * price, 5)
        tax_amt    = round(line_total * tax_rate / 100, 5)
        item_total += line_total; tax_total += tax_amt
        item_info.append({
            "item_id": it["item_id"], "name": it["full_name"], "sku": it["sku"],
            "tax_code": it["tax_code"], "tax_code_id": it["tax_code_id"],
            "tax_rate": tax_rate, "tax_amount": tax_amt,
            "uom": it["uom"], "uom_id": it["uom_id"],
            "base_uom_id": it["base_uom_id"], "base_uom": it["base_uom"],
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
            "actual_sku": it["sku"], "actual_name": it["full_name"],
            "actual_uom": it["uom"], "actual_uom_id": it["uom_id"],
            "actual_base_uom_id": it["base_uom_id"], "actual_base_uom": it["base_uom"],
            "actual_conversion_rate": 1, "actual_qty_for_return": qty,
            "actual_price": price, "drop_shipping": False,
            "drop_shipping_from_product": "No Drop Ship", "tags": [],
        })
    if not item_info: return None

    result = await snc_call("/b2b/order/save", {"data": {
        "company_id": company_id, "source": "B2B", "platform": "Whatsapp",
        "store_id": store_id, "store": store,
        "branch_id": branch_id, "branch_name": branch_name,
        "currency": "SGD", "items_count": len(item_info),
        "order_status": "Pending", "paid_status": "Pending",
        "delivery_type": "Delivery", "delivery_date": delivery,
        "item_total": item_total, "tax_amount": tax_total,
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
    }})
    if result:
        order_num = result.get("result",{}).get("order_number") or result.get("order_number")
        logger.info("Order created: %s", order_num)
        return order_num
    return None

# ─── Agent message handler ────────────────────────────────────────────────────
def build_reply(analysis: Dict, sender: str) -> str:
    if analysis.get("reply"): return analysis["reply"]
    intent = analysis.get("intent","UNKNOWN")
    items  = analysis.get("items",[])
    order_num = analysis.get("order_number")

    if intent == "NEW_ORDER":
        if items:
            lines = "\n".join([f"  • *{i['name']}* × {i['qty']} {i['uom']}" for i in items])
            return f"Got it {sender}! 🛒\n\n{lines}\n\n✅ Reply *CONFIRM* to place order\n📅 Delivery date?"
        return f"Hi {sender}! 🛒 What would you like to order?\n\nExample: _5 boxes chicken karaage, delivery tomorrow_"
    elif intent == "AMEND_ORDER":
        msg = f"Sure {sender}! ✏️ "
        if order_num: msg += f"Order *{order_num}* — "
        if items: msg += "\n".join([f"{i['name']} → {i['qty']} {i['uom']}" for i in items])
        return msg + "\n\n✅ Reply *CONFIRM* to apply changes"
    elif intent == "CANCEL_ORDER":
        if order_num: return f"Cancel *{order_num}*? ❌\n\nReply *CANCEL {order_num}* to confirm."
        return f"Please share your *order number* to cancel."
    elif intent == "ORDER_STATUS":
        return f"Checking your order status, {sender}... 📦\n\nPlease share your order number if you have it."
    elif intent == "ENQUIRY":
        return f"Thanks for your enquiry {sender}! 💬\n\nContact your sales rep for pricing & availability."
    elif intent == "GREETING":
        return (f"Hi {sender}! 👋\n\nHow can I help?\n"
                "🛒 New order\n✏️ Amend order\n❌ Cancel order\n📦 Order status\n💬 Enquiry")
    return f"Thanks {sender}! How can I help you today? 🙏"

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
        if count < 100: break
        page += 1
    logger.info("Customer sync complete: %d total", total)

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
    body          = await request.json()
    chat_id       = body.get("chat_id", "")
    items_raw     = body.get("items", [])
    delivery_date = body.get("delivery_date", "")
    sender_name   = body.get("sender_name", "")

    if not items_raw:
        raise HTTPException(status_code=400, detail="No items provided")

    # Convert raw items to lookup format
    items = [{"name": i["name"], "qty": i.get("qty",1), "uom": i.get("uom","unit")} for i in items_raw if i.get("name","").strip()]

    # Look up products in SNC
    enriched = await lookup_products_for_items(items)
    found    = [i for i in enriched if i.get("found")]

    if not found:
        names = ", ".join(i["name"] for i in items)
        raise HTTPException(status_code=404, detail=f"Products not found in SNC catalogue: {names}")

    # Create order
    order_num = await push_order_to_snc(
        items=found,
        chat_id=chat_id,
        delivery_date=delivery_date,
        sender_name=sender_name,
    )

    if not order_num:
        raise HTTPException(status_code=502, detail="Order creation failed — check SNC credentials and customer assignment")

    # Send WhatsApp confirmation
    item_lines = ", ".join([f"{i['full_name']} x{i['qty']}" for i in found])
    msg = f"✅ Order *{order_num}* placed!\n📦 {item_lines}\n📅 Delivery: {delivery_date or 'as scheduled'}"
    await whapi_send(chat_id, msg)

    return {"order_number": order_num, "items": len(found), "status": "created"}


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
