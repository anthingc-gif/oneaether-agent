"""
oneaether.ai Agent Server - Railway Edition
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("oneaether")

credentials: Dict[str, Any] = {
    "snc": {
        "api_url":      os.getenv("SNC_API_URL", ""),
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
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("oneaether.ai starting on port %s", os.getenv("PORT", 8000))
    yield
    logger.info("oneaether.ai shutting down")

app = FastAPI(title="oneaether.ai", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class CredentialsPayload(BaseModel):
    snc:   Optional[Dict[str, str]] = None
    whapi: Optional[Dict[str, str]] = None

class SendMessagePayload(BaseModel):
    contact_id:   str
    body:         str
    type:         str = "text"
    phone_number: Optional[str] = None
    mentions:     Optional[List[str]] = None

def snc_headers():
    return {"Authorization": f"Bearer {credentials['snc'].get('access_token','')}", "Content-Type": "application/json"}

def snc_base():
    return {"company_id": credentials["snc"].get("company_id",""), "user_id": credentials["snc"].get("user_id",""),
            "username": credentials["snc"].get("username",""), "timezone": credentials["snc"].get("timezone","Asia/Singapore"), "request_from": "WEB"}

def whapi_headers():
    return {"Authorization": f"Bearer {credentials['whapi'].get('token','')}", "Content-Type": "application/json"}

async def poll_job(job_id: str, timeout: int = 30) -> Optional[Dict]:
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        return None
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=15) as client:
        while time.time() < deadline:
            try:
                resp = await client.get(f"{api_url}/job/{job_id}", headers=snc_headers())
                data = resp.json()
                if data.get("process_status") == "processed" or data.get("result"):
                    return data
                await asyncio.sleep(1.5)
            except Exception as e:
                logger.warning(f"Job poll error: {e}")
                await asyncio.sleep(2)
    return None

async def snc_call(endpoint: str, body: Dict) -> Optional[Dict]:
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        return None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}{endpoint}", json={**snc_base(), **body}, headers=snc_headers())
            data = resp.json()
            if not data.get("status", {}).get("success"):
                return None
            job_id = data.get("job_id")
            return await poll_job(job_id) if job_id else data
    except Exception as e:
        logger.error(f"SNC call failed ({endpoint}): {e}")
        return None

async def send_whatsapp_reply(contact_id: str, message: str, phone_number: Optional[str] = None, mentions: Optional[List[str]] = None) -> bool:
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        return False
    payload = {**snc_base(), "data": {"contact_id": contact_id, "type": "text", "body": message,
                                       **({"phone_number": phone_number} if phone_number else {}),
                                       **({"mentions": mentions} if mentions else {})}}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}/whatsapp/messages/send", json=payload, headers=snc_headers())
            return resp.json().get("status", {}).get("success", False)
    except Exception as e:
        logger.error(f"send_reply failed: {e}")
        return False

async def process_incoming_message(message: Dict, contact: Dict, company_id: str, chat_id: str) -> str:
    text = message.get("text", {}).get("body", "").strip().lower() if message.get("type") == "text" else ""
    sender_name = message.get("from_name") or contact.get("name") or "there"
    snc_customers = contact.get("customers", [])
    snc_customer_name = snc_customers[0]["customer_name"] if snc_customers else None

    if any(kw in text for kw in ["hi", "hello", "hey", "good morning"]):
        msg = f"Hi {sender_name}! 👋"
        if snc_customer_name:
            msg += f" Ordering as *{snc_customer_name}*."
        msg += "\n\n• Type *products* — browse catalogue\n• Type *orders* — check recent orders\n• Type *order <item>* — request an order"
        return msg

    elif any(kw in text for kw in ["product", "catalogue", "catalog", "menu"]):
        result = await snc_call("/products/list", {"data": {"filter_by": {"search_text": [], "search_on": ["name","sku","product_code"], "confidence": 0.5, "exact_match": False, "pagination": {"page_no":1,"no_of_recs":10,"sort_by":"cts","order_by":False}, "view":"individual","status":"Active","include_columns":["name","sku","prices","uom"],"merged":True,"bundles":False}}})
        if result and result.get("result", {}).get("data"):
            lines = ["Here are our products:\n"]
            for p in result["result"]["data"][:8]:
                prices = p.get("prices", [])
                price = f" — ${prices[0].get('price','')} {p.get('uom','')}" if prices else ""
                lines.append(f"• *{p.get('name','?')}* ({p.get('sku','')}){price}")
            lines.append("\nType *order <product> x <qty>* to order!")
            return "\n".join(lines)
        return "Couldn't fetch products right now. Please try again shortly."

    elif text.startswith("order") and len(text) > 6:
        return f"Got it! Order request noted for *{text[6:].strip()}*. Our team will confirm shortly."

    elif any(kw in text for kw in ["order", "orders", "status"]):
        today = datetime.now().strftime("%d-%m-%Y")
        thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
        result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {"source":"B2B","platform":"Whatsapp","store_id":[],"branch_id":[],"status":"All","date_range":[thirty_ago,today],"search_on":["order_number","reference"],"search_text":"","exact_match":False,"pagination":{"page_no":1,"no_of_recs":5,"sort_by":"order_cts","order_by":False},"whatsapp_contact_id":[chat_id]}}})
        if result and result.get("result", {}).get("data"):
            lines = [f"Recent orders for {sender_name}:\n"]
            for o in result["result"]["data"][:5]:
                lines.append(f"• *{o.get('order_number','—')}* | {o.get('order_status','?')} | ${o.get('total_amount',0):.2f} | {o.get('delivery_date','')}")
            return "\n".join(lines)
        return "No recent orders found. Contact your sales rep for order status."

    return f"Thanks {sender_name}! 🙏 Type *help* to see what I can do."

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_file = Path(__file__).parent / "oneaether.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text())
    return HTMLResponse(content="<h2 style='font-family:sans-serif;padding:40px'>✓ oneaether.ai backend running! Upload oneaether.html to serve frontend.</h2>")

@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(),
            "snc_configured": bool(credentials["snc"].get("access_token")),
            "whapi_configured": bool(credentials["whapi"].get("token"))}

@app.post("/credentials")
async def save_credentials(payload: CredentialsPayload):
    if payload.snc:
        credentials["snc"].update({k: v for k, v in payload.snc.items() if v})
    if payload.whapi:
        credentials["whapi"].update({k: v for k, v in payload.whapi.items() if v})
    return {"status": "ok"}

@app.get("/credentials/status")
async def credentials_status():
    return {"snc": {"api_url": credentials["snc"].get("api_url",""), "company_id": credentials["snc"].get("company_id",""), "authenticated": bool(credentials["snc"].get("access_token"))},
            "whapi": {"base_url": credentials["whapi"].get("base_url",""), "channel_id": credentials["whapi"].get("channel_id",""), "configured": bool(credentials["whapi"].get("token"))}}

@app.post("/process-message")
async def process_message(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    messages = body.get("messages", [])
    contact  = body.get("contacts", {})
    company_id = body.get("company_id", credentials["snc"].get("company_id", ""))
    for message in messages:
        if message.get("from_me"):
            continue
        if message.get("type") not in ("text", "image", "document", "audio"):
            continue
        background_tasks.add_task(handle_message_background, message=message, contact=contact, company_id=company_id, chat_id=message.get("chat_id",""))
    return JSONResponse({"status": "ok", "received": len(messages)})

async def handle_message_background(message: Dict, contact: Dict, company_id: str, chat_id: str):
    try:
        reply = await process_incoming_message(message=message, contact=contact, company_id=company_id, chat_id=chat_id)
        sender_phone = message.get("from", "")
        await send_whatsapp_reply(contact_id=chat_id, message=reply, phone_number=sender_phone if not chat_id.endswith("@g.us") else None)
    except Exception as e:
        logger.error(f"handle_message error: {e}", exc_info=True)

@app.get("/proxy/whapi/chats")
async def proxy_whapi_chats(page: int = 1, count: int = 20):
    base = credentials["whapi"].get("base_url", "https://gate.whapi.cloud")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{base}/chats", headers=whapi_headers(), params={"page": page, "count": count})
            return resp.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

@app.get("/proxy/whapi/messages/{chat_id}")
async def proxy_whapi_messages(chat_id: str, page: int = 1, count: int = 40):
    base = credentials["whapi"].get("base_url", "https://gate.whapi.cloud")
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{base}/messages/list/{chat_id}", headers=whapi_headers(), params={"page": page, "count": count})
            return resp.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))

@app.post("/proxy/whapi/send")
async def proxy_whapi_send(payload: SendMessagePayload):
    success = await send_whatsapp_reply(contact_id=payload.contact_id, message=payload.body, phone_number=payload.phone_number, mentions=payload.mentions)
    if not success:
        raise HTTPException(status_code=502, detail="Failed to send message")
    return {"status": "ok", "sent": True}

@app.post("/proxy/snc/customers")
async def proxy_snc_customers(request: Request):
    body = await request.json()
    result = await snc_call("/customers/get", {"data": {"filter_by": {"search_on": ["customer_id","customer_name","customer_phone","customer_email"], "exact_match": False, "pagination": {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False}, **body.get("filter_by", {})}}})
    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result

@app.post("/proxy/snc/products")
async def proxy_snc_products(request: Request):
    body = await request.json()
    search_text = body.get("search_text", [])
    result = await snc_call("/products/list", {"data": {"filter_by": {"search_text": search_text if isinstance(search_text, list) else [search_text], "search_on": ["name","sku","product_code"], "confidence": 0.5, "exact_match": False, "pagination": {"page_no":1,"no_of_recs":40,"sort_by":"cts","order_by":False}, "view":"individual","status":"All","include_columns":["name","sku","prices","uom","quantity","images"],"merged":True,"bundles":False}}})
    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result

@app.post("/proxy/snc/orders")
async def proxy_snc_orders(request: Request):
    body = await request.json()
    date_range = body.get("date_range", [])
    if not date_range:
        today = datetime.now().strftime("%d-%m-%Y")
        thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
        date_range = [thirty_ago, today]
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {"source":"B2B","platform":"Whatsapp","store_id":[],"branch_id":[],"status":body.get("status","All"),"date_range":date_range,"search_on":["order_number","reference","store","tags"],"search_text":"","exact_match":False,"pagination":{"page_no":1,"no_of_recs":40,"sort_by":"order_cts","order_by":False},"whatsapp_contact_id":body.get("whatsapp_contact_id",[]),"review_required":False}}})
    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, log_level="info")
