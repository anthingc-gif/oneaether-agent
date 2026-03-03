"""
oneaether.ai Agent Server
=========================
FastAPI server running at http://158.140.133.82:8000

Flow:
  WhatsApp → Whapi → SellnChill ERP webhook → POST /process-message (this server)
  → AI logic → POST /whatsapp/messages/send (SellnChill ERP sends reply via Whapi)

Endpoints:
  POST /process-message        - Receives Whapi webhook events from SellnChill ERP
  POST /credentials            - Save credentials from CRM frontend
  GET  /health                 - Health check
  GET  /proxy/whapi/chats      - Proxy: get Whapi chats (for CRM frontend)
  GET  /proxy/whapi/messages/{chat_id} - Proxy: get messages for a chat
  POST /proxy/whapi/send       - Proxy: send WhatsApp message via Whapi
  POST /proxy/snc/customers    - Proxy: get SNC customers
  POST /proxy/snc/products     - Proxy: get SNC products
  POST /proxy/snc/orders       - Proxy: get SNC orders
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("oneaether-agent")

# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="oneaether.ai Agent",
    description="WhatsApp AI Agent connecting Whapi ↔ SellnChill ERP",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── In-memory credential store (replace with DB/file in production) ─────────
# These are populated by the CRM frontend via POST /credentials
credentials: Dict[str, Any] = {
    "snc": {
        "api_url": os.getenv("SNC_API_URL", ""),          # e.g. https://dev2.snc.delivernchill.com/api
        "access_token": os.getenv("SNC_TOKEN", ""),
        "user_id": os.getenv("SNC_USER_ID", ""),
        "username": os.getenv("SNC_USERNAME", ""),
        "company_id": os.getenv("SNC_COMPANY_ID", ""),
        "timezone": os.getenv("SNC_TIMEZONE", "Asia/Singapore"),
    },
    "whapi": {
        "base_url": os.getenv("WHAPI_BASE_URL", "https://gate.whapi.cloud"),
        "token": os.getenv("WHAPI_TOKEN", ""),
        "channel_id": os.getenv("WHAPI_CHANNEL_ID", ""),
    },
}

# ─── Pydantic models ─────────────────────────────────────────────────────────

class CredentialsPayload(BaseModel):
    snc: Optional[Dict[str, str]] = None
    whapi: Optional[Dict[str, str]] = None


class SendMessagePayload(BaseModel):
    contact_id: str          # e.g. "120363420772664070@g.us"
    body: str
    type: str = "text"
    phone_number: Optional[str] = None
    media: Optional[str] = None
    mentions: Optional[List[str]] = None


# ─── Helpers ─────────────────────────────────────────────────────────────────

def snc_headers() -> Dict[str, str]:
    token = credentials["snc"].get("access_token", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def snc_base() -> Dict[str, str]:
    """Common fields required in every SNC request body."""
    return {
        "company_id": credentials["snc"].get("company_id", ""),
        "user_id": credentials["snc"].get("user_id", ""),
        "username": credentials["snc"].get("username", ""),
        "timezone": credentials["snc"].get("timezone", "Asia/Singapore"),
        "request_from": "WEB",
    }


def whapi_headers() -> Dict[str, str]:
    token = credentials["whapi"].get("token", "")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


async def poll_job(job_id: str, timeout: int = 30) -> Optional[Dict]:
    """Poll SellnChill /job/{job_id} until done or timeout."""
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        logger.error("SNC api_url not configured")
        return None

    url = f"{api_url}/job/{job_id}"
    deadline = time.time() + timeout

    async with httpx.AsyncClient(timeout=15) as client:
        while time.time() < deadline:
            try:
                resp = await client.get(url, headers=snc_headers())
                data = resp.json()
                # Job done when process_status == "processed" or result exists
                if data.get("process_status") == "processed" or data.get("result"):
                    return data
                # Still pending — wait a bit
                await asyncio.sleep(1.5)
            except Exception as e:
                logger.warning(f"Job poll error: {e}")
                await asyncio.sleep(2)

    logger.warning(f"Job {job_id} timed out after {timeout}s")
    return None


async def snc_call(endpoint: str, body: Dict) -> Optional[Dict]:
    """
    Make a POST to SNC ERP, then poll the returned job_id for results.
    Returns the final result dict, or None on failure.
    """
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        logger.error("SNC api_url not configured — call POST /credentials first")
        return None

    url = f"{api_url}{endpoint}"
    payload = {**snc_base(), **body}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(url, json=payload, headers=snc_headers())
            data = resp.json()
            logger.info(f"SNC {endpoint} → status {resp.status_code}")

            if not data.get("status", {}).get("success"):
                logger.error(f"SNC error: {data}")
                return None

            job_id = data.get("job_id")
            if job_id:
                return await poll_job(job_id)
            return data
        except Exception as e:
            logger.error(f"SNC call failed ({endpoint}): {e}")
            return None


async def send_whatsapp_reply(contact_id: str, message: str,
                               phone_number: Optional[str] = None,
                               mentions: Optional[List[str]] = None) -> bool:
    """Send a WhatsApp reply via SellnChill ERP's /whatsapp/messages/send."""
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        logger.error("Cannot send reply — SNC not configured")
        return False

    payload = {
        **snc_base(),
        "data": {
            "contact_id": contact_id,
            "type": "text",
            "body": message,
            **({"phone_number": phone_number} if phone_number else {}),
            **({"mentions": mentions} if mentions else {}),
        },
    }

    url = f"{api_url}/whatsapp/messages/send"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(url, json=payload, headers=snc_headers())
            data = resp.json()
            success = data.get("status", {}).get("success", False)
            logger.info(f"Send reply to {contact_id}: {'OK' if success else 'FAILED'}")
            return success
        except Exception as e:
            logger.error(f"send_whatsapp_reply failed: {e}")
            return False


# ─── AI Message Processing ───────────────────────────────────────────────────

async def process_incoming_message(
    message: Dict,
    contact: Dict,
    company_id: str,
    chat_id: str,
) -> str:
    """
    Core AI logic: understand the customer's message and generate a reply.

    The flow here is intentionally simple/rule-based so you can see it working
    and then replace with an LLM call (Claude, GPT, etc.) later.

    message fields: id, from_me, type, chat_id, text.body, from, from_name
    contact fields: id, name, pushname, customers (list of SNC customer records)
    """
    text = ""
    if message.get("type") == "text":
        text = message.get("text", {}).get("body", "").strip().lower()

    sender_name = message.get("from_name") or contact.get("name") or "there"

    # ── Match customer in SNC ────────────────────────────────────────────────
    snc_customers = contact.get("customers", [])
    snc_customer_id = snc_customers[0]["customer_id"] if snc_customers else None
    snc_customer_name = snc_customers[0]["customer_name"] if snc_customers else None

    logger.info(f"[AI] From: {sender_name} | Customer: {snc_customer_name} | Msg: {text!r}")

    # ── Intent detection (extend this with an LLM for smarter behaviour) ────
    if any(kw in text for kw in ["hi", "hello", "hey", "good morning", "good afternoon"]):
        greeting = f"Hi {sender_name}! 👋 Welcome to our ordering portal."
        if snc_customer_name:
            greeting += f" I can see you're ordering as *{snc_customer_name}*."
        greeting += "\n\nHow can I help you today?\n• Type *products* to browse our catalogue\n• Type *orders* to check your recent orders\n• Type *order <item name>* to place a new order"
        return greeting

    elif "product" in text or "catalogue" in text or "catalog" in text or "menu" in text:
        # Fetch product list from SNC
        result = await snc_call("/products/list", {
            "data": {
                "filter_by": {
                    "search_text": [],
                    "search_on": ["name", "sku", "product_code"],
                    "confidence": 0.5,
                    "exact_match": False,
                    "pagination": {"page_no": 1, "no_of_recs": 10, "sort_by": "cts", "order_by": False},
                    "view": "individual",
                    "status": "Active",
                    "include_columns": ["name", "sku", "prices", "uom"],
                    "merged": True,
                    "bundles": False,
                }
            }
        })

        if result and result.get("result", {}).get("data"):
            products = result["result"]["data"]
            lines = ["Here are some of our products:\n"]
            for p in products[:8]:
                name = p.get("name", "Unknown")
                sku = p.get("sku", "")
                price = ""
                prices = p.get("prices", [])
                if prices:
                    price = f" — ${prices[0].get('price', '')} {p.get('uom', '')}"
                lines.append(f"• *{name}* ({sku}){price}")
            lines.append("\nType *order <product name> x <qty>* to order!")
            return "\n".join(lines)
        return "I couldn't fetch the product list right now. Please try again in a moment."

    elif text.startswith("order") and len(text) > 6:
        # Simple order intent — acknowledge and note it for manual processing
        item_text = text[6:].strip()
        return (
            f"Got it! I've noted your order request for *{item_text}*. "
            f"Our team will confirm your order shortly.\n\n"
            f"_To place orders directly, please use our B2B portal or speak to your sales rep._"
        )

    elif any(kw in text for kw in ["order", "orders", "my order", "status"]):
        # Fetch recent orders for this customer
        if snc_customer_id:
            today = datetime.now().strftime("%d-%m-%Y")
            # Use a 30-day range
            from datetime import timedelta
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
            result = await snc_call("/b2b/orders/get", {
                "data": {
                    "include_orders": True,
                    "filter_by": {
                        "source": "B2B",
                        "platform": "Whatsapp",
                        "store_id": [],
                        "branch_id": [],
                        "status": "All",
                        "date_range": [thirty_days_ago, today],
                        "search_on": ["order_number", "reference", "store", "tags"],
                        "search_text": "",
                        "exact_match": False,
                        "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "order_cts", "order_by": False},
                        "whatsapp_contact_id": [chat_id],
                    }
                }
            })

            if result and result.get("result", {}).get("data"):
                orders = result["result"]["data"]
                lines = [f"Here are your recent orders, {sender_name}:\n"]
                for o in orders[:5]:
                    num = o.get("order_number", "—")
                    status = o.get("order_status", "Unknown")
                    total = o.get("total_amount", 0)
                    date = o.get("delivery_date", "")
                    lines.append(f"• *{num}* | {status} | ${total:.2f} | Delivery: {date}")
                return "\n".join(lines)

        return "I couldn't find any recent orders. Please check with your sales rep for order status."

    elif any(kw in text for kw in ["help", "?"]):
        return (
            f"Hi {sender_name}! Here's what I can help you with:\n\n"
            "• *products* — Browse our product catalogue\n"
            "• *orders* — Check your recent orders\n"
            "• *order <item>* — Request a new order\n\n"
            "For urgent matters, please contact your sales representative directly."
        )

    else:
        # Default fallback
        return (
            f"Thanks for your message, {sender_name}! 🙏\n\n"
            "I'm your ordering assistant. Type *help* to see what I can do, "
            "or reach out to your sales rep for assistance."
        )


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the oneaether.html frontend."""
    html_file = Path(__file__).parent / "oneaether.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text())
    return HTMLResponse(content="<h1>oneaether.ai</h1><p>Frontend not found. Upload oneaether.html next to main.py</p>")

@app.get("/health")
async def health():
    """Health check endpoint."""
    snc_ok = bool(credentials["snc"].get("access_token"))
    whapi_ok = bool(credentials["whapi"].get("token"))
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "snc_configured": snc_ok,
        "whapi_configured": whapi_ok,
    }


@app.post("/credentials")
async def save_credentials(payload: CredentialsPayload):
    """
    Called by the CRM frontend ConnectScreen to save credentials.
    Merges provided fields into the in-memory store.
    """
    if payload.snc:
        credentials["snc"].update({k: v for k, v in payload.snc.items() if v})
        logger.info("SNC credentials updated")

    if payload.whapi:
        credentials["whapi"].update({k: v for k, v in payload.whapi.items() if v})
        logger.info("Whapi credentials updated")

    return {"status": "ok", "message": "Credentials saved successfully"}


@app.get("/credentials/status")
async def credentials_status():
    """Returns which credentials are configured (never returns secrets)."""
    return {
        "snc": {
            "api_url": credentials["snc"].get("api_url", ""),
            "company_id": credentials["snc"].get("company_id", ""),
            "username": credentials["snc"].get("username", ""),
            "timezone": credentials["snc"].get("timezone", ""),
            "authenticated": bool(credentials["snc"].get("access_token")),
        },
        "whapi": {
            "base_url": credentials["whapi"].get("base_url", ""),
            "channel_id": credentials["whapi"].get("channel_id", ""),
            "configured": bool(credentials["whapi"].get("token")),
        },
    }


@app.post("/process-message")
async def process_message(request: Request, background_tasks: BackgroundTasks):
    """
    Main webhook endpoint. Receives events from SellnChill ERP (forwarded from Whapi).

    Expected payload shape (from Postman collection "Agent Webhook"):
    {
        "messages": [...],
        "event": {"type": "messages", "event": "post"},
        "channel_id": "SHAZAM-HGS85",
        "contacts": {...},
        "company_id": "mindmasters"
    }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    event = body.get("event", {})
    messages = body.get("messages", [])
    contact = body.get("contacts", {})
    company_id = body.get("company_id", credentials["snc"].get("company_id", ""))

    logger.info(f"Webhook received: event={event}, messages={len(messages)}, company={company_id}")

    # Only process inbound text/media messages (skip echoes from ourselves)
    for message in messages:
        if message.get("from_me"):
            logger.debug(f"Skipping own message {message.get('id')}")
            continue

        msg_type = message.get("type", "")
        chat_id = message.get("chat_id", "")
        sender_phone = message.get("from", "")

        if msg_type not in ("text", "image", "document", "audio"):
            logger.debug(f"Skipping unsupported message type: {msg_type}")
            continue

        # Process in background so we return 200 immediately to Whapi
        background_tasks.add_task(
            handle_message_background,
            message=message,
            contact=contact,
            company_id=company_id,
            chat_id=chat_id,
        )

    return JSONResponse({"status": "ok", "received": len(messages)})


async def handle_message_background(
    message: Dict,
    contact: Dict,
    company_id: str,
    chat_id: str,
):
    """Background task: generate AI reply and send it."""
    try:
        reply = await process_incoming_message(
            message=message,
            contact=contact,
            company_id=company_id,
            chat_id=chat_id,
        )

        sender_phone = message.get("from", "")
        await send_whatsapp_reply(
            contact_id=chat_id,
            message=reply,
            phone_number=sender_phone if not chat_id.endswith("@g.us") else None,
        )
    except Exception as e:
        logger.error(f"handle_message_background error: {e}", exc_info=True)


# ─── Proxy Routes (for CRM frontend to avoid CORS issues) ────────────────────

@app.get("/proxy/whapi/chats")
async def proxy_whapi_chats(page: int = 1, count: int = 20):
    """Proxy: fetch chats from Whapi (called by CRM frontend)."""
    base = credentials["whapi"].get("base_url", "https://gate.whapi.cloud")
    url = f"{base}/chats"
    params = {"page": page, "count": count}

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, headers=whapi_headers(), params=params)
            return resp.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@app.get("/proxy/whapi/messages/{chat_id}")
async def proxy_whapi_messages(chat_id: str, page: int = 1, count: int = 40):
    """Proxy: fetch messages for a chat from Whapi."""
    base = credentials["whapi"].get("base_url", "https://gate.whapi.cloud")
    url = f"{base}/messages/list/{chat_id}"
    params = {"page": page, "count": count}

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url, headers=whapi_headers(), params=params)
            return resp.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=str(e))


@app.post("/proxy/whapi/send")
async def proxy_whapi_send(payload: SendMessagePayload):
    """Proxy: send a WhatsApp message via SellnChill ERP."""
    success = await send_whatsapp_reply(
        contact_id=payload.contact_id,
        message=payload.body,
        phone_number=payload.phone_number,
        mentions=payload.mentions,
    )
    if not success:
        raise HTTPException(status_code=502, detail="Failed to send message")
    return {"status": "ok", "sent": True}


@app.post("/proxy/snc/customers")
async def proxy_snc_customers(request: Request):
    """Proxy: search SNC customers (passes through filter_by from request body)."""
    body = await request.json()
    filter_by = body.get("filter_by", {})

    result = await snc_call("/customers/get", {
        "data": {"filter_by": {
            "search_on": ["customer_id", "customer_name", "customer_phone", "customer_email"],
            "exact_match": False,
            "pagination": {"page_no": 1, "no_of_recs": 40, "sort_by": "cts", "order_by": False},
            **filter_by,
        }}
    })

    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result


@app.post("/proxy/snc/products")
async def proxy_snc_products(request: Request):
    """Proxy: search SNC products."""
    body = await request.json()
    search_text = body.get("search_text", [])
    relation_id = body.get("relation_id", "")

    result = await snc_call("/products/list", {
        "data": {
            "filter_by": {
                "search_text": search_text if isinstance(search_text, list) else [search_text],
                "search_on": ["name", "sku", "product_code"],
                "confidence": 0.5,
                "exact_match": False,
                "pagination": {"page_no": 1, "no_of_recs": 40, "sort_by": "cts", "order_by": False},
                "view": "individual",
                "status": "All",
                "include_columns": ["name", "sku", "prices", "uom", "quantity", "images"],
                "merged": True,
                "bundles": False,
                **({"relation_id": relation_id} if relation_id else {}),
            }
        }
    })

    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result


@app.post("/proxy/snc/orders")
async def proxy_snc_orders(request: Request):
    """Proxy: get B2B orders."""
    body = await request.json()
    date_range = body.get("date_range", [])
    whatsapp_contact_id = body.get("whatsapp_contact_id", [])
    status = body.get("status", "All")

    from datetime import timedelta
    if not date_range:
        today = datetime.now().strftime("%d-%m-%Y")
        thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
        date_range = [thirty_ago, today]

    result = await snc_call("/b2b/orders/get", {
        "data": {
            "include_orders": True,
            "filter_by": {
                "source": "B2B",
                "platform": "Whatsapp",
                "store_id": [],
                "branch_id": [],
                "status": status,
                "date_range": date_range,
                "search_on": ["order_number", "reference", "store", "tags"],
                "search_text": "",
                "exact_match": False,
                "pagination": {"page_no": 1, "no_of_recs": 40, "sort_by": "order_cts", "order_by": False},
                "whatsapp_contact_id": whatsapp_contact_id,
                "review_required": False,
            }
        }
    })

    if result is None:
        raise HTTPException(status_code=502, detail="SNC API call failed")
    return result


# ─── Startup / main ──────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event():
    logger.info("=" * 60)
    logger.info("oneaether.ai Agent Server starting up")
    logger.info(f"SNC API URL : {credentials['snc'].get('api_url') or '⚠️  NOT SET'}")
    logger.info(f"SNC Token   : {'✓ set' if credentials['snc'].get('access_token') else '⚠️  NOT SET'}")
    logger.info(f"Whapi URL   : {credentials['whapi'].get('base_url')}")
    logger.info(f"Whapi Token : {'✓ set' if credentials['whapi'].get('token') else '⚠️  NOT SET'}")
    logger.info("=" * 60)
    logger.info("Endpoints:")
    logger.info("  POST /process-message     ← Whapi/SNC webhook")
    logger.info("  POST /credentials         ← Save credentials from CRM")
    logger.info("  GET  /health              ← Health check")
    logger.info("  GET  /proxy/whapi/chats")
    logger.info("  GET  /proxy/whapi/messages/{chat_id}")
    logger.info("  POST /proxy/whapi/send")
    logger.info("  POST /proxy/snc/customers")
    logger.info("  POST /proxy/snc/products")
    logger.info("  POST /proxy/snc/orders")
    logger.info("=" * 60)


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, log_level="info")
