"""
oneaether.ai Agent Server - Railway Edition
AI-powered WhatsApp order intent detection & processing
"""

import asyncio
import json
import logging
import os
import re
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

# ─── Global state ─────────────────────────────────────────────────────────────
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
    "agent": {
        "enabled":        os.getenv("AGENT_ENABLED", "true").lower() == "true",
        "anthropic_key":  os.getenv("ANTHROPIC_API_KEY", ""),
        "openai_key":     os.getenv("OPENAI_API_KEY", ""),
    }
}

# Conversation memory: chat_id -> last few messages for context
conversation_memory: Dict[str, List[Dict]] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("oneaether.ai starting on port %s", os.getenv("PORT", 8000))
    logger.info("AI Agent: %s", "ENABLED" if credentials["agent"]["enabled"] else "DISABLED")
    if credentials["agent"]["anthropic_key"]:
        logger.info("AI Provider: Claude (Anthropic)")
    elif credentials["agent"]["openai_key"]:
        logger.info("AI Provider: OpenAI GPT-4o-mini")
    else:
        logger.info("AI Provider: rule-based fallback (no API key set)")
    yield
    logger.info("oneaether.ai shutting down")

app = FastAPI(title="oneaether.ai", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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
    mentions:     Optional[List[str]] = None

# ─── SNC helpers ──────────────────────────────────────────────────────────────
def snc_headers():
    return {"Authorization": f"Bearer {credentials['snc'].get('access_token','')}", "Content-Type": "application/json"}

def snc_base():
    return {
        "company_id":   credentials["snc"].get("company_id", ""),
        "user_id":      credentials["snc"].get("user_id", ""),
        "username":     credentials["snc"].get("username", ""),
        "timezone":     credentials["snc"].get("timezone", "Asia/Singapore"),
        "request_from": "WEB",
    }

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
                logger.error(f"SNC error on {endpoint}: {data}")
                return None
            job_id = data.get("job_id")
            return await poll_job(job_id) if job_id else data
    except Exception as e:
        logger.error(f"SNC call failed ({endpoint}): {e}")
        return None

async def send_whatsapp_reply(contact_id: str, message: str, phone_number: Optional[str] = None) -> bool:
    api_url = credentials["snc"].get("api_url", "")
    if not api_url:
        logger.warning("Cannot send reply — SNC not configured")
        return False
    payload = {**snc_base(), "data": {"contact_id": contact_id, "type": "text", "body": message,
                                       **({"phone_number": phone_number} if phone_number else {})}}
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_url}/whatsapp/messages/send", json=payload, headers=snc_headers())
            return resp.json().get("status", {}).get("success", False)
    except Exception as e:
        logger.error(f"send_reply failed: {e}")
        return False

# ─── Intent Detection ─────────────────────────────────────────────────────────

INTENTS = {
    "NEW_ORDER": {
        "keywords": ["order", "i want", "i need", "can i get", "please send", "buy", "purchase",
                     "add to order", "place order", "new order", "want to order", "like to order"],
        "emoji": "🛒",
        "label": "New Order",
    },
    "AMEND_ORDER": {
        "keywords": ["change", "amend", "modify", "update", "edit", "instead", "replace",
                     "add more", "reduce", "increase", "decrease", "adjust", "correction"],
        "emoji": "✏️",
        "label": "Amend Order",
    },
    "CANCEL_ORDER": {
        "keywords": ["cancel", "cancellation", "don't want", "dont want", "remove", "delete order",
                     "stop order", "no longer need", "nevermind", "never mind", "call off"],
        "emoji": "❌",
        "label": "Cancel Order",
    },
    "ENQUIRY": {
        "keywords": ["price", "how much", "available", "availability", "stock", "when", "delivery",
                     "what is", "do you have", "tell me", "info", "information", "details", "?"],
        "emoji": "💬",
        "label": "Enquiry",
    },
    "ORDER_STATUS": {
        "keywords": ["status", "where is", "my order", "tracking", "delivered", "when will",
                     "update on", "check order", "order number"],
        "emoji": "📦",
        "label": "Order Status",
    },
    "GREETING": {
        "keywords": ["hi", "hello", "hey", "good morning", "good afternoon", "good evening",
                     "morning", "afternoon", "evening", "howdy", "sup"],
        "emoji": "👋",
        "label": "Greeting",
    },
}

def detect_intent_rules(text: str) -> Tuple[str, float]:
    """Rule-based intent detection. Returns (intent, confidence 0-1)."""
    text_lower = text.lower()
    scores = {}
    for intent, config in INTENTS.items():
        matches = sum(1 for kw in config["keywords"] if kw in text_lower)
        if matches > 0:
            scores[intent] = matches / len(config["keywords"])

    if not scores:
        return "UNKNOWN", 0.0

    best = max(scores, key=scores.get)
    # Normalize confidence
    confidence = min(scores[best] * 10, 1.0)
    return best, confidence

def extract_order_items(text: str) -> List[Dict]:
    """
    Extract product names and quantities from natural language.
    e.g. "I want 3 boxes of chicken karaage and 10 pcs mushroom"
    → [{"name": "chicken karaage", "qty": 3, "uom": "boxes"}, ...]
    """
    items = []
    # Pattern: number + optional unit + product name
    patterns = [
        r'(\d+)\s*(boxes?|pcs?|packets?|pkts?|kgs?|units?|packs?|cartons?|bags?|bottles?|cans?)?\s+(?:of\s+)?([a-zA-Z][a-zA-Z\s\-]{2,40}?)(?:\s+and|\s*,|\s*$)',
        r'([a-zA-Z][a-zA-Z\s\-]{2,40}?)\s*[x×]\s*(\d+)',
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            groups = match.groups()
            if len(groups) == 3:
                qty, uom, name = groups
                items.append({"name": name.strip(), "qty": int(qty), "uom": uom or "unit"})
            elif len(groups) == 2:
                name, qty = groups
                items.append({"name": name.strip(), "qty": int(qty), "uom": "unit"})

    return items

def extract_order_number(text: str) -> Optional[str]:
    """Extract order number like B2B-22082025-003 from text."""
    pattern = r'[A-Z]{2,5}[-/]\d{6,12}[-/]\d{1,5}'
    match = re.search(pattern, text.upper())
    return match.group(0) if match else None

# ─── Claude AI Intent Analysis ────────────────────────────────────────────────

async def analyze_with_ai(
    message_text: str,
    sender_name: str,
    customer_name: Optional[str],
    recent_context: List[Dict],
) -> Dict:
    """
    Use AI (Claude or OpenAI) to understand message intent.
    Falls back to rule-based if no API key is set.
    Priority: Anthropic > OpenAI > rule-based
    """
    anthropic_key = credentials["agent"].get("anthropic_key", "")
    openai_key    = credentials["agent"].get("openai_key", "")

    # Rule-based fallback function
    def rule_based():
        intent, confidence = detect_intent_rules(message_text)
        return {
            "intent": intent,
            "confidence": confidence,
            "items": extract_order_items(message_text),
            "order_number": extract_order_number(message_text),
            "summary": f"Detected {intent} with {confidence:.0%} confidence",
            "suggested_reply": None,
            "method": "rule-based",
        }

    if not anthropic_key and not openai_key:
        return rule_based()

    # Build shared prompt content
    context_str = ""
    if recent_context:
        context_str = "\n".join([f"{m['role'].upper()}: {m['text']}" for m in recent_context[-4:]])

    system_prompt = """You are an AI agent for a B2B food distribution company using WhatsApp for orders.
Analyze incoming WhatsApp messages and extract structured intent and order data.
Always respond with valid JSON only — no markdown, no explanation.

Intent types:
- NEW_ORDER: Customer wants to place a new order
- AMEND_ORDER: Customer wants to change an existing order
- CANCEL_ORDER: Customer wants to cancel an order
- ORDER_STATUS: Customer asking about order status/tracking
- ENQUIRY: Product/price/availability question
- GREETING: Just saying hello
- UNKNOWN: Cannot determine intent

JSON response format:
{
  "intent": "NEW_ORDER",
  "confidence": 0.95,
  "items": [{"name": "product name", "qty": 5, "uom": "boxes"}],
  "order_number": null,
  "summary": "one sentence summary",
  "suggested_reply": "friendly professional reply to send back",
  "urgency": "normal"
}"""

    user_prompt = f"""Analyze this WhatsApp message:

Sender: {sender_name}
Customer account: {customer_name or 'Unknown'}
Recent conversation:
{context_str}

Latest message: "{message_text}"

Extract the intent and order details. Write a friendly, professional suggested_reply."""

    # ── Try Anthropic Claude ──
    if anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": anthropic_key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": "claude-haiku-4-5-20251001", "max_tokens": 600,
                          "system": system_prompt, "messages": [{"role": "user", "content": user_prompt}]},
                )
                data = resp.json()
                text = data["content"][0]["text"].strip()
                text = re.sub(r'^```json\s*|\s*```$', '', text.strip())
                result = json.loads(text)
                result["method"] = "claude-ai"
                return result
        except Exception as e:
            logger.error(f"Claude API error: {e} — trying OpenAI fallback")

    # ── Try OpenAI ──
    if openai_key:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                    json={
                        "model": "gpt-4o-mini",
                        "max_tokens": 600,
                        "temperature": 0.2,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": user_prompt},
                        ],
                        "response_format": {"type": "json_object"},
                    },
                )
                data = resp.json()
                text = data["choices"][0]["message"]["content"].strip()
                result = json.loads(text)
                result["method"] = "openai-gpt4o-mini"
                return result
        except Exception as e:
            logger.error(f"OpenAI API error: {e} — falling back to rule-based")

    return rule_based()

# ─── Intent Handlers ────────────────────────────────────────────────────────
# ─── Intent Handlers ──────────────────────────────────────────────────────────

async def handle_new_order(analysis: Dict, sender_name: str, chat_id: str) -> str:
    items = analysis.get("items", [])
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]

    if not items:
        return (
            f"Hi {sender_name}! 🛒 I'd like to help you place an order.\n\n"
            "Could you please specify:\n"
            "• *Product name*\n"
            "• *Quantity & unit* (e.g. 5 boxes, 10 pcs)\n"
            "• *Delivery date*\n\n"
            "Example: _I want 3 boxes chicken karaage and 10 pcs mushroom for delivery tomorrow_"
        )

    item_lines = "\n".join([f"  • {i['name']} × {i['qty']} {i['uom']}" for i in items])
    return (
        f"Got it {sender_name}! 🛒 Let me confirm your order:\n\n"
        f"{item_lines}\n\n"
        f"✅ Please reply *CONFIRM* to place this order, or let me know if anything needs to change.\n"
        f"📅 What delivery date would you like?"
    )

async def handle_amend_order(analysis: Dict, sender_name: str, chat_id: str) -> str:
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]
    order_num = analysis.get("order_number")
    items = analysis.get("items", [])
    msg = f"Got it {sender_name}! ✏️ Let me confirm the amendment:\n\n"
    if order_num:
        msg += f"📋 Order: *{order_num}*\n"
    if items:
        msg += "Changes requested:\n"
        for i in items:
            msg += f"  • {i['name']} → {i['qty']} {i['uom']}\n"
    msg += "\n✅ Reply *CONFIRM* to apply these changes, or let me know if I misunderstood."
    return msg

async def handle_cancel_order(analysis: Dict, sender_name: str, chat_id: str) -> str:
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]
    order_num = analysis.get("order_number")
    if order_num:
        return (
            f"I understand you'd like to cancel order *{order_num}*, {sender_name}. ❌\n\n"
            f"To confirm cancellation, please reply *CANCEL {order_num}*.\n\n"
            f"⚠️ Note: Orders that are already packed or dispatched may not be cancellable."
        )
    return (
        f"I understand you'd like to cancel an order, {sender_name}. ❌\n\n"
        f"Could you please provide your *order number*? (e.g. B2B-22082025-001)\n"
        f"You can find it in your order confirmation message."
    )

async def handle_order_status(analysis: Dict, sender_name: str, chat_id: str) -> str:
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]
    order_num = analysis.get("order_number")

    if order_num:
        # Try to fetch from SNC
        today = datetime.now().strftime("%d-%m-%Y")
        thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
        result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
            "source": "B2B", "platform": "Whatsapp", "store_id": [], "branch_id": [],
            "status": "All", "date_range": [thirty_ago, today],
            "search_on": ["order_number"], "search_text": order_num,
            "exact_match": True, "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "order_cts", "order_by": False},
            "whatsapp_contact_id": [],
        }}})
        if result and result.get("result", {}).get("data"):
            orders = result["result"]["data"]
            o = orders[0]
            status = o.get("order_status", "Unknown")
            total = o.get("total_amount", 0)
            delivery = o.get("delivery_date", "TBD")
            return (
                f"📦 Order *{order_num}* status:\n\n"
                f"• Status: *{status}*\n"
                f"• Total: ${total:.2f}\n"
                f"• Delivery Date: {delivery}\n\n"
                f"Contact your sales rep if you need further assistance."
            )

    # Fetch recent orders for this chat
    today = datetime.now().strftime("%d-%m-%Y")
    thirty_ago = (datetime.now() - timedelta(days=30)).strftime("%d-%m-%Y")
    result = await snc_call("/b2b/orders/get", {"data": {"include_orders": True, "filter_by": {
        "source": "B2B", "platform": "Whatsapp", "store_id": [], "branch_id": [],
        "status": "All", "date_range": [thirty_ago, today],
        "search_on": ["order_number"], "search_text": "",
        "exact_match": False, "pagination": {"page_no": 1, "no_of_recs": 5, "sort_by": "order_cts", "order_by": False},
        "whatsapp_contact_id": [chat_id],
    }}})
    if result and result.get("result", {}).get("data"):
        lines = [f"📦 Your recent orders, {sender_name}:\n"]
        for o in result["result"]["data"][:5]:
            lines.append(f"• *{o.get('order_number','—')}* | {o.get('order_status','?')} | ${o.get('total_amount',0):.2f} | {o.get('delivery_date','')}")
        return "\n".join(lines)

    return (
        f"I couldn't find recent orders for your account, {sender_name}. 📦\n\n"
        f"Please share your *order number* (e.g. B2B-22082025-001) and I'll look it up!"
    )

async def handle_enquiry(analysis: Dict, sender_name: str, chat_id: str) -> str:
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]
    return (
        f"Thanks for your enquiry, {sender_name}! 💬\n\n"
        f"I'm checking on that for you. For immediate assistance:\n"
        f"• Type *products* to browse our catalogue\n"
        f"• Contact your sales rep for pricing & availability\n\n"
        f"Is there anything specific you'd like to know?"
    )

async def handle_greeting(analysis: Dict, sender_name: str, customer_name: Optional[str]) -> str:
    if analysis.get("suggested_reply"):
        return analysis["suggested_reply"]
    msg = f"Hi {sender_name}! 👋 Welcome to oneaether ordering."
    if customer_name:
        msg += f" Great to hear from *{customer_name}*!"
    msg += (
        "\n\nHow can I help you today?\n"
        "🛒 *New order* — place an order\n"
        "✏️ *Amend order* — change an existing order\n"
        "❌ *Cancel order* — cancel an order\n"
        "📦 *Order status* — check your order\n"
        "💬 *Enquiry* — product or price info\n\n"
        "_Just type naturally — I'll understand!_"
    )
    return msg

# ─── Main AI Agent ────────────────────────────────────────────────────────────

async def process_incoming_message(
    message: Dict,
    contact: Dict,
    company_id: str,
    chat_id: str,
) -> str:
    """
    Main AI agent entry point.
    Detects intent → routes to appropriate handler → returns reply.
    """
    # Skip if agent disabled
    if not credentials["agent"]["enabled"]:
        return None  # type: ignore

    text = ""
    if message.get("type") == "text":
        text = message.get("text", {}).get("body", "").strip()
    elif message.get("type") in ("image", "document"):
        text = message.get(message["type"], {}).get("caption", "") or f"[{message['type']} received]"
    elif message.get("type") in ("audio", "voice"):
        text = "[Voice message received - please type your order for processing]"

    if not text:
        return None  # type: ignore

    sender_name  = message.get("from_name") or contact.get("name") or "there"
    snc_customers = contact.get("customers", [])
    customer_name = snc_customers[0]["customer_name"] if snc_customers else None

    # Get conversation context
    context = conversation_memory.get(chat_id, [])

    # Analyze intent
    logger.info(f"[AGENT] Analyzing: '{text[:80]}' from {sender_name}")
    analysis = await analyze_with_ai(text, sender_name, customer_name, context)
    intent = analysis.get("intent", "UNKNOWN")
    confidence = analysis.get("confidence", 0)
    logger.info(f"[AGENT] Intent: {intent} ({confidence:.0%}) via {analysis.get('method','?')}")

    # Store in conversation memory (keep last 6 turns)
    context.append({"role": "customer", "text": text})
    conversation_memory[chat_id] = context[-6:]

    # Route to handler
    intent_config = INTENTS.get(intent, {})
    emoji = intent_config.get("emoji", "💬")
    label = intent_config.get("label", intent)

    if intent == "NEW_ORDER":
        reply = await handle_new_order(analysis, sender_name, chat_id)
    elif intent == "AMEND_ORDER":
        reply = await handle_amend_order(analysis, sender_name, chat_id)
    elif intent == "CANCEL_ORDER":
        reply = await handle_cancel_order(analysis, sender_name, chat_id)
    elif intent == "ORDER_STATUS":
        reply = await handle_order_status(analysis, sender_name, chat_id)
    elif intent == "ENQUIRY":
        reply = await handle_enquiry(analysis, sender_name, chat_id)
    elif intent == "GREETING":
        reply = await handle_greeting(analysis, sender_name, customer_name)
    else:
        reply = (
            f"Thanks for your message, {sender_name}! 🙏\n\n"
            "I'm your ordering assistant. You can:\n"
            "🛒 Place a new order\n✏️ Amend an order\n❌ Cancel an order\n📦 Check order status\n💬 Make an enquiry\n\n"
            "_Just type naturally and I'll understand!_"
        )

    # Store reply in context
    conversation_memory[chat_id].append({"role": "agent", "text": reply})

    # Log intent for analytics
    logger.info(f"[AGENT] {emoji} {label} | {sender_name} | confidence={confidence:.0%} | method={analysis.get('method')}")

    return reply

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_file = Path(__file__).parent / "oneaether.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text())
    return HTMLResponse(content="<h2 style='font-family:sans-serif;padding:40px'>✓ oneaether.ai backend running!</h2>")

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "snc_configured":   bool(credentials["snc"].get("access_token")),
        "whapi_configured": bool(credentials["whapi"].get("token")),
        "agent_enabled":    credentials["agent"]["enabled"],
        "ai_model": (
            "claude-haiku" if credentials["agent"]["anthropic_key"]
            else "gpt-4o-mini" if credentials["agent"]["openai_key"]
            else "rule-based"
        ),
    }

@app.get("/agent/status")
async def agent_status():
    return {
        "enabled":       credentials["agent"]["enabled"],
        "ai_powered":    bool(credentials["agent"]["anthropic_key"]),
        "model": (
            "claude-haiku-4-5" if credentials["agent"]["anthropic_key"]
            else "gpt-4o-mini" if credentials["agent"]["openai_key"]
            else "rule-based"
        ),
        "active_chats":  len(conversation_memory),
        "intents":       list(INTENTS.keys()),
    }

@app.post("/agent/toggle")
async def toggle_agent(request: Request):
    body = await request.json()
    credentials["agent"]["enabled"] = body.get("enabled", True)
    status = "ENABLED" if credentials["agent"]["enabled"] else "DISABLED"
    logger.info(f"AI Agent {status}")
    return {"enabled": credentials["agent"]["enabled"], "status": status}

@app.post("/agent/test")
async def test_agent(request: Request):
    """Test the AI agent with a sample message without sending WhatsApp reply."""
    body = await request.json()
    text = body.get("message", "I want to order 5 boxes of chicken karaage")
    sender = body.get("sender", "Test User")

    analysis = await analyze_with_ai(text, sender, None, [])
    intent = analysis.get("intent", "UNKNOWN")

    # Generate reply
    fake_message = {"type": "text", "text": {"body": text}, "from_name": sender, "from": "test", "chat_id": "test"}
    reply = await process_incoming_message(fake_message, {}, "test", "test_chat")

    return {
        "input": text,
        "intent": intent,
        "confidence": analysis.get("confidence"),
        "items": analysis.get("items", []),
        "order_number": analysis.get("order_number"),
        "summary": analysis.get("summary"),
        "method": analysis.get("method"),
        "reply": reply,
    }

@app.post("/credentials")
async def save_credentials(payload: CredentialsPayload):
    if payload.snc:
        credentials["snc"].update({k: v for k, v in payload.snc.items() if v})
    if payload.whapi:
        credentials["whapi"].update({k: v for k, v in payload.whapi.items() if v})
    if payload.agent:
        if "enabled" in payload.agent:
            credentials["agent"]["enabled"] = payload.agent["enabled"] == "true"
        if "anthropic_key" in payload.agent and payload.agent["anthropic_key"]:
            credentials["agent"]["anthropic_key"] = payload.agent["anthropic_key"]
        if "openai_key" in payload.agent and payload.agent["openai_key"]:
            credentials["agent"]["openai_key"] = payload.agent["openai_key"]
    return {"status": "ok"}

@app.get("/credentials/status")
async def credentials_status():
    return {
        "snc":   {"api_url": credentials["snc"].get("api_url",""), "company_id": credentials["snc"].get("company_id",""), "authenticated": bool(credentials["snc"].get("access_token"))},
        "whapi": {"base_url": credentials["whapi"].get("base_url",""), "channel_id": credentials["whapi"].get("channel_id",""), "configured": bool(credentials["whapi"].get("token"))},
        "agent": {"enabled": credentials["agent"]["enabled"], "ai_powered": bool(credentials["agent"]["anthropic_key"] or credentials["agent"]["openai_key"])},
    }

@app.post("/process-message")
async def process_message(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    messages   = body.get("messages", [])
    contact    = body.get("contacts", {})
    company_id = body.get("company_id", credentials["snc"].get("company_id", ""))

    logger.info(f"Webhook: {len(messages)} messages, company={company_id}")

    for message in messages:
        if message.get("from_me"):
            continue
        if message.get("type") not in ("text", "image", "document", "audio", "voice"):
            continue
        background_tasks.add_task(
            handle_message_background,
            message=message, contact=contact,
            company_id=company_id, chat_id=message.get("chat_id", ""),
        )

    return JSONResponse({"status": "ok", "received": len(messages)})

async def handle_message_background(message: Dict, contact: Dict, company_id: str, chat_id: str):
    try:
        reply = await process_incoming_message(
            message=message, contact=contact,
            company_id=company_id, chat_id=chat_id,
        )
        if reply:
            sender_phone = message.get("from", "")
            await send_whatsapp_reply(
                contact_id=chat_id,
                message=reply,
                phone_number=sender_phone if not chat_id.endswith("@g.us") else None,
            )
    except Exception as e:
        logger.error(f"handle_message error: {e}", exc_info=True)

# ─── Proxy Routes ─────────────────────────────────────────────────────────────

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
    success = await send_whatsapp_reply(contact_id=payload.contact_id, message=payload.body, phone_number=payload.phone_number)
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
