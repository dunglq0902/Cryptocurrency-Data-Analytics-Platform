"""
alert-engine/notifier/notifiers.py
Notification dispatchers for Email and Webhook channels.

Each notifier exposes an async send(event, rule) coroutine.
The FastAPI notification service (below) routes to the correct notifier.
"""

import logging
import os
import smtplib
import ssl
from abc import ABC, abstractmethod
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("Notifiers")

# ─────────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────────

SMTP_HOST           = os.getenv("SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT           = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER           = os.getenv("SMTP_USER",     "")
SMTP_PASSWORD       = os.getenv("SMTP_PASSWORD", "")
EMAIL_FROM          = os.getenv("EMAIL_FROM",    "alerts@crypto-analytics.io")


# ═══════════════════════════════════════════════════════════════════════════════
# Base Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class BaseNotifier(ABC):
    @abstractmethod
    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Send a notification. Returns True on success."""

    @staticmethod
    def _format_message(event: Dict[str, Any], rule: Dict[str, Any]) -> str:
        """Build a human-readable alert message."""
        action       = event.get("action", "SIGNAL")
        symbol       = event.get("symbol", "UNKNOWN")
        close_price  = event.get("close_price")
        rsi          = event.get("rsi_14")
        macd         = event.get("macd")
        volume_ratio = event.get("volume_ratio")
        pattern      = event.get("candle_pattern", "N/A")
        timeframe    = event.get("timeframe", "")
        triggered_at = event.get("triggered_at", datetime.utcnow().isoformat())

        emoji = "🟢" if action == "BUY" else ("🔴" if action == "SELL" else "🔵")

        lines = [
            f"{emoji} *{action} Signal Triggered*",
            f"📊 Symbol:    `{symbol}` ({timeframe})",
            f"💰 Price:     `{close_price:.4f} USDT`" if close_price else "💰 Price:     N/A",
            f"📈 RSI(14):   `{rsi:.2f}`"    if rsi   is not None else "📈 RSI(14):   N/A",
            f"〰️  MACD:      `{macd:.4f}`"  if macd  is not None else "〰️  MACD:      N/A",
            f"📦 Vol Ratio: `{volume_ratio:.2f}x`" if volume_ratio else "📦 Vol Ratio: N/A",
            f"🕯️  Pattern:   `{pattern}`",
            f"⏰ At:         `{triggered_at}`",
            f"🔔 Rule ID:   `{event.get('rule_id', '')[:8]}...`",
        ]
        return "\n".join(lines)





# ═══════════════════════════════════════════════════════════════════════════════
# Email Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class EmailNotifier(BaseNotifier):
    """
    Sends alert notifications via SMTP (TLS).
    The recipient email is stored in the alert rule document.
    """

    def _build_html(self, event: Dict[str, Any], rule: Dict[str, Any]) -> str:
        action      = event.get("action", "SIGNAL")
        symbol      = event.get("symbol", "")
        close_price = event.get("close_price")
        rsi         = event.get("rsi_14")
        macd        = event.get("macd")
        timeframe   = event.get("timeframe", "")
        pattern     = event.get("candle_pattern", "N/A")
        color       = "#27ae60" if action == "BUY" else ("#e74c3c" if action == "SELL" else "#3498db")

        close_str = f"{close_price:.4f} USDT" if close_price is not None else "N/A"
        rsi_str   = f"{rsi:.2f}" if rsi is not None else "N/A"
        macd_str  = f"{macd:.4f}" if macd is not None else "N/A"

        return f"""
        <html><body style="font-family:Arial,sans-serif;max-width:500px;margin:auto;">
          <div style="background:{color};color:#fff;padding:20px;border-radius:8px 8px 0 0;">
            <h2 style="margin:0;">{action} Signal – {symbol}</h2>
            <p style="margin:4px 0;opacity:.85;">{timeframe} | {event.get('triggered_at','')[:19]} UTC</p>
          </div>
          <div style="background:#f9f9f9;padding:20px;border-radius:0 0 8px 8px;">
            <table width="100%" cellpadding="8">
              <tr><td><b>Price</b></td><td>{close_str}</td></tr>
              <tr><td><b>RSI(14)</b></td><td>{rsi_str}</td></tr>
              <tr><td><b>MACD</b></td><td>{macd_str}</td></tr>
              <tr><td><b>Pattern</b></td><td>{pattern}</td></tr>
              <tr><td><b>Rule ID</b></td><td>{event.get('rule_id','')[:12]}…</td></tr>
            </table>
            <p style="color:#888;font-size:12px;">
              You are receiving this because you set up an alert rule on Crypto Analytics Platform.
            </p>
          </div>
        </body></html>
        """

    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        recipient = rule.get("email_address") or "luudungpkt922005@gmail.com"
        if not recipient:
            logger.warning("No email_address in rule %s", rule.get("rule_id"))
            return False

        action = event.get("action", "SIGNAL")
        symbol = event.get("symbol", "")
        subject = f"[Crypto Alert] {action} Signal – {symbol}"

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_FROM
        msg["To"]      = recipient
        msg.attach(MIMEText(self._format_message(event, rule), "plain"))
        msg.attach(MIMEText(self._build_html(event, rule), "html"))

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.ehlo()
                server.starttls(context=context)
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(EMAIL_FROM, recipient, msg.as_string())
            logger.info("Email sent to %s for rule %s", recipient, event.get("rule_id"))
            return True
        except smtplib.SMTPException as exc:
            logger.error("SMTP error: %s", exc)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Email send error: %s", exc)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Webhook Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class WebhookNotifier(BaseNotifier):
    """
    POSTs alert event payload as JSON to the user-configured webhook URL.
    Supports optional HMAC-SHA256 signature for verification.
    """

    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        webhook_url = rule.get("webhook_url")
        if not webhook_url:
            logger.warning("No webhook_url in rule %s", rule.get("rule_id"))
            return False

        payload = {
            "event_type":    "alert_triggered",
            "rule_id":       event.get("rule_id"),
            "user_id":       event.get("user_id"),
            "symbol":        event.get("symbol"),
            "timeframe":     event.get("timeframe"),
            "action":        event.get("action"),
            "triggered_at":  str(event.get("triggered_at", "")),
            "data": {
                "close_price":    event.get("close_price"),
                "rsi_14":         event.get("rsi_14"),
                "macd":           event.get("macd"),
                "volume_ratio":   event.get("volume_ratio"),
                "candle_pattern": event.get("candle_pattern"),
            },
        }

        # Optional HMAC signature
        webhook_secret = rule.get("webhook_secret", "")
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if webhook_secret:
            import hashlib, hmac, json
            body      = json.dumps(payload).encode()
            signature = hmac.new(
                webhook_secret.encode(), body, hashlib.sha256
            ).hexdigest()
            headers["X-Crypto-Signature"] = f"sha256={signature}"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(webhook_url, json=payload, headers=headers)
                resp.raise_for_status()
                logger.info(
                    "Webhook delivered | url=%s rule=%s status=%d",
                    webhook_url, event.get("rule_id"), resp.status_code,
                )
                return True
        except httpx.HTTPStatusError as exc:
            logger.error("Webhook HTTP error %d: %s", exc.response.status_code, exc.response.text)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Webhook error: %s", exc)
        return False


# ─────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────
NOTIFIER_REGISTRY: Dict[str, BaseNotifier] = {
    "email":    EmailNotifier(),
    "webhook":  WebhookNotifier(),
}


def get_notifier(channel: str) -> Optional[BaseNotifier]:
    """Return the notifier instance for the given channel name."""
    notifier = NOTIFIER_REGISTRY.get(channel.lower())
    if not notifier:
        logger.warning("Unknown notification channel: %s", channel)
    return notifier
