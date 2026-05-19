"""
alert-engine/notifier/notification_service.py
Lightweight FastAPI microservice that receives dispatch requests
from the Alert Engine API and routes them to the correct notifier channel.

Run:
    uvicorn alert-engine.notifier.notification_service:app --port 8001
"""

import logging
import os
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Path

from .notifiers import get_notifier

logger = logging.getLogger("NotificationService")

app = FastAPI(
    title="Crypto Notification Service",
    description="Routes alert events to Email and Webhook channels.",
    version="1.0.0",
)


@app.post("/notify/{channel}")
async def notify(
    channel: str = Path(..., description="Notification channel: email | webhook"),
    body: Dict[str, Any] = None,
):
    """
    Dispatch an alert event to the specified notification channel.
    Expected body: { "event": {...}, "rule": {...} }
    """
    if body is None:
        raise HTTPException(status_code=400, detail="Request body is required.")

    event = body.get("event") or body   # Allow flat event payload too
    rule  = body.get("rule", {})

    notifier = get_notifier(channel)
    if not notifier:
        raise HTTPException(status_code=400, detail=f"Unknown channel: {channel}")

    success = await notifier.send(event, rule)
    if not success:
        raise HTTPException(status_code=502, detail=f"Failed to send via {channel}.")

    return {"status": "sent", "channel": channel, "alert_id": event.get("alert_id")}


@app.get("/health")
async def health():
    return {"status": "healthy"}
