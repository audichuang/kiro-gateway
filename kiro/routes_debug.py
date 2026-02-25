# -*- coding: utf-8 -*-

# Kiro Gateway - Debug Dashboard Routes
# Provides API endpoints for the built-in Debug Dashboard.

import asyncio
import json
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.responses import StreamingResponse

from kiro.debug_logger import (
    debug_logger,
    add_sse_subscriber,
    remove_sse_subscriber,
)

router = APIRouter(prefix="/debug", tags=["Debug Dashboard"])

# Path to the dashboard HTML file
DASHBOARD_HTML = Path(__file__).parent / "static" / "dashboard.html"


@router.get("/", response_class=HTMLResponse)
async def debug_dashboard():
    """Serve the Debug Dashboard HTML page."""
    if DASHBOARD_HTML.exists():
        return HTMLResponse(content=DASHBOARD_HTML.read_text(encoding="utf-8"))
    return HTMLResponse(
        content="<h1>Dashboard not found</h1><p>Missing: kiro/static/dashboard.html</p>",
        status_code=404,
    )


@router.get("/api/history")
async def get_history():
    """Return summary list of all recorded requests."""
    return JSONResponse(content=debug_logger.get_history())


@router.get("/api/history/{record_id}")
async def get_record(record_id: str):
    """Return full detail for a specific request record."""
    record = debug_logger.get_record(record_id)
    if record is None:
        return JSONResponse(content={"error": "Record not found"}, status_code=404)
    return JSONResponse(content=record)


@router.delete("/api/history")
async def clear_history():
    """Clear all request history."""
    debug_logger.clear_history()
    return JSONResponse(content={"status": "ok"})


@router.get("/api/stream")
async def sse_stream():
    """
    SSE endpoint for real-time request updates.
    
    Clients connect here to receive push notifications when new
    requests complete. Each event contains a request summary.
    """
    queue: asyncio.Queue = asyncio.Queue()
    add_sse_subscriber(queue)

    async def event_generator():
        try:
            # Send initial keepalive
            yield "data: {\"type\": \"connected\"}\n\n"
            while True:
                try:
                    record_summary = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"data: {json.dumps(record_summary)}\n\n"
                except asyncio.TimeoutError:
                    # Send keepalive comment to prevent connection timeout
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            remove_sse_subscriber(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
