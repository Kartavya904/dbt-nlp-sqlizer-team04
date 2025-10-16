# app/ai/llm.py
from __future__ import annotations
import httpx
from ..settings import settings

class LLMNotConfigured(Exception):
    pass

def _client() -> httpx.Client:
    if not settings.LLM_BASE_URL or not settings.LLM_MODEL:
        raise LLMNotConfigured("LLM not configured. Set LLM_BASE_URL and LLM_MODEL in .env")
    headers = {"Content-Type": "application/json"}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    return httpx.Client(base_url=settings.LLM_BASE_URL, headers=headers, timeout=30)

def chat_complete(system: str, user: str) -> str:
    """
    Minimal chat-completions call against any OpenAI-compatible endpoint.
    Returns assistant text. Raise if not configured.
    """
    with _client() as c:
        data = {
            "model": settings.LLM_MODEL,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0.1,
            "max_tokens": 512,
        }
        r = c.post("/chat/completions", json=data)
        r.raise_for_status()
        j = r.json()
        return j["choices"][0]["message"]["content"].strip()
