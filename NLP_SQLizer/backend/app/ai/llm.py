# app/ai/llm.py
from __future__ import annotations
import httpx
from ..settings import settings

class LLMNotConfigured(Exception):
    pass

def _client(timeout: float = 60.0) -> httpx.Client:
    """
    Create HTTP client for LLM service.
    
    Args:
        timeout: Request timeout in seconds (default 120 for larger models)
    """
    if not settings.LLM_BASE_URL or not settings.LLM_MODEL:
        raise LLMNotConfigured("LLM not configured. Set LLM_BASE_URL and LLM_MODEL in .env")
    headers = {"Content-Type": "application/json"}
    if settings.LLM_API_KEY:
        headers["Authorization"] = f"Bearer {settings.LLM_API_KEY}"
    return httpx.Client(base_url=settings.LLM_BASE_URL, headers=headers, timeout=timeout)

def chat_complete(system: str, user: str, timeout: float = 60.0, max_tokens: int = 256) -> str:
    """
    Minimal chat-completions call against any OpenAI-compatible endpoint.
    Returns assistant text. Raise if not configured.
    
    Args:
        system: System prompt
        user: User prompt
        timeout: Request timeout in seconds (default 120 for larger models)
        max_tokens: Maximum tokens to generate (default 512)
    """
    import httpx
    
    try:
        with _client(timeout=timeout) as c:
            data = {
                "model": settings.LLM_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
            }
            r = c.post("/chat/completions", json=data)
            r.raise_for_status()
            j = r.json()
            return j["choices"][0]["message"]["content"].strip()
    except httpx.TimeoutException as e:
        raise LLMNotConfigured(
            f"LLM request timed out after {timeout} seconds. "
            f"The model ({settings.LLM_MODEL}) may be too slow or the request too complex. "
            f"Try increasing the timeout or using a faster model."
        )
    except httpx.ConnectError as e:
        raise LLMNotConfigured(
            f"Could not connect to LLM service at {settings.LLM_BASE_URL}. "
            f"Please ensure the LLM service is running and LLM_BASE_URL is correct in your .env file."
        )
    except httpx.HTTPStatusError as e:
        raise LLMNotConfigured(
            f"LLM service returned error {e.response.status_code}: {e.response.text}. "
            f"Please check your LLM configuration."
        )
    except httpx.ReadTimeout as e:
        raise LLMNotConfigured(
            f"LLM read timeout after {timeout} seconds. "
            f"The model ({settings.LLM_MODEL}) is taking too long to respond. "
            f"Try increasing the timeout or using a faster model."
        )
