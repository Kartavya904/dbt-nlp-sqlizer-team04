from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # existing...
    DATABASE_URL: str = "sqlite:///./demo.db"
    ALLOWED_ORIGIN: str = "http://localhost:5173"

    # LLM (generic “OpenAI-compatible” chat endpoint)
    LLM_BASE_URL: str | None = None   # e.g., "http://localhost:11434/v1" or "https://api.your-llm.com/v1"
    LLM_MODEL: str | None = None      # e.g., "qwen2.5-coder", "meta-llama-3.1-8b-instruct"
    LLM_API_KEY: str | None = None    # if your endpoint needs it

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
