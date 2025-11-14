from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .settings import settings

def make_engine(url: str | None = None) -> Engine:
    url = url or settings.DATABASE_URL
    kwargs = {}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)

engine = make_engine()
