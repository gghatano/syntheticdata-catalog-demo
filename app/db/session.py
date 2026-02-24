from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import DB_DIR, DB_URL

DB_DIR.mkdir(parents=True, exist_ok=True)

engine = create_engine(DB_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    """FastAPI Depends 用セッションジェネレータ."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
