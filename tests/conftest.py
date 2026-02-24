from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db.base import Base
import app.db.models  # noqa: F401 – register models
from app.storage.file_store import FileStore


@pytest.fixture()
def tmp_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture()
def db_session(tmp_dir: Path) -> Session:
    """In-memory SQLite session with all tables created."""
    db_path = tmp_dir / "test.db"
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = TestSession()
    yield session
    session.close()
    engine.dispose()


@pytest.fixture()
def file_store(tmp_dir: Path) -> FileStore:
    """FileStore backed by a temporary directory."""
    store = FileStore(base_dir=tmp_dir / "data_store")
    return store
