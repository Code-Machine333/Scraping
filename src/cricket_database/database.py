"""Database connection and session management."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session

from .config import settings


# Global engine instance
_engine: Engine | None = None
_SessionLocal: sessionmaker | None = None


def get_database_engine() -> Engine:
    """Get or create the database engine."""
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.database.url,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,  # Set to True for SQL debugging
        )
    return _engine


def get_session_local() -> sessionmaker:
    """Get or create the session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=get_database_engine()
        )
    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Get a database session with automatic cleanup."""
    session_local = get_session_local()
    session = session_local()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_tables() -> None:
    """Create all database tables."""
    from .models import Base
    Base.metadata.create_all(bind=get_database_engine())


def drop_tables() -> None:
    """Drop all database tables."""
    from .models import Base
    Base.metadata.drop_all(bind=get_database_engine())
