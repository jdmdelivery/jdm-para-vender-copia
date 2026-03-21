# -*- coding: utf-8 -*-
"""
Configuración de base de datos PostgreSQL para JDM Cash Now.
"""
import os
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

from .models import Base

DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or "postgresql://localhost/jdm_cash"
if str(DATABASE_URL).lower() in ("", "null", "none"):
    DATABASE_URL = "postgresql://localhost/jdm_cash"

# Render usa postgres://; SQLAlchemy requiere postgresql://
if DATABASE_URL and str(DATABASE_URL).startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=bool(os.getenv("SQL_ECHO")),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_factory = scoped_session(SessionLocal)


def get_session():
    return session_factory()


@contextmanager
def session_scope():
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db(drop_all=False):
    """Crea todas las tablas. Si drop_all=True, las borra primero."""
    if drop_all:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def init_app(app):
    """Registra teardown para cerrar sesiones y hacer commit."""
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        try:
            if exception is None:
                session_factory().commit()
        except Exception:
            session_factory().rollback()
        finally:
            session_factory.remove()
