from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

class Base(DeclarativeBase):
    pass

def make_engine(database_url: str, connect_timeout: int = 2) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, pool_timeout=connect_timeout, connect_args={"connect_timeout": connect_timeout})

def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
