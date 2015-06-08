import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from .app_config import DB_CONN

engine = create_engine(DB_CONN, 
                       convert_unicode=True, 
                       server_side_cursors=True)

db_session = scoped_session(sessionmaker(bind=engine,
                                         autocommit=False,
                                         autoflush=False))

Base = declarative_base()

def init_db(sess=None, eng=None):
    import sunshine.models
    Base.metadata.create_all(bind=engine)

