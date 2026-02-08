from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime

import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./data/open_interest.db")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class OIFunding(Base):
    __tablename__ = "oi_funding"

    id = Column(Integer, primary_key=True, index=True)
    pair = Column(String, index=True)
    timestamp = Column(DateTime, index=True)  # UTC timestamp
    open_interest = Column(Float)
    funding_rate = Column(Float)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
