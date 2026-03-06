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
    mark_price = Column(Float)
    taker_buy_volume = Column(Float)
    taker_sell_volume = Column(Float)
    cvd = Column(Float)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Automatically add new columns if they are missing (e.g. for existing deployments)
    from sqlalchemy import text
    async with engine.begin() as conn:
        columns_to_add = [
            ("mark_price", "FLOAT"),
            ("taker_buy_volume", "FLOAT"),
            ("taker_sell_volume", "FLOAT"),
            ("cvd", "FLOAT")
        ]
        for col_name, col_type in columns_to_add:
            try:
                await conn.execute(text(f"ALTER TABLE oi_funding ADD COLUMN {col_name} {col_type}"))
            except Exception:
                # Ignore errors, typically means column already exists
                pass
