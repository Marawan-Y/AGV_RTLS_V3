"""Advanced database connection manager with pooling and retry logic."""

import os
import time
import asyncio
from typing import Optional, Dict, Any, List, Generator
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime, timedelta

import pymysql
import aiomysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, text, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from dbutils.pooled_db import PooledDB
from loguru import logger
import pandas as pd

Base = declarative_base()

class DatabaseManager:
    """Production-grade database manager with connection pooling."""
    
    def __init__(self):
        self.config = self._load_config()
        self.sync_pool = self._create_sync_pool()
        self.async_pool = None
        self.engine = self._create_engine()
        self.SessionLocal = sessionmaker(bind=self.engine)
        self._init_checks()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load database configuration from environment."""
        return {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER', 'agv'),
            'password': os.getenv('MYSQL_PASSWORD', 'agvpass'),
            'database': os.getenv('MYSQL_DATABASE', 'agv_rtls'),
            'charset': 'utf8mb4',
            'connect_timeout': 10,
            'autocommit': False
        }
    
    def _create_sync_pool(self) -> PooledDB:
        """Create synchronous connection pool."""
        return PooledDB(
            creator=pymysql,
            maxconnections=50,
            mincached=5,
            maxcached=20,
            blocking=True,
            maxusage=None,
            setsession=['SET time_zone = "+00:00"'],
            cursorclass=DictCursor,
            **self.config
        )
    
    def _create_engine(self):
        """Create SQLAlchemy engine with advanced pooling."""
        url = (f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
               f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
               f"?charset={self.config['charset']}")
        
        return create_engine(
            url,
            poolclass=pool.QueuePool,
            pool_size=20,
            max_overflow=30,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=False
        )
    
    async def _create_async_pool(self):
        """Create asynchronous connection pool."""
        if not self.async_pool:
            self.async_pool = await aiomysql.create_pool(
                minsize=5,
                maxsize=20,
                echo=False,
                **self.config
            )
        return self.async_pool
    
    def _init_checks(self):
        """Perform initial database checks."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT VERSION()")
                    version = cursor.fetchone()
                    logger.info(f"Connected to MySQL {version['VERSION()']}")
                    
                    # Check table existence
                    cursor.execute("""
                        SELECT COUNT(*) as count 
                        FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = 'agv_positions'
                    """, (self.config['database'],))
                    
                    if cursor.fetchone()['count'] == 0:
                        logger.warning("Required tables not found. Run schema.sql first.")
        except Exception as e:
            logger.error(f"Database initialization check failed: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool."""
        conn = self.sync_pool.connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    @asynccontextmanager
    async def get_async_connection(self):
        """Get an async connection from the pool."""
        pool = await self._create_async_pool()
        async with pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                try:
                    yield cursor
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    logger.error(f"Async database error: {e}")
                    raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get SQLAlchemy session."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    
    def execute_many(self, query: str, data: List[tuple], batch_size: int = 1000):
        """Execute bulk insert with batching."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    cursor.executemany(query, batch)
                    conn.commit()
                    logger.debug(f"Inserted batch {i//batch_size + 1}")
    
    def query_dataframe(self, query, params= None) -> pd.DataFrame:
        """Execute query and return as pandas DataFrame."""
        if isinstance(params, list):
            params = tuple(params)
        with self.get_connection() as conn:
            return pd.read_sql_query(sql=query, con=self.engine, params=params)
    
    async def insert_position(self, data: Dict) -> int:
        """Insert a single position record asynchronously."""
        query = """
            INSERT INTO agv_positions (
                ts, agv_id, lat, lon, heading_deg, speed_mps, 
                quality, plant_x, plant_y, zone_id, battery_percent, status
            ) VALUES (
                %(ts)s, %(agv_id)s, %(lat)s, %(lon)s, %(heading_deg)s, %(speed_mps)s,
                %(quality)s, %(plant_x)s, %(plant_y)s, %(zone_id)s, %(battery_percent)s, %(status)s
            )
        """
        
        async with self.get_async_connection() as cursor:
            await cursor.execute(query, data)
            return cursor.lastrowid
    
    def get_agv_trajectory(self, agv_id: str, start: datetime, end: datetime, 
                          downsample: int = 1) -> pd.DataFrame:
        """Get AGV trajectory with optional downsampling."""
        query = """
            SELECT ts, plant_x, plant_y, heading_deg, speed_mps, zone_id
            FROM agv_positions
            WHERE agv_id = :agv_id 
            AND ts BETWEEN :start AND :end
            ORDER BY ts
        """
        
        df = self.query_dataframe(query, {
            'agv_id': agv_id,
            'start': start,
            'end': end
        })
        
        if downsample > 1 and len(df) > downsample:
            return df.iloc[::downsample]
        return df
    
    def get_fleet_positions(self, time_window: timedelta = timedelta(seconds=10)) -> pd.DataFrame:
        """Get current fleet positions."""
        query = """
            SELECT DISTINCT 
                p1.agv_id, p1.ts, p1.plant_x, p1.plant_y, 
                p1.heading_deg, p1.speed_mps, p1.zone_id,
                p1.battery_percent, p1.status,
                r.display_name, r.type, r.assigned_category
            FROM agv_positions p1
            INNER JOIN (
                SELECT agv_id, MAX(ts) as max_ts
                FROM agv_positions
                WHERE ts >= NOW() - INTERVAL :seconds SECOND
                GROUP BY agv_id
            ) p2 ON p1.agv_id = p2.agv_id AND p1.ts = p2.max_ts
            LEFT JOIN agv_registry r ON p1.agv_id = r.agv_id
            ORDER BY p1.agv_id
        """
        
        return self.query_dataframe(query, {'seconds': int(time_window.total_seconds())})
    
    def cleanup_old_data(self, days_to_keep: int = 90):
        """Clean up old data with partition management."""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                # Archive old data if needed
                cursor.execute("""
                    INSERT INTO agv_positions_archive 
                    SELECT * FROM agv_positions 
                    WHERE ts < %s
                """, (cutoff_date,))
                
                archived = cursor.rowcount
                
                # Delete old data
                cursor.execute("""
                    DELETE FROM agv_positions 
                    WHERE ts < %s
                    LIMIT 10000
                """, (cutoff_date,))
                
                deleted = cursor.rowcount
                
                logger.info(f"Archived {archived} and deleted {deleted} old records")
    
    def __del__(self):
        """Cleanup on deletion."""
        if hasattr(self, 'engine'):
            self.engine.dispose()
        if hasattr(self, 'async_pool') and self.async_pool:
            self.async_pool.close()

# Singleton instance
db_manager = DatabaseManager()