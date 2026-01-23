"""Async SQLite database manager implementation."""

import asyncio
import sqlite3

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import aiosqlite

from core.logger import logger
from core.settings import settings
from interfaces.db.base import IDatabaseManager, ITransactionManager


class SQLiteTransactionManager(ITransactionManager):
    """Async SQLite transaction manager."""

    def __init__(self, connection: aiosqlite.Connection):
        self.connection = connection

    async def begin_transaction(self) -> None:
        """Begin a new transaction asynchronously."""
        await self.connection.execute("BEGIN TRANSACTION")

    async def commit(self) -> None:
        """Commit the current transaction asynchronously."""
        await self.connection.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction asynchronously."""
        await self.connection.rollback()

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        try:
            await self.begin_transaction()
            yield
            await self.commit()
        except Exception:
            await self.rollback()
            raise


class SQLiteManager(IDatabaseManager):
    """Async SQLite database manager."""

    def __init__(self, db_path: str = str(settings.DB_SQLITE_PATH), **kwargs: Any):
        self.db_path = db_path
        self.pool_size = kwargs.get("pool_size", settings.POOL_SIZE)
        self._connection_pool = None

    async def initialize(self) -> None:
        """Initialize database and connection pool."""
        await self._init_database()

    @asynccontextmanager
    async def get_connection(self) -> AsyncIterator[aiosqlite.Connection]:
        """Async context manager for database connections."""
        # Используем пул соединений для лучшей производительности
        logger.info(f"Initializing SQLite database at {self.db_path}")
        conn = await aiosqlite.connect(
            str(self.db_path),
            # detect_types=sqlite3.PARSE_DECLTYPES,
        )
        conn.row_factory = aiosqlite.Row

        try:
            yield conn
        finally:
            await conn.close()

    async def _init_database(self) -> None:
        """Initialize database tables asynchronously."""
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)

        async with self.get_connection() as conn:
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA foreign_keys=ON")
            await conn.execute("PRAGMA synchronous=NORMAL")

            # Таблицы
            sql_script = """
            -- Таблица для кодов товаров поставщиков
            CREATE TABLE IF NOT EXISTS supplier_product_codes (
                -- Первичный ключ (автоинкремент)
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                -- Код товара у поставщика
                code INTEGER NOT NULL CHECK (code > 0),

                -- Наименование товара поставщика
                name TEXT NOT NULL CHECK (LENGTH(TRIM(name)) > 0),

                -- Группа товаров
                product_group TEXT,

                -- Подгруппа товаров
                subgroup TEXT,

                -- Идентификатор поставщика
                supplier_id INTEGER NOT NULL,

                -- Уникальность связки (код поставщика + код товара)
                CONSTRAINT unique_supplier_code UNIQUE (code, supplier_id)
            );

            -- Индекс для быстрого поиска по поставщику и коду
            CREATE INDEX IF NOT EXISTS idx_supplier_code
            ON supplier_product_codes (supplier_id, code);

            -- Индекс для поиска по названию товара
            CREATE INDEX IF NOT EXISTS idx_product_name
            ON supplier_product_codes (name);

            """

            await conn.executescript(sql_script)
            await conn.commit()
            logger.info("SQLite database initialized asynchronously")

    async def execute_query(
        self, query: str, params: tuple[Any] | None = None
    ) -> list[Any]:
        """Execute raw SQL query asynchronously."""
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params or ())
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]

    async def execute_many(self, query: str, params_list: list[Any]) -> None:
        """Execute many SQL statements asynchronously."""
        async with self.get_connection() as conn:
            await conn.executemany(query, params_list)
            await conn.commit()

    async def backup(self, backup_path: str | None = None) -> bool:
        """Create database backup using native SQLite API (thread-safe)."""
        backup_path = backup_path or f"{self.db_path}.backup"

        try:

            def _run_backup() -> None:
                src = sqlite3.connect(self.db_path)
                dst = sqlite3.connect(backup_path)
                src.backup(dst)
                dst.close()
                src.close()

            await asyncio.to_thread(_run_backup)
        except (OSError, sqlite3.Error) as e:
            logger.error(f"Backup failed: {e}")
            with suppress(OSError):
                Path(backup_path).unlink()
            return False
        else:
            logger.info(f"SQLite database backed up to {backup_path}")
            return True

    async def health_check(self) -> bool:
        """Check database health asynchronously."""
        try:
            async with self.get_connection() as conn:
                cursor = await conn.execute("SELECT 1")
                await cursor.fetchone()
                await cursor.close()
                return True
        except sqlite3.Error as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def close(self) -> None:
        """Close all connections asynchronously."""
        # Для SQLite aiosqlite соединения закрываются автоматически

    def create_transaction_manager(
        self, connection: aiosqlite.Connection
    ) -> SQLiteTransactionManager:
        """Create transaction manager for a connection."""
        return SQLiteTransactionManager(connection)
