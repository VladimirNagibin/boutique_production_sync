import asyncio
import sqlite3

from pathlib import Path
from typing import Any

import pandas as pd

from pandas import DataFrame

from core.logger import logger
from core.settings import settings
from db.factory import AsyncDatabaseFactory
from interfaces.db.base import IDatabaseManager


class SupplierCodesRepo:
    def __init__(self, db_path: str = str(settings.DB_SQLITE_PATH)) -> None:
        self.db_path = db_path
        self._db_manager: IDatabaseManager | None = None

    async def _get_db_manager(self) -> IDatabaseManager:
        """Lazy initialization of database manager."""
        if self._db_manager is None:
            self._db_manager = await AsyncDatabaseFactory.get_manager()
        return self._db_manager

    async def load_data(
        self, file_path: str | Path, table_name: str = "supplier_product_codes"
    ) -> dict[str, Any]:
        """
        Загружает данные из CSV файла в таблицу базы данных.

        Args:
            file_path: Путь к CSV файлу
            table_name: Имя целевой таблицы

        Returns:
            Dict с результатами загрузки

        Raises:
            FileNotFoundError: Если файл не существует
            ValueError: Если файл пуст или имеет неверный формат
            RuntimeError: Если произошла ошибка при загрузке данных
        """
        csv_path = Path(file_path)

        logger.info(
            "Начало загрузки данных из CSV",
            extra={
                "file_path": str(csv_path),
                "table_name": table_name,
                "db_path": str(self.db_path),
            },
        )

        # Проверка существования файла
        if not csv_path.exists():
            error_msg = f"CSV файл не найден: {csv_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        if not csv_path.is_file():
            error_msg = f"Путь не является файлом: {csv_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Запускаем тяжелую операцию в отдельном потоке
        try:
            # Запускаем синхронную операцию в отдельном потоке
            result = await asyncio.to_thread(
                self._sync_load_operation, csv_path, table_name
            )

            logger.info(
                "Данные успешно загружены",
                extra={
                    "file_path": str(csv_path),
                    "table_name": table_name,
                    # "rows_loaded": result["rows_loaded"],
                    "processing_time_ms": result.get("processing_time_ms", 0),
                    "columns_loaded": result.get("columns_loaded", []),
                },
            )
        except ValueError as e:
            # Ошибки валидации данных (пустой файл и т.д.)
            logger.warning(f"Ошибка валидации данных: {e}")
            raise  # Пробрасываем дальше

        except pd.errors.EmptyDataError as e:
            error_msg = f"CSV файл пуст или содержит только заголовки: {csv_path}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg) from e

        except pd.errors.ParserError as e:
            error_msg = f"Ошибка парсинга CSV файла {csv_path}: {e}"
            logger.error(error_msg, exc_info=True)
            raise ValueError(error_msg) from e

        except sqlite3.Error as e:
            error_msg = f"Ошибка базы данных при загрузке {csv_path}: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

        except Exception as e:
            error_msg = f"Неожиданная ошибка при загрузке {csv_path}: {e}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e
        else:
            return result

    def _sync_load_operation(self, csv_path: Path, table_name: str) -> dict[str, Any]:
        """
        Синхронная операция загрузки данных.

        Args:
            csv_path: Путь к CSV файлу
            table_name: Имя целевой таблицы

        Returns:
            Dict с результатами загрузки
        """
        # import time
        # start_time = time.time()

        logger.debug(
            "Начало синхронной загрузки данных",
            extra={"file_path": str(csv_path), "table_name": table_name},
        )
        conn = sqlite3.connect(self.db_path)

        try:
            logger.info(f"Starting CSV load from {csv_path} to table {table_name}")

            # Чтение CSV
            df: DataFrame = pd.read_csv(csv_path, sep=";", escapechar="\\")
            logger.info(f"Прочитано {len(df)} строк из файла.")
            # Проверка на пустой DataFrame
            self._validate_data_frame(df)

            # Запись в SQL
            df.to_sql(
                table_name,
                conn,
                if_exists="replace",  # append replace
                index=False,
                chunksize=10000,
                method="multi",
            )

            logger.info(f"Успешная запись в таблицу '{table_name}' ({len(df)} строк).")

            return {"status": "success", "rows_inserted": len(df), "table": table_name}
        except pd.errors.EmptyDataError as e:
            logger.warning(f"File {csv_path} is empty.")
            error_message = "Файл пуст или содержит только заголовки."
            raise ValueError(error_message) from e
        except Exception as e:
            logger.error(f"Error during Pandas processing: {e}")
            raise
        finally:
            conn.close()
            logger.debug("Соединение с SQLite закрыто.")

    def _validate_data_frame(self, df: DataFrame) -> None:
        if df.empty:
            error_message = (
                "Файл успешно прочитан, но не содержит данных (пустой DataFrame)."
            )
            raise ValueError(error_message)


def get_supplier_codes_repo() -> SupplierCodesRepo:
    return SupplierCodesRepo()
