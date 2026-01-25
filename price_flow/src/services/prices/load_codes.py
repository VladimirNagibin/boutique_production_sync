import asyncio
import shutil
import uuid
import zipfile

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import aiofiles  # type: ignore[import-untyped]
import aiofiles.os as aios  # type: ignore[import-untyped]

from fastapi import Depends, UploadFile

from core.exceptions import (
    DatabaseAppError,
    DataProcessingError,
    FileAppNotFoundError,
    FileNotZipError,
    FileSizeError,
    FileUploadError,
    ZipExtractionError,
)
from core.logger import logger
from core.settings import settings
from db.factory import AsyncDatabaseFactory
from interfaces.db.base import IDatabaseManager
from repositories.supplier_codes_repo import SupplierCodesRepo, get_supplier_codes_repo
from schemas.response_schemas import SuccessResponse
from services.helpers import extract_zip


UPLOAD_DIR = "uploads"  # Директория для загрузки файлов
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB максимальный размер файла


class LoaderCodes:
    ERR_MSG_NOT_ZIP = "Файл должен быть в формате ZIP"
    ERR_MSG_INVALID_ZIP = "Файл не является валидным ZIP архивом"
    ERR_MSG_SIZE_LIMIT = "Размер файла превышает лимит"
    ERR_MSG_SAVE_FAILED = "Ошибка при сохранении файла"
    ERR_MSG_VALIDATION_FAILED = "Ошибка при проверке ZIP архива"
    ERR_MSG_CSV_NOT_FOUND = "CSV файл не найден внутри архива"
    ERR_MSG_UNZIP_FAILED = "Не удалось распаковать архив"

    def __init__(
        self,
        supplier_codes_repo: SupplierCodesRepo,
        upload_dir: str | None = None,
        max_file_size: int | None = None,
    ) -> None:
        self.supplier_codes_repo = supplier_codes_repo
        self._db_manager: IDatabaseManager | None = None
        self._db_manager_lock = asyncio.Lock()
        self.upload_dir = settings.BASE_DIR / Path(upload_dir or UPLOAD_DIR)
        self.max_file_size = int(max_file_size or MAX_FILE_SIZE)

        # Создаем директорию для загрузок при инициализации
        self.upload_dir.mkdir(parents=True, exist_ok=True)

    async def _get_db_manager(self) -> IDatabaseManager:
        """
        Ленивая инициализация менеджера БД.

        Returns:
            Менеджер БД

        Raises:
            DatabaseError: Если не удалось инициализировать менеджер
        """
        if self._db_manager is None:
            async with self._db_manager_lock:
                if self._db_manager is None:  # Double-check locking
                    try:
                        self._db_manager = await AsyncDatabaseFactory.get_manager()
                        logger.debug("Менеджер БД успешно инициализирован")
                    except Exception as e:
                        logger.error(
                            "Ошибка инициализации менеджера БД",
                            extra={"error": str(e)},
                            exc_info=True,
                        )
                        raise DatabaseAppError(
                            message=(
                                "Не удалось инициализировать менеджер базы данных: "
                                f"{e!s}"
                            )
                        ) from e
        return self._db_manager

    async def cleanup(self) -> None:
        """Очистка ресурсов."""
        if self._db_manager:
            try:
                await self._db_manager.close()
                logger.debug("Менеджер БД закрыт")

            except asyncio.CancelledError:
                # Обработка отмены задачи
                logger.warning("Очистка ресурсов была отменена")
                raise  # Пробрасываем дальше для правильной обработки отмены

            except ConnectionError as e:
                # Ошибки соединения с БД
                logger.error(f"Ошибка соединения при закрытии менеджера БД: {e}")

            except TimeoutError as e:
                # Таймаут при закрытии соединения
                logger.warning(f"Таймаут при закрытии менеджера БД: {e}")

            except RuntimeError as e:
                # Ошибки выполнения (например, уже закрыто)
                if "closed" in str(e).lower() or "not connected" in str(e).lower():
                    logger.debug(f"Менеджер БД уже закрыт: {e}")
                else:
                    logger.error(f"Ошибка выполнения при закрытии менеджера БД: {e}")

            except AttributeError as e:
                # Если у менеджера нет метода close()
                logger.error(f"Менеджер БД не поддерживает метод close(): {e}")

            except Exception as e:  # noqa: BLE001
                # Обработка любых других неожиданных ошибок с логированием
                logger.error(
                    f"Неожиданная ошибка при очистке ресурсов: {e}", exc_info=True
                )

            finally:
                # Всегда сбрасываем ссылку на менеджер
                self._db_manager = None
                logger.debug("Ссылка на менеджер БД сброшена")

    async def load_file(self, file: UploadFile) -> SuccessResponse:
        """
        Орхестратор процесса:
        Загрузка -> Распаковка -> Парсинг -> Загрузка в БД -> Очистка.
        """
        zip_file_path: Path | None = None
        extract_dir: Path | None = None

        try:
            # 1. Загрузка ZIP файла
            upload_response = await self.upload_file(file)
            self._validate_upload_response(upload_response)
            zip_file_path = Path(upload_response.details["file_path"])  # type: ignore

            # 2. Подготовка директории для распаковки
            # Создаем временную папку с UUID, чтобы избежать конфликтов
            extract_subdir = f"{zip_file_path.stem}_extracted_{uuid.uuid4().hex[:8]}"
            extract_dir = zip_file_path.parent / extract_subdir
            extract_dir.mkdir(exist_ok=True)

            # 3. Асинхронная распаковка
            await self._unzip_file_async(zip_file_path, extract_dir)

            # 4. Поиск CSV файла
            csv_files = list(extract_dir.glob("*.csv"))
            txt_files = list(extract_dir.glob("*.txt"))
            all_files = csv_files + txt_files
            self._validate_file_found(all_files)

            # Берем первый найденный CSV
            csv_file_path = all_files[0]

            # 5. Загрузка в БД
            db_result = await self.load_file_to_db(str(csv_file_path))

            return SuccessResponse(
                message="Данные успешно обработаны", details=db_result
            )

        except (
            FileNotZipError,
            FileSizeError,
            ZipExtractionError,
            FileUploadError,
            FileAppNotFoundError,
            DataProcessingError,
        ) as e:
            # Ошибки бизнес-логики, логируем warning и пробрасываем
            logger.warning(f"Ошибка обработки файла: {e}")
            raise

        except Exception as e:
            # Непредвиденные ошибки
            logger.exception(f"Критическая ошибка в load_file: {e}")
            error_message = "Внутренняя ошибка при обработке файла"
            raise RuntimeError(error_message) from e

        finally:
            # 6. Гарантированная очистка ресурсов
            if zip_file_path and zip_file_path.exists():
                await remove_file_async(zip_file_path)

            if extract_dir and extract_dir.exists():
                await remove_directory_async(extract_dir)

    def _validate_upload_response(self, upload_response: SuccessResponse) -> None:
        if not upload_response.details:
            error_message = "File not uploaded"
            raise FileUploadError(error_message)

    def _validate_file_found(self, files: list[Path]) -> None:
        if not files:
            raise DataProcessingError(self.ERR_MSG_CSV_NOT_FOUND)

    async def upload_file(
        self, file: UploadFile, save_subpath: str | None = None
    ) -> SuccessResponse:
        """
        Асинхронно загружает ZIP файл и сохраняет его локально

        Args:
            file: Загружаемый ZIP файл
            save_subpath: Опциональный подпуть для сохранения файла

        Returns:
            SuccessResponse с информацией о сохраненном файле

        Raises:
            FileNotZipError: Если файл не является ZIP архивом
            FileSizeError: Если размер файла превышает лимит
            ZipExtractionError: Если не удалось проверить ZIP архив
            Exception: При других ошибках сохранения файла
        """

        # Проверяем, что файл имеет расширение .zip
        self._validate_file_extension(file)
        save_dir = self._get_save_directory(save_subpath)

        original_name = file.filename or "unknown_upload.zip"

        logger.info(f"Начало загрузки файла: {original_name}")
        # Генерируем уникальное имя файла чтобы избежать перезаписи
        unique_filename = self._generate_unique_filename(original_name)
        file_path = save_dir / unique_filename

        try:
            # Асинхронно сохраняем файл с проверкой размера
            file_size = await self._save_file_with_size_check(
                file=file, file_path=file_path
            )

            # Проверяем валидность ZIP архива
            zip_info = await self._validate_zip_file(file_path)

            # Получаем информацию о файле
            file_info = self._build_file_info(
                original_filename=original_name,
                saved_filename=unique_filename,
                file_path=file_path,
                file_size=file_size,
                zip_info=zip_info,
            )

            logger.info(
                f"Файл успешно сохранен: {file_path} (Размер: {file_size} байт)"
            )

            return SuccessResponse(
                message="ZIP файл успешно сохранен", details=file_info
            )

        except (FileSizeError, ZipExtractionError, FileNotZipError) as e:
            # Ожидаемые ошибки бизнес-логики
            logger.warning(f"Ошибка валидации файла {original_name}: {e}")
            await self._safe_remove_file(file_path)
            raise
        except Exception as e:
            # Непредвиденные системные ошибки
            logger.exception(
                f"Критическая ошибка при сохранении файла {original_name}: {e}"
            )
            await self._safe_remove_file(file_path)
            # Создаем более конкретное исключение вместо голого Exception (TRY002)
            raise RuntimeError(self.ERR_MSG_SAVE_FAILED) from e

    def _validate_file_extension(self, file: UploadFile) -> None:
        """
        Проверяет, что файл имеет расширение .zip

        Args:
            file: Проверяемый файл

        Raises:
            FileNotZipError: Если файл не является ZIP архивом
        """
        if not file.filename or not file.filename.lower().endswith(".zip"):
            logger.warning(
                f"Попытка загрузки файла с неверным расширением: {file.filename}"
            )
            raise FileNotZipError(
                file.filename if file.filename else "empty_filename",
                self.ERR_MSG_NOT_ZIP,
            )

    def _get_save_directory(self, save_subpath: str | None = None) -> Path:
        """
        Создает и возвращает безопасный путь для сохранения файла

        Args:
            save_subpath: Опциональный подпуть

        Returns:
            Path: Директория для сохранения
        """
        if save_subpath:
            # Очищаем путь от потенциально опасных символов
            safe_subpath = Path(save_subpath).name
            save_dir = self.upload_dir / safe_subpath
        else:
            save_dir = self.upload_dir

        # Создаем директорию если ее нет
        save_dir.mkdir(parents=True, exist_ok=True)
        return save_dir

    def _generate_unique_filename(self, original_filename: str) -> str:
        """
        Генерирует уникальное имя файла с timestamp и UUID

        Args:
            original_filename: Оригинальное имя файла

        Returns:
            str: Уникальное имя файла
        """
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        uuid_part = uuid.uuid4().hex[:8]

        # Очищаем оригинальное имя файла от потенциально опасных символов
        safe_filename = Path(original_filename).name

        return f"{timestamp}_{uuid_part}_{safe_filename}"

    async def _save_file_with_size_check(
        self, file: UploadFile, file_path: Path
    ) -> int:
        """
        Асинхронно сохраняет файл с проверкой размера

        Args:
            file: Загружаемый файл
            file_path: Путь для сохранения

        Returns:
            int: Размер сохраненного файла в байтах

        Raises:
            FileSizeError: Если размер файла превышает лимит
        """
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB

        try:
            async with aiofiles.open(file_path, "wb") as buffer:
                while chunk := await file.read(chunk_size):
                    file_size += len(chunk)

                    self._ensure_size_limit_not_exceeded(file_path, file_size)

                    await buffer.write(chunk)

        except FileSizeError:
            # Пробрасываем дальше, чтобы удалить файл в блоке выше
            raise
        except OSError as e:
            # Ошибки диска (нет места, права доступа)
            logger.error(f"Ошибка записи на диск: {e}")
            raise RuntimeError(self.ERR_MSG_SAVE_FAILED) from e

        return file_size

    def _ensure_size_limit_not_exceeded(
        self, file_path: Path, current_size: int
    ) -> None:
        """
        Проверяет размер файла. Выбрасывает исключение, если лимит превышен.
        Этот метод создан для соблюдения принципа 'Abstract raise to inner function'.
        """
        if current_size > self.max_file_size:
            limit_mb = self.max_file_size / (1024 * 1024)
            logger.warning(
                f"Превышен лимит размера файла: {current_size} > {self.max_file_size}"
            )
            error_message = f"{self.ERR_MSG_SIZE_LIMIT} ({limit_mb:.2f}MB)"
            raise FileSizeError(
                file_path, error_message, max_file_size=self.max_file_size
            )

    async def _validate_zip_file(self, file_path: Path) -> dict[str, Any]:
        """
        Проверяет, является ли файл валидным ZIP архивом

        Args:
            file_path: Путь к файлу для проверки

        Returns:
            dict: Информация о содержимом ZIP архива

        Raises:
            ZipExtractionError: Если файл не является валидным ZIP архивом
        """

        def _extract_zip_info() -> dict[str, Any]:
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                file_list = zip_ref.namelist()
                return {
                    "total_files": len(file_list),
                    "files": file_list[:10],  # Ограничиваем вывод
                    "is_valid": True,
                    "compressed_size": file_path.stat().st_size,
                    "comment": (
                        zip_ref.comment.decode("utf-8", errors="ignore")
                        if zip_ref.comment
                        else None
                    ),
                }

        try:
            return await asyncio.to_thread(_extract_zip_info)

        except zipfile.BadZipFile as e:
            logger.warning(f"Невалидный ZIP архив: {file_path.name}")
            raise ZipExtractionError(file_path, self.ERR_MSG_INVALID_ZIP) from e
        except OSError as e:
            # Например, файл был удален между сохранением и проверкой
            logger.error(f"Ошибка доступа к файлу при валидации: {e}")
            raise ZipExtractionError(file_path, self.ERR_MSG_VALIDATION_FAILED) from e

    def _build_file_info(
        self,
        original_filename: str,
        saved_filename: str,
        file_path: Path,
        file_size: int,
        zip_info: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Собирает информацию о сохраненном файле

        Args:
            original_filename: Оригинальное имя файла
            saved_filename: Сохраненное имя файла
            file_path: Полный путь к файлу
            file_size: Размер файла в байтах
            zip_info: Информация о ZIP архиве

        Returns:
            dict: Структурированная информация о файле
        """
        return {
            "original_filename": original_filename,
            "saved_filename": saved_filename,
            "file_path": str(file_path),
            "file_size": file_size,
            "size_mb": round(file_size / (1024 * 1024), 2),
            "saved_at": datetime.now(UTC).isoformat(),
            "zip_info": zip_info,
        }

    async def _safe_remove_file(self, file_path: Path) -> None:
        """
        Безопасно удаляет файл, если он существует

        Args:
            file_path: Путь к файлу для удаления
        """
        if file_path.exists():
            try:
                # unlink() - это Path.remove(), но может блокировать I/O,
                # поэтому оборачиваем в to_thread для чистоты асинхронности
                await asyncio.to_thread(file_path.unlink)
                logger.debug(f"Временный файл удален: {file_path}")
            except OSError as e:
                logger.error(f"Не удалось удалить файл {file_path}: {e}")

    async def _unzip_file_async(self, zip_path: Path, extract_to: Path) -> None:
        """
        Запускает распаковку в отдельном потоке.
        """

        def _unzip_task() -> bool:
            return extract_zip(str(zip_path), str(extract_to))

        try:
            await asyncio.to_thread(_unzip_task)
            logger.info(f"Файл распакован в: {extract_to}")

        except (FileAppNotFoundError, ZipExtractionError):
            # Если это наши кастомные исключения — просто пробрасываем их выше
            raise

        except Exception as e:
            logger.error(
                f"Ошибка распаковки архива {zip_path.name}: {e}", exc_info=True
            )
            error_message = f"Ошибка при распаковке архива: {zip_path.name}"
            raise ZipExtractionError(zip_path, error_message) from e

    async def load_file_to_db(self, unpacked_file_path: str) -> dict[str, Any]:
        return await self.supplier_codes_repo.load_data(
            unpacked_file_path, "supplier_product_codes"
        )


async def remove_file_async(file_path: str | Path) -> bool:
    """
    Асинхронно удаляет файл.
    Возвращает True, если удаление прошло успешно.
    """
    path = Path(file_path)

    if not path.exists():
        logger.debug(f"Файл не найден, пропускаем удаление: {path}")
        return False

    try:
        await aios.remove(path)
        logger.info(f"Файл успешно удален: {path}")
    except OSError as e:
        logger.error(f"Ошибка при удалении файла {path}: {e}")
        return False
    else:
        return True


async def remove_directory_async(dir_path: str | Path) -> bool:
    """
    Рекурсивно удаляет директорию в отдельном потоке.
    """
    path = Path(dir_path)
    if not path.exists():
        return False

    def _rmdir_sync() -> None:
        shutil.rmtree(path)

    try:
        await asyncio.to_thread(_rmdir_sync)
        logger.info(f"Директория удалена: {path}")
    except OSError as e:
        logger.error(f"Ошибка удаления директории {path}: {e}")
        return False
    else:
        return True


def get_loader_codes(
    supplier_codes_repo: Annotated[SupplierCodesRepo, Depends(get_supplier_codes_repo)],
) -> LoaderCodes:
    return LoaderCodes(supplier_codes_repo)
