from pathlib import Path
from typing import Any


class BaseAppException(Exception):
    """Базовое исключение для приложения."""

    def __init__(
        self, error_code: str, message: str, details: Any | None = None
    ) -> None:
        self.error_code = error_code
        self.message = message
        self.details = details
        super().__init__(message)


class FileAppException(BaseAppException):
    """Базовое исключение работы с файлами."""

    def __init__(
        self,
        path: Path | str,
        error_code: str = "FILE_PROCESSING_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        path_str = str(path)
        message = message or f"Ошибка при работе с файлом: {path_str}"
        self.path = path_str
        self.details = details
        super().__init__(error_code, message, details)

    def __str__(self) -> str:
        return self.message


class FileAppNotFoundError(FileAppException, FileNotFoundError):
    """Исключение, если файл не найден."""

    def __init__(
        self,
        path: Path | str,
        message: str | None = None,
        error_code: str | None = None,
    ) -> None:
        path_str = str(path)
        message = message or f"Файл не найден: {path_str}"
        error_code = error_code or "FILE_NOT_FOUND_ERROR"
        super().__init__(path=path_str, message=message, error_code=error_code)


class ZipExtractionError(FileAppException):
    """Общая ошибка при распаковке."""

    def __init__(self, path: Path | str, message: str | None = None) -> None:
        path_str = str(path)
        message = message or f"Ошибка распаковки файла: {path_str}"
        super().__init__(
            path=path_str, message=message, error_code="ZIP_EXTRACTION_ERROR"
        )


class FileNotZipError(FileAppException):
    """Расширение файла не zip."""

    def __init__(self, path: Path | str, message: str | None = None) -> None:
        path_str = str(path)
        message = message or f"Расширение файла не zip: {path_str}"
        super().__init__(
            path=path_str, message=message, error_code="FILE_NOT_ZIP_ERROR"
        )


class FileSizeError(FileAppException):
    """Размер файла превышает максимальный."""

    def __init__(
        self,
        path: Path | str,
        message: str | None = None,
        file_size: int | None = None,
        max_file_size: int | None = None,
    ) -> None:
        path_str = str(path)
        file_size_str = f"({file_size})" if file_size else ""
        max_file_size_str = f"({max_file_size})" if max_file_size else ""
        if not message:
            message = (
                f"Размер файла{file_size_str} превышает максимальный"
                f"{max_file_size_str}: {path_str}"
            )

        super().__init__(path=path_str, message=message, error_code="FILE_SIZE_ERROR")


class CSVParsingError(FileAppException):
    """Ошибка парсинга CSV файла."""

    def __init__(
        self,
        path: Path | str,
        message: str | None = None,
    ):
        path_str = str(path)
        message = message or f"Ошибка парсинга CSV файла: {path_str}"
        self.path = path_str
        super().__init__(path, error_code="CSV_FILE_PARSING_ERROR", message=message)


class FileUploadError(FileAppException):
    """Ошибка загрузки файла."""

    def __init__(
        self,
        path: Path | str,
        message: str | None = None,
    ):
        path_str = str(path)
        message = message or f"Ошибка загрузки файла: {path_str}"
        self.path = path_str
        super().__init__(path, error_code="FILE_UPLOAD_ERROR", message=message)


class DatabaseAppError(BaseAppException):
    """Ошибка работы с БД."""

    def __init__(
        self, message: str | None = None, error_code: str = "ERROR_WORKING_WITH_DB"
    ):
        message = message or "Ошибка работы с БД"
        super().__init__(error_code=error_code, message=message)


class DatabaseLoadError(DatabaseAppError):
    """Ошибка загрузки данных в БД."""

    def __init__(
        self,
        message: str | None = None,
    ):
        message = message or "Ошибка загрузки данных в БД"
        super().__init__(error_code="ERROR_LOADING_DATA_TO_DB", message=message)


class DataProcessingError(BaseAppException):
    """Ошибка обработки данных."""

    def __init__(
        self,
        message: str | None = None,
    ):
        message = message or "Ошибка обработки данных"
        super().__init__(error_code="DATA_PROCESSING_ERROR", message=message)


class PriceProcessingError(BaseAppException):
    """Базовая ошибка обработки прайс-листа."""

    def __init__(
        self,
        error_code: str = "PRICE_PROCESSING_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        message = message or "Ошибка обработки прайс-листа"
        super().__init__(error_code, message, details)


class EmailFetchError(PriceProcessingError):
    """Ошибка при получении почты или парсинге письма."""

    def __init__(
        self,
        error_code: str = "EMAIL_FETCH_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        message = message or "Ошибка при получении почты или парсинге письма"
        super().__init__(error_code, message, details)


class DriveApiError(PriceProcessingError):
    """Ошибка при работе с Google Drive API."""

    def __init__(
        self,
        error_code: str = "DRIVE_API_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        message = message or "Ошибка при работе с Google Drive API"
        super().__init__(error_code, message, details)


class ExcelProcessingError(PriceProcessingError):
    """Ошибка при чтении или записи Excel."""

    def __init__(
        self,
        error_code: str = "EXCEL_PROCESSING_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        message = message or "Ошибка при чтении или записи Excel"
        super().__init__(error_code, message, details)


class SupplierDataError(PriceProcessingError):
    """Ошибка при чтении или записи данных поставщика."""

    def __init__(
        self,
        error_code: str = "SUPPLIER_DATA_ERROR",
        message: str | None = None,
        details: Any | None = None,
    ):
        message = message or "Ошибка при чтении или записи данных поставщика"
        super().__init__(error_code, message, details)
