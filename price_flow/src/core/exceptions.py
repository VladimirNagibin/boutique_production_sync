from pathlib import Path


class BaseAppException(Exception):
    """Базовое исключение для приложения."""

    def __init__(self, error_code: str, message: str):
        self.error_code = error_code
        self.message = message
        super().__init__(message)


class ZipFileNotFoundError(BaseAppException, FileNotFoundError):
    """Исключение, если файл архива не найден."""

    def __init__(self, path: Path | str, message: str | None = None) -> None:
        path_str = str(path)
        message = message or f"Файл не найден: {path_str}"
        error_code = "ZIP_FILE_NOT_FOUND"

        super().__init__(error_code, message)

        self.filename = path_str


class ZipExtractionError(BaseAppException):
    """Общая ошибка при распаковке."""

    def __init__(self, message: str) -> None:
        error_code = "ZIP_EXTRACTION_ERROR"
        super().__init__(error_code, message)
