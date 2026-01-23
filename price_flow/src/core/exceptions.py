class BaseAppException(Exception):
    """Базовое исключение для нашего приложения."""

    def __init__(self, error_code: str, message: str):
        self.error_code = error_code
        self.message = message
        super().__init__(message)
