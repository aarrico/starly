from typing import Any

from fastapi.responses import JSONResponse


class HttpErrorResponse(JSONResponse):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: Any = None,
        headers: dict[str, str] | None = None,
    ):
        content = {"error": {"code": code, "message": message, "details": details}}
        super().__init__(status_code=status_code, content=content, headers=headers)
