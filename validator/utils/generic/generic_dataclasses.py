import dataclasses

@dataclasses.dataclass
class GenericResponse:
    status_code: int
    content: str | None
    job_id: str
    error_message: str | None = None