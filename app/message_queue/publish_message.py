from dataclasses import dataclass
from typing import Literal


@dataclass
class PublishMessageHeader:
    event_id: str
    event_type: Literal["image.validation.result"]
    trace_id: str
    timestamp: str
    source_service: str


@dataclass
class ValidationServiceData:
    is_blank: bool


@dataclass
class PublishMessageBody:
    gid: str
    status: Literal["success", "fail"]
    completed_at: str
    payload: ValidationServiceData
