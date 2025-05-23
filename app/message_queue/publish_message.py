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
class PublishMessagePayload:
    gid: str
    status: str
    completed_at: str
    validation_results: any
