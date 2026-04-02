from dataclasses import dataclass


@dataclass(slots=True)
class ToolDefinition:
    slug: str
    name: str
    description: str
