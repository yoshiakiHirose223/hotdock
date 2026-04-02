from pydantic import BaseModel


class ToolDescriptor(BaseModel):
    slug: str
    name: str
    description: str


class ToolExecutionResult(BaseModel):
    input_text: str
    output_text: str
