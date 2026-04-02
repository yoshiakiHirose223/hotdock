from app.tools.csv_column_swap import swap_csv_columns
from app.tools.csv_to_json import convert_csv_text_to_json
from app.tools.models import ToolDefinition
from app.tools.schemas import ToolDescriptor


class ToolsService:
    def list_tools(self) -> list[ToolDescriptor]:
        tools = [
            ToolDefinition(
                slug="csv-to-json",
                name="CSV to JSON",
                description="CSV の行データを JSON 配列へ変換します。",
            ),
            ToolDefinition(
                slug="csv-column-swap",
                name="CSV Column Swap",
                description="指定した 2 カラムの表示順を入れ替えます。",
            ),
        ]
        return [
            ToolDescriptor(
                slug=tool.slug,
                name=tool.name,
                description=tool.description,
            )
            for tool in tools
        ]

    def convert_csv_to_json(self, csv_text: str) -> str:
        return convert_csv_text_to_json(csv_text)

    def swap_columns(self, csv_text: str, first_column: str, second_column: str) -> str:
        return swap_csv_columns(csv_text, first_column, second_column)
