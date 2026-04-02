import csv
import json
from io import StringIO


def convert_csv_text_to_json(csv_text: str) -> str:
    reader = csv.DictReader(StringIO(csv_text))
    rows = list(reader)
    return json.dumps(rows, ensure_ascii=False, indent=2)
