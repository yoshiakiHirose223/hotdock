import csv
from io import StringIO


def swap_csv_columns(csv_text: str, first_column: str, second_column: str) -> str:
    reader = csv.DictReader(StringIO(csv_text))
    fieldnames = list(reader.fieldnames or [])
    if first_column not in fieldnames or second_column not in fieldnames:
        raise ValueError("指定されたカラムが CSV に存在しません。")

    first_index = fieldnames.index(first_column)
    second_index = fieldnames.index(second_column)
    fieldnames[first_index], fieldnames[second_index] = (
        fieldnames[second_index],
        fieldnames[first_index],
    )

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        writer.writerow(row)

    return output.getvalue()
