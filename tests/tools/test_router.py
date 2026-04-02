def test_csv_to_json_preview(client):
    response = client.post(
        "/tools/csv-to-json",
        data={
            "csv_text": "name,score\nAlice,90\nBob,82",
            "action": "preview",
        },
    )

    assert response.status_code == 200
    assert '"name": "Alice"' in response.text
    assert '"score": "82"' in response.text


def test_csv_column_swap_preview(client):
    response = client.post(
        "/tools/csv-column-swap",
        data={
            "csv_text": "name,score,team\nAlice,90,A\nBob,82,B",
            "first_column": "score",
            "second_column": "team",
            "action": "preview",
        },
    )

    assert response.status_code == 200
    assert "name,team,score" in response.text
