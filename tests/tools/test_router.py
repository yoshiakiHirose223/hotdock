def test_csv_to_json_page(client):
    response = client.get("/tools/csv-to-json")

    assert response.status_code == 200
    assert "CSV to JSON" in response.text
    assert "JSON変換スタート" in response.text
    assert "/static/js/tools/csv-to-json/app.js" in response.text


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
