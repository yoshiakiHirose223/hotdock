def test_exam_index_lists_questions(client):
    response = client.get("/exam")

    assert response.status_code == 200
    assert "HTTP メソッドの基礎" in response.text


def test_exam_answer_flow(client):
    response = client.post(
        "/exam/questions/1",
        data={"selected_label": "B"},
    )

    assert response.status_code == 200
    assert "正解" in response.text
