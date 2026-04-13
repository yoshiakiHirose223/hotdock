def test_home_page_returns_heeloworld(client):
    response = client.get("/")

    assert response.status_code == 200
    assert "heeloworld" in response.text


def test_removed_feature_routes_return_not_found(client):
    for path in ("/blog", "/tools", "/exam"):
        response = client.get(path)
        assert response.status_code == 404
