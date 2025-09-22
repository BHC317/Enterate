from fastapi.testclient import TestClient
from app.main import app

test_client = TestClient(app)

# test GET /
def test_get_root():
    response = test_client.get("/")
    assert response.status_code == 200
