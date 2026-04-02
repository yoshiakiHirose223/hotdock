import os
import tempfile
import uuid

import pytest
from fastapi.testclient import TestClient

os.environ["APP_ENV"] = "test"
os.environ["DATABASE_URL"] = f"sqlite:///{tempfile.gettempdir()}/project-test-{uuid.uuid4().hex}.db"

from app.main import app


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client
