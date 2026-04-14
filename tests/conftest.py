import os
import tempfile
import uuid

import pytest
from fastapi.testclient import TestClient

os.environ["APP_ENV"] = "test"
os.environ["DATABASE_URL"] = f"sqlite:///{tempfile.gettempdir()}/project-test-{uuid.uuid4().hex}.db"

from app.core.database import SessionLocal, init_db
from app.main import app
from app.models.project_bookmark import ProjectBookmark


@pytest.fixture
def client():
    init_db()
    db = SessionLocal()
    db.query(ProjectBookmark).delete()
    db.commit()
    db.close()
    with TestClient(app) as test_client:
        yield test_client
