import os
import tempfile
import uuid

import pytest
from fastapi.testclient import TestClient

os.environ["APP_ENV"] = "test"
os.environ["DATABASE_URL"] = f"sqlite:///{tempfile.gettempdir()}/project-test-{uuid.uuid4().hex}.db"
os.environ["GITHUB_MOCK_OAUTH_ENABLED"] = "true"
os.environ["GITHUB_APP_INSTALL_URL"] = "https://github.com/apps/hotdock/installations/new"
os.environ["GITHUB_APP_WEBHOOK_SECRET"] = "test-webhook-secret"

from app.core.database import SessionLocal, init_db
from app.main import app
from app.models import Base
from app.models.project_bookmark import ProjectBookmark


@pytest.fixture
def client():
    init_db()
    db = SessionLocal()
    for table in reversed(Base.metadata.sorted_tables):
        db.execute(table.delete())
    db.commit()
    db.close()
    with TestClient(app) as test_client:
        yield test_client
