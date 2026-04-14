from fastapi.templating import Jinja2Templates

from app.core.config import get_settings


def create_templates() -> Jinja2Templates:
    settings = get_settings()
    templates = Jinja2Templates(directory=str(settings.templates_dir))
    return templates
