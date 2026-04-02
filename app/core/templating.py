from fastapi.templating import Jinja2Templates
from jinja2 import ChoiceLoader, FileSystemLoader, PrefixLoader

from app.core.config import get_settings


def create_templates() -> Jinja2Templates:
    settings = get_settings()
    templates = Jinja2Templates(directory=str(settings.shared_templates_dir))
    templates.env.loader = ChoiceLoader(
        [
            FileSystemLoader(str(settings.shared_templates_dir)),
            PrefixLoader(
                {
                    "site": FileSystemLoader(str(settings.site_templates_dir)),
                    "blog": FileSystemLoader(str(settings.blog_templates_dir)),
                    "tools": FileSystemLoader(str(settings.tools_templates_dir)),
                    "exam": FileSystemLoader(str(settings.exam_templates_dir)),
                }
            ),
        ]
    )
    return templates
