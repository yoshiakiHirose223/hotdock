import html
import re
import unicodedata
from datetime import date

import markdown

IMAGE_PLACEHOLDER_PATTERN = re.compile(r"\[\[image:\s*([^\]]+?)\s*\]\]")


def parse_front_matter(raw_text: str) -> tuple[dict[str, str], str]:
    if not raw_text.startswith("---"):
        return {}, raw_text

    lines = raw_text.splitlines()
    metadata: dict[str, str] = {}
    end_index = None

    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_index = index
            break
        key, _, value = line.partition(":")
        if key and value:
            metadata[key.strip()] = value.strip()

    if end_index is None:
        return {}, raw_text

    body = "\n".join(lines[end_index + 1 :]).strip()
    return metadata, body


def parse_date(value: str | None) -> date:
    if not value:
        return date.today()
    return date.fromisoformat(value)


def parse_tags(value: str | None) -> list[str]:
    if not value:
        return []
    return [tag.strip() for tag in value.split(",") if tag.strip()]


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on", "published"}:
        return True
    if normalized in {"0", "false", "no", "off", "draft", "private"}:
        return False
    return default


def normalize_slug(value: str, fallback: str = "post") -> str:
    normalized = unicodedata.normalize("NFKC", value).strip().lower()
    normalized = re.sub(r"[^\w\s-]", "", normalized, flags=re.UNICODE)
    normalized = re.sub(r"[-\s]+", "-", normalized).strip("-_")
    return normalized or fallback


def build_front_matter(metadata: dict[str, str]) -> str:
    lines = ["---"]
    for key, value in metadata.items():
        if value:
            lines.append(f"{key}: {value}")
    lines.append("---")
    return "\n".join(lines)


def inject_image_placeholders(markdown_text: str, image_lookup: dict[str, dict[str, str]]) -> str:
    def replace(match: re.Match[str]) -> str:
        token = match.group(1).strip()
        image = image_lookup.get(token)
        if image is None:
            return ""

        url = html.escape(image["url"], quote=True)
        alt = html.escape(image["alt"], quote=True)
        return (
            '\n<figure class="article-image">'
            f'<img src="{url}" alt="{alt}" loading="lazy">'
            "</figure>\n"
        )

    return IMAGE_PLACEHOLDER_PATTERN.sub(replace, markdown_text)


def render_markdown(markdown_text: str) -> str:
    return markdown.markdown(
        markdown_text,
        extensions=["fenced_code", "tables", "toc", "sane_lists"],
    )
