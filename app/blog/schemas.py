from datetime import date, datetime

from pydantic import BaseModel, Field


class BlogPostSummary(BaseModel):
    slug: str
    title: str
    summary: str = ""
    published_at: date
    tags: list[str]


class BlogPostDetail(BlogPostSummary):
    content: str
    html: str


class BlogAdminPostSummary(BaseModel):
    id: int
    slug: str
    title: str
    summary: str = ""
    published_at: date
    updated_at: datetime | None = None
    is_published: bool
    tags: list[str]
    source_filename: str


class BlogTagOption(BaseModel):
    id: int
    name: str
    slug: str
    is_selected: bool = False


class BlogImageAsset(BaseModel):
    id: int | None = None
    token: str
    placeholder_tag: str
    url: str
    original_filename: str
    created_at: datetime | None = None
    is_staged: bool = False


class BlogEditorState(BaseModel):
    post_id: int | None = None
    draft_key: str = ""
    title: str = ""
    summary: str = ""
    slug: str = ""
    markdown_source: str = ""
    published_at: date = Field(default_factory=date.today)
    selected_tag_slugs: list[str] = Field(default_factory=list)
    available_tags: list[BlogTagOption] = Field(default_factory=list)
    available_images: list[BlogImageAsset] = Field(default_factory=list)
    new_tag_name: str = ""
    is_published: bool = False
    preview_html: str = ""
    source_filename: str = ""
