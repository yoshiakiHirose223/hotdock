from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.blog.common import BLOG_ADMIN_BASE_PATH, render_blog_template, service
from app.core.config import get_settings
from app.core.database import get_db

router = APIRouter()


@router.get("")
async def blog_index(request: Request, db: Session = Depends(get_db)):
    return render_blog_template(
        request,
        "blog/index.html",
        page_title="Blog",
        posts=service.list_posts(db),
    )


@router.get("/media/{token}")
async def blog_media(token: str, db: Session = Depends(get_db)):
    image = service.get_image_file(db, token)
    if image is None:
        raise HTTPException(status_code=404, detail="Image not found")

    file_path = get_settings().blog_images_dir / image.stored_filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Image file not found")

    return FileResponse(path=file_path, media_type=image.content_type, filename=image.original_filename)


@router.get("/{slug}")
async def blog_detail(request: Request, slug: str, db: Session = Depends(get_db)):
    post = service.get_post(db, slug)
    if post is None:
        raise HTTPException(status_code=404, detail="Post not found")

    return render_blog_template(
        request,
        "blog/detail.html",
        page_title=post.title,
        post=post,
    )
