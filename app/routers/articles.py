from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List
from app.models.article import Article, ArticleCreate
from app.core.security import get_current_user

router = APIRouter(prefix="/articles", tags=["articles"])

@router.post("/", response_model=Article)
async def create_article(
    request: Request,
    article: ArticleCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.insert_article(article.title, article.content)
    article_id = client._get_next_id() - 1
    result = client.get_article(article_id)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return {"id": result[0], "title": result[1], "content": result[2], "created_at": result[3]}

@router.get("/", response_model=List[Article])
async def read_articles(
    request: Request,
    skip: int = 0,
    limit: int = 10,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    results = client.get_articles(skip, limit)
    return [
        {"id": r[0], "title": r[1], "content": r[2], "created_at": r[3]} for r in results
    ]

@router.get("/{article_id}", response_model=Article)
async def read_article(
    request: Request,
    article_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.get_article(article_id)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return {"id": result[0], "title": result[1], "content": result[2], "created_at": result[3]}

@router.put("/{article_id}", response_model=Article)
async def update_article(
    request: Request,
    article_id: int,
    article: ArticleCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.update_article(article_id, article.title, article.content)
    result = client.get_article(article_id)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return {"id": result[0], "title": result[1], "content": result[2], "created_at": result[3]}

@router.delete("/{article_id}")
async def delete_article(
    request: Request,
    article_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.delete_article(article_id)
    if not result:
        raise HTTPException(status_code=404, detail="Article not found")
    return {"message": "Article deleted"}