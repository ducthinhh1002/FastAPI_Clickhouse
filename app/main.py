from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer
from app.routers import auth, articles
from app.core.config import settings

app = FastAPI(title="FastAPI ClickHouse API")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

app.include_router(auth.router)
app.include_router(articles.router)

@app.get("/")
async def root():
    return {"message": "FastAPI ClickHouse API"}