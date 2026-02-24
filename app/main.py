from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.config import SESSION_SECRET_KEY
from app.db.base import Base
from app.db.session import engine
from app.web.routers import auth, hr, proposer

app = FastAPI(title="合成データ活用デモ基盤")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")

app.include_router(auth.router)
app.include_router(hr.router, prefix="/hr")
app.include_router(proposer.router, prefix="/proposer")


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
