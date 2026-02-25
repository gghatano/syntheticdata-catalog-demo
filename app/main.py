from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.config import SESSION_SECRET_KEY
from app.db.base import Base
from app.db.session import engine
from app.dependencies import RequiresLoginException
from app.api import api_router
from app.web.routers import auth, hr, proposer

app = FastAPI(title="合成データ活用デモ基盤")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)

app.mount("/static", StaticFiles(directory="app/web/static"), name="static")

app.include_router(api_router, prefix="/api")
app.include_router(auth.router)
app.include_router(hr.router, prefix="/hr")
app.include_router(proposer.router, prefix="/proposer")


@app.exception_handler(RequiresLoginException)
async def requires_login_handler(request: Request, exc: RequiresLoginException):
    return RedirectResponse(url="/login", status_code=303)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
