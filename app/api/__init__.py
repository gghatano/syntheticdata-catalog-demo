from fastapi import APIRouter

from app.api.datasets import router as datasets_router
from app.api.proposals import router as proposals_router

api_router = APIRouter()
api_router.include_router(datasets_router, prefix="/datasets", tags=["datasets"])
api_router.include_router(proposals_router, prefix="/proposals", tags=["proposals"])
