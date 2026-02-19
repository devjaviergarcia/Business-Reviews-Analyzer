from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import settings
from src.database import close_mongo_connection, connect_to_mongo
from src.routers.analysis import router as analysis_router
from src.routers.business import router as business_router
from src.routers.health import router as health_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    await connect_to_mongo()
    try:
        yield
    finally:
        await close_mongo_connection()


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="API for scraping, preprocessing and analyzing business reviews.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(business_router)
app.include_router(analysis_router)
