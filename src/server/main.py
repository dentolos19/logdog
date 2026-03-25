import asyncio
import os
from contextlib import asynccontextmanager

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.requests import Request

load_dotenv()

from lib.database import create_all_tables

ALLOWED_ORIGIN = os.getenv("APP_URL", "http://localhost:3000")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.to_thread(create_all_tables)
    from lib.parsers.orchestrator import register_pipelines

    register_pipelines()
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"message": exc.detail})


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(status_code=422, content={"message": "Invalid request."})


app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"],
)

from routes.auth import router as auth_router
from routes.logs import router as logs_router
from routes.parser import router as parser_router
from routes.stats import router as stats_router

app.include_router(auth_router)
app.include_router(logs_router)
app.include_router(parser_router)
app.include_router(stats_router)


@app.get("/")
async def root():
    return {"message": "Hello, World!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
