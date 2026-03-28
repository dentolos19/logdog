import asyncio
import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv

load_dotenv()
for key, val in os.environ.items():
    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        os.environ[key] = val[1:-1]

import uvicorn
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.requests import Request

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
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"message": "Invalid request."})


app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN],
    allow_credentials=True,
    allow_headers=["*"],
    allow_methods=["*"],
)

from routes.auth import router as auth_router
from routes.chat import router as chat_router
from routes.logs import router as logs_router
from routes.parser import router as parser_router
from routes.stats import router as stats_router

app.include_router(auth_router)
app.include_router(chat_router)
app.include_router(logs_router)
app.include_router(parser_router)
app.include_router(stats_router)


@app.get("/")
async def root():
    return "Hello, world!"

@app.get("/test")
async def test():
    return "Hello, test!"

@app.get("/env")
async def show_env():
    for key, val in os.environ.items():
        print(f"{key}={val}")
    return os.environ.items()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
