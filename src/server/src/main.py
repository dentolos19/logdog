from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI

from parsers.orchestrator import register_pipelines
from routes.auth import get_current_user, router as auth_router
from routes.logs import router as logs_router
from routes.stats import router as stats_router


@asynccontextmanager
async def lifespan(_: FastAPI):
    register_pipelines()
    yield


app = FastAPI(lifespan=lifespan)

app.include_router(auth_router)
app.include_router(logs_router)
app.include_router(stats_router)


@app.get("/")
async def root(_: object = Depends(get_current_user)):
    return "Logdog"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3001)
