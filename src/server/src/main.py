import uvicorn
from fastapi import Depends, FastAPI

from src.lib.database import create_tables
from src.parsers.orchestrator import register_pipelines
from src.routes.auth import get_current_user, router as auth_router
from src.routes.logs import router as logs_router

app = FastAPI()

app.include_router(auth_router)
app.include_router(logs_router)


@app.on_event("startup")
def startup() -> None:
    create_tables()
    register_pipelines()


@app.get("/")
async def root(_: object = Depends(get_current_user)):
    return "Logdog API"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
