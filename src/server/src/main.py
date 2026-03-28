import uvicorn
from fastapi import FastAPI

from src.lib.database import create_tables
from src.parsers.orchestrator import register_pipelines

app = FastAPI()


@app.on_event("startup")
def startup() -> None:
    create_tables()
    register_pipelines()


@app.get("/")
async def root():
    return "Hello, world!"


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
