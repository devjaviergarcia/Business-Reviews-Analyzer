from __future__ import annotations

from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from src.business_catalog.business_artifact_runtime import BusinessArtifactRuntime


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_ROOT = (PROJECT_ROOT / "artifacts").resolve()
artifact_runtime = BusinessArtifactRuntime(
    project_root=PROJECT_ROOT,
    artifacts_root=ARTIFACTS_ROOT,
)


class OpenArtifactRequest(BaseModel):
    path: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


app = FastAPI(
    title="Local Artifact Opener Bridge",
    description="Host-side bridge that opens local report artifacts with the desktop default app.",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, object]:
    return {
        "ok": True,
        "project_root": str(PROJECT_ROOT),
        "artifacts_root": str(ARTIFACTS_ROOT),
    }


@app.post("/open")
async def open_artifact(payload: OpenArtifactRequest) -> dict[str, object]:
    try:
        resolved_path = artifact_runtime.open_report_artifact_path(path=payload.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "path": str(resolved_path)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8766)
