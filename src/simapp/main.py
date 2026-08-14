"""FastAPI application — simulation dataset upload and simulation orchestration."""

from __future__ import annotations

import os
from uuid import UUID

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from simapp.config import settings
from simapp.deps import get_scheduler, get_session
from simapp.models import Dataset, DatasetStatus, Simulation, SimulationStatus
from simapp.schemas import DatasetResponse, SimulationRequest, SimulationResponse
from simapp.scheduler import SimulationScheduler

app = FastAPI(title="SimApp", version="0.1.0")

os.makedirs(settings.upload_dir, exist_ok=True)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/datasets", response_model=DatasetResponse, status_code=201)
def upload_dataset(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
    scheduler: SimulationScheduler = Depends(get_scheduler),
) -> DatasetResponse:
    dataset = Dataset(filename=file.filename or "unknown", status=DatasetStatus.pending)
    session.add(dataset)
    session.flush()

    upload_path = os.path.join(settings.upload_dir, f"{dataset.id}_{file.filename}")
    with open(upload_path, "wb") as f:
        while chunk := file.file.read(1024 * 1024):
            f.write(chunk)

    scheduler.schedule_dataset_processing(
        session=session,
        dataset_id=dataset.id,
        filename=dataset.filename,
    )
    session.commit()
    session.refresh(dataset)
    return DatasetResponse.model_validate(dataset)


@app.get("/datasets/{dataset_id}", response_model=DatasetResponse)
def get_dataset(
    dataset_id: UUID,
    session: Session = Depends(get_session),
) -> DatasetResponse:
    dataset = session.get(Dataset, dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse.model_validate(dataset)


@app.post("/simulations", response_model=SimulationResponse, status_code=201)
def start_simulation(
    request: SimulationRequest,
    session: Session = Depends(get_session),
    scheduler: SimulationScheduler = Depends(get_scheduler),
) -> SimulationResponse:
    dataset = session.get(Dataset, request.dataset_id)
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found")

    simulation = Simulation(
        dataset_id=dataset.id,
        status=SimulationStatus.pending,
        parameters=request.parameters.model_dump(),
    )
    session.add(simulation)
    session.flush()

    scheduler.schedule_simulation(
        session=session,
        simulation_id=simulation.id,
        dataset_id=dataset.id,
        parameters=request.parameters.model_dump(),
    )
    session.commit()
    session.refresh(simulation)
    return SimulationResponse.model_validate(simulation)


@app.get("/simulations/{simulation_id}", response_model=SimulationResponse)
def get_simulation(
    simulation_id: UUID,
    session: Session = Depends(get_session),
) -> SimulationResponse:
    simulation = session.get(Simulation, simulation_id)
    if simulation is None:
        raise HTTPException(status_code=404, detail="Simulation not found")
    return SimulationResponse.model_validate(simulation)
