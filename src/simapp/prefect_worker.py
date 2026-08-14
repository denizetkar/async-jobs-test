"""Prefect worker entrypoint — serves flows for the simulation scenario."""

from __future__ import annotations

from prefect import serve

from simapp.prefect_flows import process_dataset_flow, simulation_flow


def main() -> None:
    d1 = process_dataset_flow.to_deployment(name="process-dataset-deployment")
    d2 = simulation_flow.to_deployment(name="simulation-deployment")
    serve(d1, d2)


if __name__ == "__main__":
    main()
