"""Validation suite and stage definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobSpec:
    name: str
    job: str
    description: str


SUITES: dict[str, JobSpec] = {
    "run": JobSpec(
        name="run",
        job="notebook_parity",
        description="Full notebook-parity validation job.",
    ),
    "extras": JobSpec(
        name="extras",
        job="extras",
        description="Extra regression validation job.",
    ),
    "metadata": JobSpec(
        name="metadata",
        job="metadata",
        description="Metadata sync validation job.",
    ),
}

STAGES: dict[str, JobSpec] = {
    "load-graph": JobSpec(
        name="load-graph",
        job="stage_load_graph",
        description="Load aircraft CSV data into Neo4j.",
    ),
    "connection": JobSpec(
        name="connection",
        job="stage_connection",
        description="Create the UC JDBC connection and validate remote_query().",
    ),
    "federated": JobSpec(
        name="federated",
        job="stage_federated",
        description="Run getting-started live remote_query() federated queries.",
    ),
    "materialized": JobSpec(
        name="materialized",
        job="stage_materialized",
        description="Materialize Neo4j data to Delta and validate Delta SQL.",
    ),
    "metadata-api": JobSpec(
        name="metadata-api",
        job="stage_metadata_api",
        description="Register Neo4j schema through the External Metadata API.",
    ),
    "advanced-federated": JobSpec(
        name="advanced-federated",
        job="stage_advanced_federated",
        description="Run advanced remote_query() and Delta join checks.",
    ),
    "connection-smoke": JobSpec(
        name="connection-smoke",
        job="stage_connection_smoke",
        description="Run broader connection smoke checks.",
    ),
    "federated-extra": JobSpec(
        name="federated-extra",
        job="stage_federated_extra",
        description="Run broader federated regression checks.",
    ),
    "metadata-tables": JobSpec(
        name="metadata-tables",
        job="stage_metadata_tables",
        description="Materialize discovered Neo4j labels and relationships as Delta.",
    ),
    "metadata-grant": JobSpec(
        name="metadata-grant",
        job="stage_metadata_grant",
        description="Grant CREATE_EXTERNAL_METADATA for metadata validation.",
    ),
}


def stage_names() -> list[str]:
    return list(STAGES)
