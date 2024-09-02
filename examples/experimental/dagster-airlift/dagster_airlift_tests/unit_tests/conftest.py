from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Sequence, Tuple

from dagster import AssetSpec, Definitions, SensorResult, build_sensor_context
from dagster._core.definitions.events import AssetMaterialization
from dagster._core.definitions.repository_definition.repository_definition import (
    RepositoryDefinition,
)
from dagster._time import get_current_datetime
from dagster_airlift.core import build_defs_from_airflow_instance
from dagster_airlift.test import make_dag_run, make_instance


def strip_to_first_of_month(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def build_defs_from_airflow_asset_graph(
    assets_per_task: Dict[str, Dict[str, List[Tuple[str, List[str]]]]],
    additional_defs: Definitions = Definitions(),
) -> RepositoryDefinition:
    specs = []
    dag_and_task_structure = defaultdict(list)
    for dag_id, task_structure in assets_per_task.items():
        for task_id, asset_structure in task_structure.items():
            dag_and_task_structure[dag_id].append(task_id)
            for asset_key, deps in asset_structure:
                specs.append(
                    AssetSpec(
                        asset_key,
                        deps=deps,
                        tags={"airlift/dag_id": dag_id, "airlift/task_id": task_id},
                    )
                )
    instance = make_instance(
        dag_and_task_structure=dag_and_task_structure,
        dag_runs=[
            make_dag_run(
                dag_id=dag_id,
                run_id=f"run-{dag_id}",
                start_date=get_current_datetime() - timedelta(minutes=10),
                end_date=get_current_datetime(),
            )
            for dag_id in dag_and_task_structure.keys()
        ],
    )
    defs = Definitions.merge(
        additional_defs,
        Definitions(assets=specs),
    )
    repo_def = build_defs_from_airflow_instance(instance, defs=defs).get_repository_def()
    repo_def.load_all_definitions()
    return repo_def


def build_and_invoke_sensor(
    assets_per_task: Dict[str, Dict[str, List[Tuple[str, List[str]]]]],
    additional_defs: Definitions = Definitions(),
) -> SensorResult:
    repo_def = build_defs_from_airflow_asset_graph(assets_per_task, additional_defs=additional_defs)
    sensor = next(iter(repo_def.sensor_defs))
    context = build_sensor_context(repository_def=repo_def)
    result = sensor(context)
    assert isinstance(result, SensorResult)
    return result


def assert_expected_key_order(
    mats: Sequence[AssetMaterialization], expected_key_order: Sequence[str]
) -> None:
    assert [mat.asset_key.to_user_string() for mat in mats] == expected_key_order
