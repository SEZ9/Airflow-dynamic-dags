
from airflow.utils.types import DagRunType

def get_run_id_by_execution_date(execution_date: str) -> str:
    return f"{DagRunType.MANUAL}__{execution_date}"