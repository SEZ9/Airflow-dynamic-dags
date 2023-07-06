import re
from datetime import timedelta
from typing import List

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

from config.conf import Config
from task_factory.base_task_factory import BaseTaskFactory

class ScheduleTaskFactory(BaseTaskFactory):
    """ schedule特殊task group工厂类
    - 针对给定的 job，创建一个airflow task group，用于实现以下两个目标
        - 触发 job dag run执行
        - 监听 job dag run执行状态，如果dag run为success状态则此task group标记为成功
        - 监听 job dag run执行状态，如果dag run为running状态则继续等待，此task group保持running状态
        - 监听 job dag run执行状态，如果dag run为failed状态则此task group标记为失败
    - 所需的airflow task group由单个TriggerDagRunOperator组成，TriggerDagRunOperator必要参数说明：
        - wait_for_completion = True, 代表监听模式，触发external dag之后会持续监听该external dag的执行状态
        - allowed_states = ["success"]，当监听到external dag处于success状态时，此任务会标记为success
        - failed_states = ["failed"]，当监听到external dag处于failed状态时，此任务会标记为failed
    """    
    TRIGGER_DAGRUN_WAIT_FOR_COMPLETION = True
    TRIGGER_DAGRUN_ALLOWED_STATES = ["success"]
    TRIGGER_DAGRUN_FAILED_STATES = ["failed"]
    TRIGGER_DAGRUN_RESET_DAG_RUN = True

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def get_task_group_instance(self) -> TaskGroup:

        with TaskGroup(group_id=self.job_name, prefix_group_id=False) as tgi:
            trigger_task = TriggerDagRunOperator(
                task_id=f"trigger_{self.job_name}",
                trigger_dag_id=f"job-{self.job_name}",
                execution_date="{{  ts  }}",
                reset_dag_run=self.TRIGGER_DAGRUN_RESET_DAG_RUN,
                trigger_rule=Config.ETL_SCHEDULE_TRIGGER_RULE,
                wait_for_completion=self.TRIGGER_DAGRUN_WAIT_FOR_COMPLETION,
                allowed_states=self.TRIGGER_DAGRUN_ALLOWED_STATES,
                failed_states=self.TRIGGER_DAGRUN_FAILED_STATES
            )

        return tgi