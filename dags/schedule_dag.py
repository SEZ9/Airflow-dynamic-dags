"""Schedule Dag.
"""
from datetime import timedelta
from typing import List, Dict, Any
import logging
from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from config.conf import Config
from lib.schedule_parser import ScheduleParser
from task_factory import ScheduleTaskFactory
    

class ScheduleDagFactory:
    """schedule dag工厂类.
    """

    SCHEDULE_DAG_NAME_PREFIX = "schedule"
    SCHEDULE_DAG_START_DATE = days_ago(1)
    SCHEDULE_DAG_CATCHUP = False
    SCHEDULE_DAG_MAX_ACTIVE_RUNS = 1
    SCHEDULE_DAG_DEFAULT_RETRIES = 0
    SCHEDULE_DAG_DEFAULT_RETRY_DELAY = timedelta(seconds=30)

    def __init__(
        self,
        schedule_id: int,
        schedule_name: str,
        schedule_description: str,
        schedule_params: dict,
        schedule_job_configs: list,
        schedule_job_dependencies: list
    ):
        self.schedule_id = schedule_id
        self.schedule_name = schedule_name
        self.schedule_description = schedule_description
        self.schedule_params = schedule_params
        self.schedule_job_configs = schedule_job_configs
        self.schedule_job_dependencies = schedule_job_dependencies

    def _create_dag(self) -> DAG:
        """创建schedule dag实例"""
        return DAG(
            dag_id=f"{self.SCHEDULE_DAG_NAME_PREFIX}-{self.schedule_name}",
            description=self.schedule_description,
            tags=[self.SCHEDULE_DAG_NAME_PREFIX],
            schedule_interval=self.schedule_params["schedule_interval"],
            catchup=self.SCHEDULE_DAG_CATCHUP,
            max_active_runs=self.SCHEDULE_DAG_MAX_ACTIVE_RUNS,
            default_args={
                "start_date": days_ago(1),
                "retries": self.SCHEDULE_DAG_DEFAULT_RETRIES,
                "retry_delay": self.SCHEDULE_DAG_DEFAULT_RETRY_DELAY
            }
        )

    def _create_trigger_job_tasks(self, job) -> TaskGroup:
        """创建trigger  job任务实例"""
        trigger_job_tgi = ScheduleTaskFactory(
            job_id=job["id"],
            job_name=job["job_name"]
        ).get_task_group_instance()

        return trigger_job_tgi

    def _create_dummy_start_task(self) -> BaseOperator:
        """创建dummy start task实例"""  
        dummy_start = DummyOperator(
            task_id='start',
            trigger_rule=TriggerRule.ALL_DONE
        )

        return dummy_start

    def _create_dummy_end_task(self) -> BaseOperator:
        """创建dummy end task实例"""
        dummy_end = DummyOperator(
            task_id='end',
            trigger_rule=TriggerRule.ALL_DONE
        )
        
        return dummy_end

    def generate_dag_instance(self) -> DAG:
        """获取schedule dag实例，主要步骤：
        1. 创建schedule dag实例
        2. 遍历关联job列表，逐一创建trigger job任务实例
        3. 遍历job dependencies列表，逐一创建trigger job之间依赖关系
        5. 创建dummy_start, dummy_end任务实例

        Returns:
            DAG: 返回1个schedule dag实例.
        """
        # 创建dag实例
        with self._create_dag() as dag:
            # 创建trigger job任务实例
            for job in self.schedule_job_configs:
                locals()[f"var_{job['job_name']}"] = self._create_trigger_job_tasks(job)
        
            logging.warning(str(locals()))
            try:
                # 逐一创建trigger job之间依赖关系
                if self.schedule_job_dependencies:
                    for jd in self.schedule_job_dependencies:
                        locals()[f"var_{jd['upstream_job_name']}"].set_downstream(locals()[f"var_{jd['downstream_job_name']}"])
            except Exception as err:
                raise Exception(  str(locals())) 
            roots_task_list = dag.roots
            leaves_task_list = dag.leaves

            # 创建dummy_start, dummy_end任务实例
            dummy_start_ti = self._create_dummy_start_task()
            dummy_end_ti = self._create_dummy_end_task()

            # 创建剩余任务依赖关系
            for ti in roots_task_list:
                dummy_start_ti >> ti

            for ti in leaves_task_list:
                ti >> dummy_end_ti
        
        return dag
    

# 调用 schedule配置解析类，获取 schedule配置
schedule_config_parser = ScheduleParser()
schedule_configs = schedule_config_parser.parse()

# 遍历 schedule配置列表
if schedule_configs:
    for schedule_config in schedule_configs:
        try:
            # 调用 schedule dag工厂类，在全局命名空间逐一创建 schedule dag实例
            schedule_dag_factory = ScheduleDagFactory(
                schedule_id=schedule_config["schedule_id"],
                schedule_name=schedule_config["schedule_name"],
                schedule_description=schedule_config["schedule_description"],
                schedule_params=schedule_config["schedule_params"],
                schedule_job_configs=schedule_config["schedule_job_configs"],
                schedule_job_dependencies=schedule_config["schedule_job_dependencies"]
            )
            globals()[f"var_{schedule_config['schedule_name']}"] = schedule_dag_factory.generate_dag_instance()
        except Exception as err:
            raise Exception(f"Failed to create  schedule dag\nSchedule id: {schedule_config['schedule_id']}\nSchedule name: {schedule_config['schedule_name']}")