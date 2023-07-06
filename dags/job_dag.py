""" Job Dag.
"""
import uuid
from datetime import timedelta
from typing import List, Dict, Any

from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from config.conf import Config
from lib.job_parser import JobParser
from task_factory.base_task_factory import BaseTaskFactory


class JobDagFactory:
    """ job dag工厂类.
    """
    JOB_DAG_NAME_PREFIX = "job"
    JOB_DAG_SCHEDULE_INTERVAL = None
    JOB_DAG_START_DATE = days_ago(1)
    JOB_DAG_CATCHUP = False
    JOB_DAG_MAX_ACTIVE_RUNS = 1
    JOB_DAG_DEFAULT_RETRIES = 2
    JOB_DAG_DEFAULT_RETRY_DELAY = timedelta(seconds=30)

    def __init__(
        self,
        job_id: int,
        job_name: str,
        job_type: str,
        job_params: dict,
        job_task_configs: list,
        job_task_dependencies: list
    ):
        self.job_id = job_id
        self.job_name = job_name
        self.job_type = job_type
        self.job_params = job_params
        self.job_task_configs = job_task_configs
        self.job_task_dependencies = job_task_dependencies
    
    def _create_dag(self) -> DAG:
        """创建 job dag实例"""        
        return DAG(
            dag_id=f"{self.JOB_DAG_NAME_PREFIX}-{self.job_name}",
            tags=[self.JOB_DAG_NAME_PREFIX],
            schedule_interval=self.JOB_DAG_SCHEDULE_INTERVAL,
            catchup=self.JOB_DAG_CATCHUP,
            max_active_runs=self.JOB_DAG_MAX_ACTIVE_RUNS,
            default_args={
                "start_date": days_ago(1),
                "retries": self.JOB_DAG_DEFAULT_RETRIES,
                "retry_delay": self.JOB_DAG_DEFAULT_RETRY_DELAY
            }
        )

    def _get_task_factory_class_by_task_type(self, task_type: str) -> BaseTaskFactory:
        """通过 task类型，获取 task工厂类"""        
        task_factory_class_name = Config.TASK_FACTORY_MAPPING_DICT.get(task_type)

        if not task_factory_class_name:
            raise Exception(f"Cannot find task factory class for task type {task_type}. Please make sure corresponding task facotry class has already been implemented.")
        
        task_factory_class = getattr(
            __import__(Config.TASK_FACTORY_MODULE_PATH, fromlist=["None"]),
            task_factory_class_name
        )

        if not task_factory_class:
            raise Exception(f"Cannot import task factory class {task_factory_class}.")
        
        return task_factory_class
    
    
    def _create_tasks(self, task: dict) -> TaskGroup:
        """创建 task实例， task封装为一个task group
        """        
        try:
            with TaskGroup(group_id=f"group_of_tasks-{task['task_name']}", prefix_group_id=False) as outer_tgi:
                task_factory_class = self._get_task_factory_class_by_task_type(task["task_type"])

                tgi = task_factory_class(
                    task_id=task["task_id"],
                    task_name=task["task_name"],
                    task_type=task["task_type"],
                    task_params=task["task_params"],
                    job_id=self.job_id,
                    job_name=self.job_name,
                    job_params=self.job_params
                ).get_task_group_instance()

              
            return outer_tgi

        except Exception as err:
            raise Exception(f"Error while creating task instances for  task \n task id: {task['task_id']}\n task name: {task['task_name']}")
        
    
    
    def _create_dummy_start_task(self) -> BaseOperator:
        """创建dummy start task实例"""  
        dummy_start = DummyOperator(
            task_id='start',
            trigger_rule=TriggerRule.ALL_DONE
        )

        return dummy_start
    
    def _create_dummy_end_task(self) -> BaseOperator:
        """创建dummy end task实例"""
        dummy_end = PythonOperator(
            task_id='end',
            retries=0
        )

        return dummy_end

    def generate_dag_instance(self) -> DAG:
        """获取 job dag实例，主要步骤：
        1. 创建 job dag实例
        2. 遍历关联 task列表，逐一创建task group实例
        3. 遍历task dependencies列表，逐一创建 task之间依赖关系

        Returns:
            DAG: 返回1个 job dag实例.
        """
        # 创建dag实例
        with self._create_dag() as dag:
            # 创建 task任务实例
            for task in self.job_task_configs:
                locals()[f"var_{task['task_name']}"] = self._create_tasks(task)

            # 创建 task之间依赖关系
            if self.job_task_dependencies:
                for td in self.job_task_dependencies:
                    locals()[f"var_{td['upstream_task_name']}"].set_downstream(locals()[f"var_{td['downstream_task_name']}"])

                    for ti in locals()[f"var_{td['upstream_task_name']}"]:
                        for downstream_ti in locals()[f"var_{td['downstream_task_name']}"]:
                            ti >> downstream_ti
        return dag


# 调用 job配置解析类，获取 job配置
job_config_parser = JobParser()
job_configs = job_config_parser.parse()

# 遍历 job配置
if job_configs:
    for job_config in job_configs:
        try:
            # 调用 job dag工厂类，在全局命名空间逐一创建 job dag实例
            job_dag_factory = JobDagFactory(
                job_id=job_config["id"],
                job_name=job_config["job_name"],
                job_type=job_config["job_type"],
                job_params=job_config["job_params"],
                job_task_configs=job_config["job_task_configs"],
                job_task_dependencies=job_config["job_task_dependencies"],
            )
            globals()[f"var_{job_config['job_name']}"] = job_dag_factory.generate_dag_instance()
        except Exception as err:
            raise Exception(f"Failed to create  job dag\nJob id: {str(job_config)}")