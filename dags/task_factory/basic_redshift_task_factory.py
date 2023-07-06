import json
import logging
import re
import requests
import uuid
from typing import List
import redshift_connector

from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from config.conf import Config
from task_factory.base_task_factory import BaseTaskFactory

class BasicRedshiftTaskFactory(BaseTaskFactory):
    
    REDSHIFT_JOB_TIMEOUT=1200

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


    def execute_redshift_job(
        self,
        sql: str,
        **context
    ):
        """执行redshift query，主要步骤：
        - 获取redshift连接
        - 提交redshift query job

        Args:
            sql (str): _description_
        """    

        # 获取redshift连接

        redshift_client = redshift_connector.connect(
                host=Config.REDSHIFT_HOST,
                database='dev',
                port=Config.REDSHIFT_PORT,
                user=Config.REDSHIFT_USER,
                password=Config.REDSHIFT_PWD
            )

        # 提交redshift query job
        logging.info(f'Submit redshift query job...')


        try:
            # 获取redshift query job执行结果
            cursor = redshift_client.cursor()

            # Query a table using the Cursor
            cursor.execute(sql)

            if cursor.error_result:
                raise Exception(query_job.error_result)
        except Exception as err:
            logging.error(f'Query job failed due to %s' % str(err))
            context['ti'].xcom_push(key="error_reason", value=str(err))
            raise Exception(err)

        logging.info(
            f'''
            Query job has been completed:
            '''
        )

    def get_task_group_instance(self) -> TaskGroup:
        with TaskGroup(group_id=self.task_name) as tgi:
            task_execute_redshift_job = PythonOperator(
                task_id="execute_redshift_job",
                python_callable=self.execute_redshift_job,
                op_kwargs={
                    "sql": self.task_params["sql"]
                }
            )
            
        return tgi