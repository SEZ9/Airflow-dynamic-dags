"""Job解析类."""
import json
import logging
import re
import boto3

from typing import List, Dict, Any
from boto3.dynamodb.conditions import Attr
from config.conf import Config

from util.utils import (
    safe_convert_comma_separated_string_to_list,
    safe_convert_json_string_to_dict
)

class JobParser:
    """job数据库配置信息解析类.
    """
    @staticmethod
    def get_active_jobs() -> List[Dict[str, Any]]:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(Config.JOB_DYNAMODB_TABLE)
        """获取有效状态的job"""
        active_jobs = (
            table.scan(
                FilterExpression=Attr('status').eq(1)
            )["Items"]
        )

        return active_jobs

    @staticmethod
    def get_active_tasks() -> List[Dict[str, Any]]:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(Config.TASK_DYNAMODB_TABLE)
        """获取有效状态的task"""
        active_tasks = (
            table.scan(
                FilterExpression=Attr('status').eq(1)
            )["Items"]
        )

        return active_tasks

    def _get_task_by_id(self, task_id: int, task_lookup_list: list):
        """使用task id获取task"""
        matched_tasks = [
            task
            for task in task_lookup_list
            if str(task["task_id"]) == str(task_id)
        ]
        
        if not matched_tasks:
            raise Exception(f"Cannot find matched task for task id {task_id} ." + str(task_lookup_list))
        
        return matched_tasks[0]

    def _get_job_correlated_tasks(self, job_record, task_lookup_list: list) -> List:
        """获取job所关联的task列表"""
        correlated_task_ids = safe_convert_comma_separated_string_to_list(job_record["task_list"])

        if not correlated_task_ids:
            raise Exception(f"Error no correlated task lists found.")

        correlated_tasks = [
            self._get_task_by_id(task_id, task_lookup_list)
            for task_id in correlated_task_ids
        ]

        return correlated_tasks

    @staticmethod
    def parse_job_config(job_record) -> Dict[str, Any]:
        """解析job级别配置"""
        job_config = dict()
        job_config["id"] = job_record["job_id"]
        job_config["job_name"] = job_record["job_name"]
        job_config["job_type"] = job_record["job_type"]
        job_config["job_params"] = safe_convert_json_string_to_dict(job_record["job_params"])

        return job_config

    def parse_task_config(self, task_record) -> Dict[str, Any]:
        """解析task级别配置"""
        task_config = dict()

        task_config["task_id"] = task_record["task_id"]
        task_config["task_name"] = task_record["task_name"]
        task_config["task_type"] = task_record["task_type"]
        task_config["task_params"] = safe_convert_json_string_to_dict(task_record["task_params"])

        return task_config

    def validate_task_dependency(self, task_depencency: dict) -> bool:
        """校验task dependencies配置是否合法"""
        if not task_depencency["pid"]:
            return False

        if task_depencency["pid"] == task_depencency["task_id"]:
            return False

        return True

    def _parse_task_dependencies(self, task_dependency: dict, task_lookup_list: list) -> Dict[str, Any]:
        """解析task dependencies配置"""
        task_dependency_parsed = dict()

        task_dependency_parsed["upstream_task_id"] = task_dependency["pid"]
        task_dependency_parsed["downstream_task_id"] = task_dependency["task_id"]
        task_dependency_parsed["upstream_task_name"] = self._get_task_by_id(task_dependency["pid"], task_lookup_list)["task_name"]
        task_dependency_parsed["downstream_task_name"] = self._get_task_by_id(task_dependency["task_id"], task_lookup_list)["task_name"]
        
        return task_dependency_parsed

    def parse(self) -> List[Dict[str, Any]]:
        """获取数据库job配置信息，并解析为一个由字典构成的列表，主要步骤：
        1. 获取job有效列表
        2. 遍历job，逐一解析为字典格式
            - 解析job级别信息
            - 解析task级别信息
            - 解析task dependencies信息

        Returns:
            List[Dict[str, Any]]: 返回统一解析过的job配置信息.
        """
        # 获取有效job列表
        active_jobs = self.get_active_jobs()
        active_tasks = self.get_active_tasks()

        # 如果不存在有效job，则返回空
        if not active_jobs:
            return
        
        # 遍历job，逐一解析为字典格式
        job_config_list = list()
        for job in active_jobs:
            try:
                job_config = dict()

                # 解析job级别配置
                job_config.update(self.parse_job_config(job))

                # 解析task级别配置
                correlated_tasks = self._get_job_correlated_tasks(job, active_tasks)

                job_config["job_task_configs"] = [
                    self.parse_task_config(task)
                    for task in correlated_tasks
                ]

                # 解析task dependencies配置
                task_dependencies = safe_convert_json_string_to_dict(job["job_task_dependencies"])

                job_config["job_task_dependencies"] = [
                    self._parse_task_dependencies(td, active_tasks)
                    for td in task_dependencies
                    if self.validate_task_dependency(td)
                ]
                
                job_config_list.append(job_config)
            except Exception as err:
                raise Exception("Error while parsing job ")

        return job_config_list