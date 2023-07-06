"""Schedule解析类."""
import boto3
import json
import logging
import re

from boto3.dynamodb.conditions import Attr

from typing import List, Dict, Any

from config.conf import Config

from util.utils import (
    safe_convert_comma_separated_string_to_list,
    safe_convert_json_string_to_dict
)

from lib.job_parser import JobParser

class ScheduleParser:
    """ schedule数据库配置信息解析类.
    """
    @staticmethod
    def get_active_schedules() -> List[Dict[str, Any]]:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(Config.Scheduler_DYNAMODB_TABLE)
        """获取有效状态的 schedule"""
        active_schedules = (
            table.scan(
                FilterExpression=Attr('status').eq(1)
            )['Items']
        )

        return active_schedules

    def _get_job_by_id(self, job_id: int, job_lookup_list: list):
        """使用job id获取 job"""
        matched_jobs = [
            job
            for job in job_lookup_list
            if str(job["job_id"]) == str(job_id)
        ]
        
        if not matched_jobs:
            raise Exception(f"Cannot find matched job for job id {job_id}. Please turn the job online if it's currently offline.")
        
        return matched_jobs[0]

    def _get_schedule_correlated_jobs(self, schedule_record, job_lookup_list: list) -> List:
        """获取 schedule所关联的 job列表"""
        correlated_job_ids = safe_convert_comma_separated_string_to_list(schedule_record["job_list"])

        if not correlated_job_ids:
            raise Exception(f"Error no correlated job lists found.")

        correlated_jobs = [
            self._get_job_by_id(job_id, job_lookup_list)
            for job_id in correlated_job_ids
        ]

        return correlated_jobs

    def parse_schedule_config(self, schedule_record) -> Dict[str, Any]:
        """解析 schedule级别配置"""
        schedule_config = dict()

        schedule_config["schedule_id"] = schedule_record["schedule_id"]
        schedule_config["schedule_name"] = schedule_record["schedule_name"]
        schedule_config["schedule_description"] = schedule_record["schedule_description"]
        schedule_config["schedule_params"] = safe_convert_json_string_to_dict(schedule_record["schedule_params"])

        return schedule_config

    def validate_job_dependency(self, job_dependency: dict) -> bool:
        """校验job dependencies配置是否合法"""
        if not job_dependency["pid"]:
            return False

        if job_dependency["pid"] == job_dependency["job_id"]:
            return False

        return True

    def _parse_job_dependencies(self, job_dependency: dict, job_lookup_list: list) -> Dict[str, Any]:
        """解析job dependencies配置"""
        job_dependency_parsed = dict()

        job_dependency_parsed["upstream_job_id"] = job_dependency["pid"]
        job_dependency_parsed["downstream_job_id"] = job_dependency["job_id"]
        job_dependency_parsed["upstream_job_name"] = self._get_job_by_id(job_dependency["pid"], job_lookup_list)["job_name"]
        job_dependency_parsed["downstream_job_name"] = self._get_job_by_id(job_dependency["job_id"], job_lookup_list)["job_name"]
        
        return job_dependency_parsed

    def parse(self) -> List[Dict[str, Any]]:
        """获取数据库 schedule配置信息，并解析为一个由字典构成的列表，主要步骤：
        1. 获取 schedule有效列表
        2. 遍历 schedule，逐一解析为字典格式
            - 解析schedule级别信息
            - 解析job级别信息
            - 解析job dependencies信息

        Returns:
            List[Dict[str, Any]]: 返回统一解析过的 job配置信息.
        """
        # 获取有效 schedule列表
        active_schedules = self.get_active_schedules()
        active_jobs = JobParser.get_active_jobs()

        # 如果不存在有效 schedule，则返回空
        if not active_schedules:
            return

        # 遍历 schedule，逐一解析为字典格式
        schedule_config_list = list()
        for schedule in active_schedules:
            try:
                schedule_config = dict()

                # 解析schedule级别配置
                schedule_config.update(self.parse_schedule_config(schedule))

                # 解析job级别配置
                correlated_jobs = self._get_schedule_correlated_jobs(schedule, active_jobs)

                schedule_config["schedule_job_configs"] = [
                    JobParser.parse_job_config(job)
                    for job in correlated_jobs
                ]

                # 解析job dependencies配置
                job_dependencies = safe_convert_json_string_to_dict(schedule["schedule_job_dependencies"])

                schedule_config["schedule_job_dependencies"] = [
                    self._parse_job_dependencies(jd, active_jobs)
                    for jd in job_dependencies
                    if self.validate_job_dependency(jd)
                ]

                schedule_config_list.append(schedule_config)
            except Exception as err:
                raise Exception(f"Error while parsing  schedule ")

        return schedule_config_list