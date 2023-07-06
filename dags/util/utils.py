"""
提供一些可复用的公共方法.
"""

import json
import logging
import os
import yaml
from datetime import datetime, timedelta
from typing import List

from airflow.models import XCom

def convert_list_to_str_with_parenthesis(data: List) -> str:
    """将一维list转换为带圆括号的字符串

    Args:
        data (List): 一个一维的列表.

    Returns:
        str: 带圆括号的字符串.
    """    
    data_str_with_parenthesis = str(data).replace("[", "(").replace("]", ")")
    
    return data_str_with_parenthesis

def read_yaml_from_path(yaml_path: str) -> dict:
    """从指定路径读取yaml文件内容

    Args:
        yaml_path (str): yaml文件的相对路径，默认当前路径为dags/目录.

    Returns:
        dict: 返回解析成字典格式的yaml文件内容
    """       
    # 获取dag目录绝对路径
    dir_path = os.path.dirname(os.path.abspath(__file__))
    dag_dir_path = os.path.dirname(dir_path)

    # 获取yaml文件绝对路径
    yaml_path = os.path.join(dag_dir_path, yaml_path)

    # 读取yaml文件
    with open(yaml_path, 'r') as f:
        yaml_dict = yaml.safe_load(f)
    
    return yaml_dict


def read_text_file_from_path(file_path: str) -> str:
    """从指定路径读取文件内容

    Args:
        file_path (str): 文件的相对路径，默认当前路径为dags/目录.

    Returns:
        str: 返回str格式的文件内容
    """    
    # 获取dag目录绝对路径
    dir_path = os.path.dirname(os.path.abspath(__file__))
    dag_dir_path = os.path.dirname(dir_path)

    # 获取text file绝对路径
    file_path = os.path.join(dag_dir_path, file_path)

    # 读取text file
    with open(file_path, 'r') as f:
        file_str = f.read()
    
    return file_str


def cleanup_xcom(context, session=None):
    dag_id = context.get('dag_run').dag_id
    execution_date = context.get('dag_run').execution_date
    session.query(XCom).filter(XCom.dag_id == dag_id, XCom.execution_date == execution_date).delete()


def safe_convert_json_string_to_dict(text: str) -> dict:
    if not text:
        return dict()
    
    return json.loads(text)


def safe_convert_comma_separated_string_to_list(text: str) -> list:
    if not text:
        return dict()
    
    return json.loads(f'[{text}]')

def get_current_date_str_hkt() -> str:
    current_datetime_hkt = datetime.utcnow() + timedelta(hours=8)

    return current_datetime_hkt.strftime("%Y-%m-%d")