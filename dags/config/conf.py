from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# 统计的配置文件写在这里
class BaseConfig(object):
    AWS_DEFAULT_REGION = 'ap-southeast-1'
    
    TASK_FACTORY_MAPPING_DICT = {
        "REDSHIFT-SQL": "BasicRedshiftTaskFactory"
    }
    TASK_FACTORY_MODULE_PATH = "task_factory"
    ETL_SCHEDULE_TRIGGER_RULE = TriggerRule.ALL_DONE
 


class ProductionConfig(BaseConfig):
    REDSHIFT_REGION = 'us-east-1'
    Scheduler_DYNAMODB_TABLE = 'mwaa_scheduler'
    JOB_DYNAMODB_TABLE = 'mwaa_job'
    TASK_DYNAMODB_TABLE = 'mwaa_task'
    REDSHIFT_HOST = 'xxx.us-east-1.redshift-serverless.amazonaws.com'
    REDSHIFT_PORT = 5439
    REDSHIFT_USER = ''
    REDSHIFT_PWD = ''


class UatConfig(BaseConfig):
    REDSHIFT_REGION = 'us-east-1'
    Scheduler_DYNAMODB_TABLE = 'mwaa_scheduler'
    JOB_DYNAMODB_TABLE = 'mwaa_job'
    TASK_DYNAMODB_TABLE = 'mwaa_task'
    REDSHIFT_HOST = 'xxx.us-east-1.redshift-serverless.amazonaws.com'
    REDSHIFT_PORT = 5439
    REDSHIFT_USER = ''
    REDSHIFT_PWD = ''


    
class DevConfig(BaseConfig):
    REDSHIFT_REGION = 'us-east-1'
    Scheduler_DYNAMODB_TABLE = 'mwaa_scheduler'
    JOB_DYNAMODB_TABLE = 'mwaa_job'
    TASK_DYNAMODB_TABLE = 'mwaa_task'
    REDSHIFT_HOST = 'xxx.us-east-1.redshift-serverless.amazonaws.com'
    REDSHIFT_PORT = 5439
    REDSHIFT_USER = ''
    REDSHIFT_PWD = ''


if Variable.get('env') == 'Prod':
    Config = ProductionConfig
elif Variable.get('env') == 'Uat':
    Config = UatConfig
else:
    Config = DevConfig