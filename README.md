# EN
#### Project description
Apache Airflow is an open-source tool used for writing, scheduling, and monitoring workflows, which are sequences of processes and tasks, programmatically. With Amazon MWAA, you can create workflows using Apache Airflow and Python without the need to manage the underlying infrastructure for scalability, availability, and security. Amazon MWAA automatically scales its workflow execution capability to meet your needs. It integrates with AWS security services to help you securely access data quickly.

In this project, Amazon DynamoDB is used to record and manage task content, task dependencies, and edit trigger times. Basic DAG code has been written to dynamically generate DAGs using Amazon MWAA (Airflow). Additionally, reference code is provided with an example of an Amazon Redshift SQL task.

#### Overall architecture：

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/9725fd76-e014-4c21-8e1d-7996f4650d0f">

#### Explanation of task and DAG organization

A specific data processing task is referred to as a "Task," such as executing a Redshift SQL query, processing a batch task using Spark, or running a Python script. This solution provides a basic Task template class and offers example code for Redshift SQL tasks to assist users in writing generic Task methods. In this solution, a Job DAG consists of multiple Tasks and can organize their task dependencies. To accommodate more complex scenarios, a custom Scheduler DAG has been added. It consists of multiple Jobs, and the Scheduler DAG sets the timing trigger strategy as the main DAG for job execution. The specific jobs are triggered in sequence based on their dependencies. With this design, businesses can achieve tasks such as concurrently processing data for multiple distributed business modules before performing aggregate task processing.

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/37803f14-3a20-4a1c-ba47-b60b277f2cd2">

## Project structure

```
.
├──README.md                         
├──requirements.txt                      # Python dependencies required for MWAA environment
├──dags/                                 # Directory for MWAA environment DAG code
│  ├── .airflowignore/                   # Files excluded from the Airflow DAG parsing process
│  ├── config/                           # Common Configuration files
│  ├── util/                             # Reusable utility methods
│  ├── lib/                              # Miscellaneous classes
│      ├── job_parser.py/                # DAG configuration parsing class
│      ├── schedule_parser.py/           # Schedule configuration parsing class
│  ├── task_factory/                     # Task factory class
│      ├── job_dag.py                    # Job DAG file
│      ├── schedule_dag.py               # Schedule DAG file
```

## Deployment
1. Create DynamoDB tables. Three DynamoDB tables need to be created to manage Scheduler, Job, and Task. Name them as follows: mwaa_scheduler, mwaa_job, mwaa_task. The table fields and descriptions are as follows:

#### Table Name: mwaa_scheduler
```
Partition Key: schedule_id
Attribute Fields:
job_list (String): Associated job IDs. Example: 1,2
schedule_description (String): Description of the scheduler
schedule_job_configs (String): Additional information about the scheduler
schedule_job_dependencies (String): Dependencies between jobs. Example: [{"job_id":"1","pid":"2"}]
schedule_name (String): Name of the scheduler (must be in English)
schedule_params (String): Configuration parameters for the scheduler. Example: {"schedule_interval":"0 0 12 * * "}
status (Number): Scheduler status. 1 for enabled, 0 for disabled
```
#### Table Name: mwaa_job

```
Partition Key: job_type
Sort Key: job_id
Attribute Fields:
job_name (String): Job name
job_params (String): Additional parameters for the job
job_task_configs (String): Additional parameters for associated tasks
job_task_dependencies (String): Dependencies between tasks. Example: [{"task_id":"1","pid":"2"}]
status (Number): Job status. 1 for enabled, 0 for disabled
```
#### Table Name: mwaa_task
```
Partition Key: task_id
Attribute Fields:
task_name (String): Task name
task_params (String): Task parameters. Example: {"sql":""}
task_type (String): Task type
```
2. Create MWAA cluster & Upload code to S3

https://docs.aws.amazon.com/zh_cn/mwaa/latest/userguide/get-started.html



3. Specify MWAA cluster parameters


 <img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/336b5150-a3a2-44c9-a1e3-d6d874774059">
 

4. Editing tasks on DynamoDB

- Task

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/2a0c3f5f-89af-4d69-b558-3be1d6ec2f1a">

- Job

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/953525b7-61a7-46ad-97e7-2e0465844304">

- Scheduler

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/7165e11c-6df5-4e75-acb6-47ef3093c97e">


5. Viewing the automatically generated DAG on the Airflow UI

- DAG
   
<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/77b68c6e-b004-4beb-b319-eeb6709b565e">

- dependency

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/7bf785f7-f831-437d-9f74-1199df92d3ff">


#### Other recommendations:

1. It is advisable to implement task alerting mechanisms. This can help notify relevant stakeholders or team members in case of task failures, delays, or any other critical events. Implementing alerts ensures timely awareness and enables prompt actions to resolve issues.

2. It is recommended to enable detailed logging for tasks. By capturing comprehensive execution logs, you can gain better visibility into task executions, troubleshoot any issues, and track the progress of tasks over time. Alternatively, you can consider storing the execution details, such as task outputs or results, in a database table for easy access and analysis.





# 中文
#### 项目说明
通过Amazon Dynamodb记录管理任务内容及任务依赖关系。触发时间等，Amazon MWAA(airflow) 动态生成dags，以Amazon Redshift SQL 任务为示例。

<img width="416" alt="image" src="https://github.com/SEZ9/Airflow-dynamic-dags/assets/20342912/9725fd76-e014-4c21-8e1d-7996f4650d0f">


#### 目录结构
```
.
├──README.md                         
├──requirements.txt                      # mwaa环境所需的python依赖文件
├──dags/                                 # mwaa环境dag代码目录
│  ├── .airflowignore/                   # 不进入airflow dag解析流程的文件列表
│  ├── config/                           # 配置文件
│  ├── util/                             # 可复用的公共方法
│  ├── lib/                              # 一些其他的类
│      ├── job_parser.py/                #  dag配置解析类
│      ├── schedule_parser.py/           #  schdule配置解析类
│  ├── task_factory/                     #  task工厂类
│      ├── job_dag.py                    #  job dag文件
│      ├── schedule_dag.py               #  schedule dag文件
```

#### 部署方式

1. 创建dynamodb 表，需要创建三张DynamoDB表格分别管理Scheduler、Job，Task，命名如下 mwaa_scheduler，mwaa_job，mwaa_task。其中表字段及说明如下。
- 表名：mwaa_scheduler
```
分区键：schedule_id
属性字段：
job_list 字段类型：字符串；解释：关联的job id，示例：1,2
schedule_description 字段类型：字符串；解释：调度器的描述信息
schedule_job_configs 字段类型：字符串；解释：记录调度器的额外信息
schedule_job_dependencies 字段类型：字符串；解释：job间的依赖关系，示例：[{"job_id":"1","pid":"2"}]
schedule_name 字段类型：字符串；解释：调度器的名称，必须为英文
schedule_params 字段类型：字符串；解释：调度器配置参数，示例：{"schedule_interval":"0 0 12 * * "}
status 字段类型 数字；解释：调度器状态 1为启用 0 为禁用
```
- 表名：mwaa_job
 ``` 
   分区键：job_type
排序字段：job_id
属性字段：
job_name 字段类型：字符串；解释：作业名称
job_params 字段类型：字符串；解释：作业的额外参数
job_task_configs 字段类型：字符串；解释：关联task的额外参数
job_task_dependencies 字段类型：字符串；解释：Task间的依赖关系，示例：[{"task_id":"1","pid":"2"}]
status 字段类型 数字；解释：作业状态 1为启用 0 为禁用
```
- 表名：mwaa_task
  ```
   分区键：task_id
属性字段：
task_name 字段类型：字符串；解释：任务名称
task_params 字段类型：字符串；解释：任务参数，示例：{"sql":""}
task_type 字段类型：字符串；解释：任务类型
```
2. 创建mwaa集群
   1. 上传代码至S3；
   2. 指定mwaa集群参数；
   3. 创建mwaa集群；
   4. 编辑任务
3. 开发指南
4. 监控告警集成

# 生产环境建议
1. 记录任务级别数据埋点
2. 增加运行失败回调
