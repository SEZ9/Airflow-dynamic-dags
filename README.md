# 项目说明
通过Amazon Dynamodb记录管理任务内容及任务依赖关系。触发时间等，Amazon MWAA(airflow) 动态生成dags，以Amazon Redshift SQL 任务为示例。


# 目录结构
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

# 部署方式

1. 创建dynamodb 表，需要创建三张DynamoDB表格分别管理Scheduler、Job，Task，命名如下 mwaa_scheduler，mwaa_job，mwaa_task。其中表字段及说明如下。
- 表名：mwaa_scheduler
  
   分区键：schedule_id
属性字段：
job_list 字段类型：字符串；解释：关联的job id，示例：1,2
schedule_description 字段类型：字符串；解释：调度器的描述信息
schedule_job_configs 字段类型：字符串；解释：记录调度器的额外信息
schedule_job_dependencies 字段类型：字符串；解释：job间的依赖关系，示例：[{"job_id":"1","pid":"2"}]
schedule_name 字段类型：字符串；解释：调度器的名称，必须为英文
schedule_params 字段类型：字符串；解释：调度器配置参数，示例：{"schedule_interval":"0 0 12 * * "}
status 字段类型 数字；解释：调度器状态 1为启用 0 为禁用

- 表名：mwaa_job
  
   分区键：job_type
排序字段：job_id
属性字段：
job_name 字段类型：字符串；解释：作业名称
job_params 字段类型：字符串；解释：作业的额外参数
job_task_configs 字段类型：字符串；解释：关联task的额外参数
job_task_dependencies 字段类型：字符串；解释：Task间的依赖关系，示例：[{"task_id":"1","pid":"2"}]
status 字段类型 数字；解释：作业状态 1为启用 0 为禁用

- 表名：mwaa_task
  
   分区键：task_id
属性字段：
task_name 字段类型：字符串；解释：任务名称
task_params 字段类型：字符串；解释：任务参数，示例：{"sql":""}
task_type 字段类型：字符串；解释：任务类型

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
