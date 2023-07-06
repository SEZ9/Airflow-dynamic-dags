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

1. 创建dynamodb 表；

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
