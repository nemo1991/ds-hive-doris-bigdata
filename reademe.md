
# 部署注意事项
1. 注册 Doris BE 节点：将 BE 节点绑定到 FE。Doris 的 Backend 需要在 FE 中进行注册绑定才能工作：
ALTER SYSTEM ADD BACKEND "doris-be:9050";
2. hiveserver2 /tmp目录权限设置
3. hive 初始化后需要将环境变量IS_RESUME设置为true
4. 挂载hive-site.xml至对应的metastore/server2节点
5. hive-metastore 根据


# 组件清单
| 应用 | 访问方式 | 账号 | 密码 |
| ---- | ---- | ---- | ---- |
| Dolphin Scheduler | http://localhost:12345/dolphinscheduler/ui  | admin | dolphinscheduler123|
| Doris Fe Web UI | http://localhost:8030 | root | - | 
| Doris Fe Mysql | localhost:9030 | - | - |
| Doris Be Web UI | localhost:9010 | - |- |
| HiveServer2 | http://localhost:10002 | - | - |

# 目标

将 PostgreSQL (PG) 作为源头数据，通过 Hive 进行离线清洗与过渡，并由 DolphinScheduler (海豚调度) 统一编排，最终灌入 Apache Doris，是非常标准的典型数仓离线架构（T+1/按小时批量同步）。

这种架构中：
 - PostgreSQL：业务源数据库 (OLTP)   
 - Hive：数仓 ODS/DWD 贴源层和数据清洗层（存储历史明细）   
 - DolphinScheduler：作为 DAG 自动化工作流调度中心   
 - Doris：数仓 ADS/DWM 最终的 OLAP 极速查询与报表分析层  

```
[PostgreSQL] (源业务库)
    │
    │  1. DolphinScheduler 调度 DataX / Sqoop / Spark SQL 抽取
    ▼
[Hive - ODS层] (HDFS 数据湖/数仓贴源层存储)
    │
    │  2. DolphinScheduler 调度 Hive SQL / Spark SQL 进行 ETL 清洗
    ▼
[Hive - DWD/ADS层] (清洗后的宽表/指标表)
    │
    │  3. DolphinScheduler 调度 (Catalog 方式 或 Stream Load/DataX) 导入
    ▼
[Apache Doris] (OLAP 分析引擎，用于对外提供报表/API 查询)
```


# 操作步骤

## 1. 在 DolphinScheduler 中配置 Hive 与 Doris 数据源

登录 DolphinScheduler 控制台，进入 数据源中心 (Datasource Center) -> 创建数据源 (Create Datasource)

1. 配置 Hive 数据源  
数据源类型：HIVE  
数据源名称：hive_docker  
IP/主机名：hiveserver2 (Docker 网络下填内部主机名)  
端口：10000  
数据库名：default  
用户名/密码：hive / hive （或留空）  

2. 配置 Doris 数据源  
数据源类型：MYSQL 或 DORIS  
数据源名称：doris_docker  
IP/主机名：doris-fe  
端口：9030  
数据库名：information_schema 或您在 Doris 创建的数据库  
用户名/密码：root / (留空)  
