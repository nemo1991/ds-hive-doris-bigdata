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


### 二、 方案实施全流程

#### 阶段 1：PostgreSQL -> Hive（数据同步至 ODS 贴源层）

把 PG 增量/全量数据抽取到 Hive，在 DolphinScheduler 中最常见的方式有两种：**SQL 节点（通过 Hive Catalog 外表直接查）** 或 **DataX/Sqoop 抽取**。这里推荐使用最通用、最稳健的 **DataX / Sqoop 任务节点**。

##### 1. 在 DolphinScheduler 的资源中心准备 DataX 模板 (`pg_2_hive.json`)
如果使用 DataX（DS 原生支持 DataX 任务类型）：

```json
{
  "job": {
    "setting": {
      "speed": { "channel": 3 }
    },
    "content": [
      {
        "reader": {
          "name": "postgresqlreader",
          "parameter": {
            "username": "postgres",
            "password": "your_pg_password",
            "column": ["id", "user_name", "amount", "create_time"],
            "connection": [
              {
                "querySql": [
                  "SELECT id, user_name, amount, create_time FROM public.orders WHERE create_time >= '${dt} 00:00:00' AND create_time <= '${dt} 23:59:59'"
                ],
                "jdbcUrl": ["jdbc:postgresql://<pg_host>:5432/your_db"]
              }
            ]
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "hdfs://<namenode>:8020",
            "fileType": "orc",
            "path": "/user/hive/warehouse/ods.db/ods_orders/dt=${dt}",
            "fileName": "ods_orders",
            "column": [
              { "name": "id", "type": "BIGINT" },
              { "name": "user_name", "type": "STRING" },
              { "name": "amount", "type": "DECIMAL" },
              { "name": "create_time", "type": "STRING" }
            ],
            "writeMode": "truncate"
          }
        }
      }
    ]
  }
}
```

##### 2. 创建并加载 Hive 分区表
在 Hive 中创建对应 ODS 层的映射表：
```sql
CREATE TABLE IF NOT EXISTS ods.ods_orders (
    id BIGINT,
    user_name STRING,
    amount DECIMAL(10, 2),
    create_time STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

---

#### 阶段 2：Hive 内部 ETL（ODS -> DWD/ADS 层清洗）

在 DolphinScheduler 中拖入一个 **SQL 节点**，用于按天清洗 Hive 数据并聚合出待导入 Doris 的指标表：

* **节点名称**：`hive_etl_dwd`
* **数据源**：选择类型为 `HIVE` 的数据源
* **执行 HQL 脚本**：
  ```sql
  -- 根据业务逻辑对 ODS 数据进行清洗与过滤，写到 ADS 聚合表
  INSERT OVERWRITE TABLE ads.ads_daily_user_sales PARTITION (dt = '${system.biz.date}')
  SELECT 
      user_name,
      SUM(amount) AS total_amount,
      COUNT(1) AS order_count
  FROM ods.ods_orders
  WHERE dt = '${system.biz.date}'
  GROUP BY user_name;
  ```

---

#### 阶段 3：Hive -> Apache Doris（数据灌入分析层）

从 Hive 向 Doris 同步数据，**最优雅、高性能且零额外开发** 的方式是直接利用 **Doris 的 Hive Catalog 功能**。

##### 步骤 A：在 Doris 中配置 Hive Catalog（仅需执行一次）
登录 Doris 的 MySQL 端口（9030），注册 Hive 目录：

```sql
CREATE CATALOG hive_catalog PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://<hive_metastore_host>:9083'
);
```

##### 步骤 B：在 Doris 中创建最终展示的目标表
```sql
CREATE DATABASE IF NOT EXISTS ads_db;

CREATE TABLE IF NOT EXISTS ads_db.ads_daily_user_sales (
    dt DATE,
    user_name VARCHAR(100),
    total_amount DECIMAL(12, 2),
    order_count BIGINT
)
ENGINE=OLAP
UNIQUE KEY(dt, user_name)
DISTRIBUTED BY HASH(user_name) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
```

##### 步骤 C：在 DolphinScheduler 中直接通过 SQL 驱动 Doris 拉取 Hive 数据
在 DolphinScheduler 中创建一个 **SQL 节点**，连接数据源为 **Doris**：

* **节点名称**：`sync_hive_to_doris`
* **数据源**：选择 `MYSQL` 或 `DORIS` 数据源 (指向 Doris FE 的 9030 端口)
* **执行 SQL**：
  ```sql
  -- 使用 Insert Into Select 直接跨引擎拉取 Hive 结果集并灌入 Doris 本地表
  INSERT INTO ads_db.ads_daily_user_sales
  SELECT 
      CAST(dt AS DATE) AS dt,
      user_name,
      total_amount,
      order_count
  FROM hive_catalog.ads.ads_daily_user_sales
  WHERE dt = '${system.biz.date}';
  ```

---

### 三、 在 DolphinScheduler 中编排全流程工作流 (DAG)

打开 DolphinScheduler 工作流设计器，按如下依赖树创建并连接节点：

```
     ┌────────────────────────┐
     │  1. DataX 节点 (PG->Hive)│ (抽取 PG 增量至 Hive ODS)
     └───────────┬────────────┘
                 │ (成功后)
                 ▼
     ┌────────────────────────┐
     │  2. SQL 节点 (Hive ETL) │ (清洗 ODS 生成 Hive ADS 结果)
     └───────────┬────────────┘
                 │ (成功后)
                 ▼
     ┌────────────────────────┐
     │ 3. SQL 节点 (Doris Push)│ (利用 Catalog 将 Hive 数据灌入 Doris)
     └────────────────────────┘
```

#### 工作流调度配置（Cron 定时）
1. **定义全局通用参数**：
   在工作流设置中增加自定义参数 `system.biz.date`（DolphinScheduler 内置的业务日期参数，默认为前一天 `${system.biz.date}`，即 `yyyy-MM-dd`）。
2. **设置定时 (Timing)**：
   * 触发周期：每天凌晨 `01:00:00` 自动运行。
   * 失败重试次数：设置 `3` 次，重试间隔 `5` 分钟。

---

### 四、 方案总结与核心优势

1. **逻辑解耦，链路清晰**：
   PostgreSQL 专注于 OLTP 事务；Hive 负责存放大容量历史数据湖并完成重度的 MapReduce/Tez/Spark 算力清洗；Doris 负责秒级响应前端报表与大屏查询。
2. **轻量高效的 Hive -> Doris 导数**：
   避免了编写复杂代码或借由第三方中转，利用 Doris 的 **Hive Catalog + Broker Load/Stream Load 内部引擎**，Doris 可以并行直接去 HDFS 抓取文件并高效写入本地，性能极佳。
3. **完全可调度的鲁棒性**：
   在 DolphinScheduler 控制台中可以清晰可视化查看节点血缘关系，一旦出现 PG 断连或 HDFS 空间不足，支持一键重试、断点续跑和邮件/钉钉告警。


# Q without A

- doris be/fe 分别是什么？关系？互相协作方式
- hive metastore/server2 同上

    典型的标准大数据架构中，`Hive Metastore (HMS)` 和 `HiveServer2 (HS2)` 是分工明确的两个服务：

    1. **Hive Metastore (HMS)：** 专门负责管理元数据（表结构、分区等），与 PostgreSQL 数据库直接打交道。**初始化 Schema 的工作由它（或专门的初始化任务）一次性完成即可**。
    2. **HiveServer2 (HS2)：** 专门负责接收用户的 SQL 查询（Beeline、JDBC/ODBC 连接），然后解析 SQL 并提交任务。
    HiveServer2 **不需要直接连接 PostgreSQL**，而是通过 Thrift 协议连接 `hive-metastore` 容器

- 注册 Doris BE 节点：将 BE 节点绑定到 FE。Doris 的 Backend 需要在 FE 中进行注册绑定才能工作：
```sql
ALTER SYSTEM ADD BACKEND "doris-be:9050";
```
- hiveserver2 /tmp目录权限设置
```bash
hadoop fs -chmod -R 777 /tmp/hive
```
- hive 根据IS_RESUME判断是否需要进行数据库初始化，初始化后需要将环境变量IS_RESUME设置为true
- 挂载hive-site.xml至对应的metastore/server2节点
- 新版本的 Apache Doris 镜像内置的入口启动脚本 (`init_fe.sh` / `init_be.sh`) 要求必须通过环境变量传入集群节点的地址配置 (`FE_SERVERS` 和 `BE_ADDR`)。
- 更新hive postgres驱动,挂载到hive容器  
``` bash
wget https://jdbc.postgresql.org/download/postgresql-42.6.2.jar
```
- hive 日志位于 `/tmp/hive/hive.log` 可以查看此文件分析hive2启动失败的原因
```bash
# 查看端口占用状态
netstat -tlpn
# 连接hive2
beeline -u "jdbc:hive2://localhost:10000" -n hive 
```

- Hive 3.x 默认启用了 Tez 执行引擎 (`hive.execution.engine=tez`)。在没有部署 Hadoop YARN 和 HDFS 的容器环境中，HiveServer2 在启动时会在后台无限循环尝试连接并初始化 Tez 会话池，导致服务永远无法完成加载

- Java 的 `java.net.URI` 严格遵循 RFC 域名规范：**主机名和网络域名中只允许使用字母、数字和连字符 `-`，绝不能使用下划线 `_`**

    1. **Docker 的 DNS 机制**：Docker Compose 默认使用**宿主机当前文件夹名称**（或 `.env` / `docker-compose.yml` 中的 `name`）作为项目名和网络域名，注入到容器内的 `/etc/resolv.conf` 中。如果你的项目文件夹叫 `ds-hive-doris_bigdata`（包含下划线 `_`），Docker 就会产生类似 `ds-hive-doris_bigdata-net` 的网络域。
    2. **Hive 源码的域名解析**：当 HiveServer2 拿着 `thrift://hive-metastore:9083` 去连接时，Java 底层调用了 `InetAddress.getCanonicalHostName()` 尝试获取完整主机名，结果被 Docker 补全成了 `hive-metastore.ds-hive-doris_bigdata-net`。
    3. **Java URI 校验拦截**：Java 的 `java.net.URI` 严格遵循 RFC 域名规范，校验时发现这个自动补全的域名中含有下划线 `_`，便直接抛出了 `URISyntaxException`。

- **HiveServer2 启动时默认开启了通知事件轮询（Notification Event Poll）**，它会向 Metastore 发送 `get_current_notificationEventId` 请求。但由于 `hive-metastore` 侧没有配置通知监听器（`DbNotificationListener`），Metastore 内部无法处理该请求并抛出异常，导致 HiveServer2 初始化中断并不断尝试重试。

```xml
    <!-- 禁用 HiveServer2 的 Notification Event 轮询，防止向 Metastore 查询通知事件时报错 -->
    <property>
      <name>hive.notification.event.poll.interval</name>
      <value>0ms</value>
    </property>
```