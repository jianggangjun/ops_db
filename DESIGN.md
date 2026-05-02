# ops_db 数据库运维脚本设计方案

## 一、总体架构

```
ops_db/
├── ops_db.py              # 主入口，CLI 分发
├── config/
│   └── my.cnf.j2          # MySQL 配置 Jinja2 模板
├── modules/               # 各功能模块
│   ├── __init__.py
│   ├── install.py         # MySQL 安装
│   ├── backup.py          # 备份（xtrabackup / mydumper）
│   ├── restore.py         # 恢复（全量 / PITR / partial / binlog-replay）
│   ├── replicate.py       # 主从配置
│   ├── rebuild.py          # 备库重搭
│   └── check.py           # 健康检查
├── lib/                   # 公共库
│   ├── __init__.py
│   ├── checker.py         # 前置检查（磁盘/依赖/端口/权限）
│   ├── config_gen.py      # my.cnf 渲染
│   ├── logger.py          # 日志（统一格式 + 写文件）
│   ├── mysql_conn.py      # MySQL 连接封装（PyMySQL）
│   ├── ssh_client.py      # SSH 远程执行（Paramiko）
│   └── system_detect.py   # 系统 / OS 版本检测
├── scripts/               # 辅助脚本
│   └── get_mysql_version.sh
└── requirements.txt
```

**执行模式：本地 + SSH 远程双模式**

```
模式 A：本地执行
  python3 ops_db.py install --version 8.0
  → 直接在当前机器执行

模式 B：SSH 远程执行
  python3 ops_db.py install --version 8.0 \\
      --ssh-host 192.168.1.10 --ssh-user root --ssh-key ~/.ssh/id_rsa
  → 将 ops_db 打包后通过 stdin 上传至远程主机，解压后执行

SSH 认证优先级：--ssh-key > --ssh-password > 环境变量 SSH_PASSWORD > 默认密钥 ~/.ssh/id_rsa
```

---

## 二、场景与功能设计（共 6 个场景）

### 场景 1：MySQL 服务安装 — `ops_db install`

**触发方式**

```bash
ops_db.py install --version 5.7 --type master  # 交互式可省略
```

**流程设计**

```
1. 系统探测
   ├─ 检测 OS 类型 + 版本
   │   ├─ CentOS 7     → MariaDB 5.5/10.x 或 MySQL 5.7
   │   ├─ CentOS 8/9   → MySQL 8.0
   │   ├─ Ubuntu 20.04 → MySQL 8.0
   │   ├─ Ubuntu 22.04 → MySQL 8.0 / 8.4
   │   ├─ Ubuntu 24.04 → MySQL 8.4 / 9.0
   │   └─ 其他发行版   → 友好提示暂不支持，建议手动安装
   ├─ 检测是否已安装 MySQL（rpm -qa / dpkg -l）
   └─ 检测端口 3306 是否被占用

2. 前置检查
   ├─ 磁盘空间（data目录 + log目录各预留 20% 余量）
   ├─ 依赖包（xtrabackup、mysql client、rsync等）
   └─ 权限（必须 root 或有 sudo）

3. 安装步骤
   ├─ 添加官方 YUM/Apt 源（可选，优先用系统自带源）
   ├─ 安装 MySQL server + client
   ├─ 安装 xtrabackup（与 MySQL 版本匹配）
   │   ├─ MySQL 5.7 → xtrabackup 2.4
   │   └─ MySQL 8.x → xtrabackup 8.0
   └─ 生成随机 root 密码（启动后首次登录强制修改）

4. 配置生成
   ├─ 渲染 my.cnf.j2 模板（port、datadir、log-bin、server-id等）
   ├─ 初始化 data 目录
   └─ 启动服务

5. 验证
   ├─ mysql -e "SELECT VERSION()"
   └─ systemctl status mysqld/mysql
```

**用户交互设计**

```
# 完全交互模式（无任何参数）
$ ops_db.py install
请选择安装类型 [1] 单机 [2] 主库 [3] 备库: 2
请选择版本（当前系统推荐: MySQL 8.0）: [默认 8.0 直接回车]
安装完成后会生成临时 root 密码，请尽快登录修改

# 非交互模式（供自动化调用）
$ ops_db.py install --version 8.0 --type master --port 3306 --password xxx
```

**版本推荐规则（config 中维护版本矩阵）**

| OS | 版本 | 推荐 MySQL | XtraBackup | 推荐镜像源 |
|---|---|---|---|---|
| CentOS 7 | 7.x | MySQL 5.7 | 2.4 | 腾讯云/清华 |
| CentOS 8 | 8.x | MySQL 8.0 | 8.0 | 腾讯云/清华 |
| Rocky 8/9 | 8.x/9.x | MySQL 8.0 / 8.4 | 8.2 | 腾讯云/清华 |
| Ubuntu 20.04 | 20.04 | MySQL 8.0 | 8.0 | 系统 apt |
| Ubuntu 22.04 | 22.04 | MySQL 8.0 / 8.4 | 8.2 | 系统 apt |
| Ubuntu 24.04 | 24.04 | MySQL 8.4 / 9.0 | 8.2/9.0 | 系统 apt |
| Debian 11/12 | 11/12 | MySQL 8.0 | 8.0 | 系统 apt |
| AlmaLinux 8/9 | 8/9 | MySQL 8.0 / 8.4 | 8.2 | 腾讯云/清华 |

**XtraBackup 与 MySQL 版本对应**

| XtraBackup | MySQL |
|---|---|
| 2.4 | 5.6 / 5.7 / MariaDB 10.x |
| 8.0 | 8.0.x |
| **8.2** | **8.0.x / 8.4.x** ✅ |
| 9.0 | 8.0.x / 8.4.x / 9.0.x |

**国内镜像源（`--mirror` 参数）**

| 值 | 名称 | MySQL Yum | Percona(XtraBackup) |
|---|---|---|---|
| `tencent` | 腾讯云（默认） | ✅ | ✅ |
| `aliyun` | 阿里云 | ✅ | ✅（海外） |
| `tsinghua` | 清华镜像 | ✅ | ✅ |
| `official` | 官方（海外） | ✅ | ✅ |

---

### 场景 2：MySQL 备份 — `ops_db backup`

**触发方式**

```bash
# 全量备份
ops_db.py backup --type full --host 192.168.1.10 --port 3306

# 增量备份（依赖已有全量备份）
ops_db.py backup --type incr --host 192.168.1.10 --port 3306

# 逻辑备份（mysqldump）
ops_db.py backup --type dump --host 192.168.1.10 --port 3306 --databases myapp
```

**备份策略设计**

```
备份方式选择：
├─ full（全量）    → xtrabackup --backup --parallel N --target-dir=/path/to/backup
├─ incr（增量）    → xtrabackup --backup --incremental-basedir=/path/to/base --target-dir=/path/to/incr
│                   （xtrabackup 8.0 无 --incremental 标志，基于上一次 full 或 incr）
└─ dump（逻辑）    → mysqldump --single-transaction --master-data=2

备份流程：
1. 前置检查
   ├─ 磁盘空间（备份目标分区剩余空间 > 1.5倍数据目录大小）
   ├─ xtrabackup / mysqldump 是否安装
   ├─ MySQL 连接是否正常（root 或有备份权限的账户）
   └─ 备份账户权限检查（RELOAD, LOCK TABLES, REPLICATION CLIENT）

2. 执行备份
   ├─ 创建备份目录（带时间戳：backup_YYYYMMDD_HHMMSS）
   ├─ 执行备份命令
   └─ 备份完成 --prepare（xtrabackup 需要 prepare 才能用于恢复）

3. 备份验证（新增）
   ├─ xtrabackup → xtrabackup --export --dry-run（检查完整性）
   └─ mysqldump  → grep -c "DROP TABLE / CREATE TABLE" 或行数统计

4. 记录元数据
   ├─ 备份文件路径
   ├─ 备份类型（full/incr/dump）
   ├─ 备份时间
   ├─ binlog position（show master status）
   ├─ GTID 信息（如果开启）
   └─ 数据目录大小

5. 过期清理（可选参数 --expire-days）
   └─ 保留最近 N 天的备份（按时间戳目录清理）
```

**备份保留策略**

```bash
# 保留最近 7 天全量 + 每天增量
ops_db.py backup --type full --expire-days 7 --incr-window 1

# 默认保留策略（可通过配置文件设置默认值）
# full: 保留最近 7 个
# incr: 保留最近 30 个
```

**多实例支持**

```bash
# 指定 socket 或端口，区分不同实例
ops_db.py backup --type full --port 3307 --socket /tmp/mysql_3307.sock
```

---

### 场景 3：主从配置 — `ops_db replicate`

**触发方式**

```bash
ops_db.py replicate --master-host 192.168.1.10 --master-port 3306 \
                     --slave-host 192.168.1.11 --slave-port 3306
```

**流程设计**

```
1. 前置检查（两台机器同时检查）
   ├─ 两端 MySQL 版本兼容性（主版本最好一致）
   ├─ 两端 server-id 是否唯一（不允许重复）
   ├─ 两端 port 是否可访问（telnet/nc 检测）
   ├─ 主库是否有数据（show tables / select count(*)）
   │   → 有数据时：WARNING 提示先备份再继续
   │   → 用户确认后才继续，防止数据覆盖
   └─ 备库磁盘空间是否足够

2. 主库配置（如果需要开启 binlog）
   ├─ 检查 log_bin 是否已开启
   └─ 如果未开启，帮用户生成需要追加的 my.cnf 配置项

3. 获取主库复制坐标
   ├─ FLUSH TABLES WITH READ LOCK
   ├─ SHOW MASTER STATUS  → 记录 File + Position
   ├─ SHOW MASTER STATUS  → GTID 模式时获取 GTID 集合
   └─ UNLOCK TABLES

4. 备库配置
   ├─ 修改 server-id（如果与主库相同则报错）
   ├─ 配置 relay-log
   └─ 如果备库有数据，再次确认是否清空

5. 初始化备库复制
   ├─ CHANGE MASTER TO
   │   MASTER_HOST='xxx'
   │   MASTER_LOG_FILE='xxx'
   │   MASTER_LOG_POS=xxx
   │   MASTER_USER='repl'
   │   MASTER_PASSWORD='xxx'
   │   （或 GTID 模式：MASTER_AUTO_POSITION=1）
   └─ START SLAVE

6. 验证
   ├─ SHOW SLAVE STATUS\G
   │   ├─ Slave_IO_Running: Yes
   │   ├─ Slave_SQL_Running: Yes
   │   └─ Seconds_Behind_Master: N
   └─ 延时大于 0 时给出 WARNING 提示

7. 复制账户（新增）
   └─ 自动在主库创建 repl 用户（如果不存在）
```

**关键提示**

```
⚠️ 检测到主库存在数据，继续操作会覆盖备库数据！
请选择：
  [1] 继续（先备份主库，再配置主从）
  [2] 退出（手动处理）
  [3] 仅导出主库数据到备库（mysqldump，不清空备库现有数据）

输入选项 [1/2/3]:
```

---

### 场景 4：备库重搭 — `ops_db rebuild`

这是最复杂的场景，区分两种子场景：

#### 4a. 从库延时过大 — `ops_db rebuild --reason lag`

```
条件：Seconds_Behind_Master > 300（5分钟），且 IO/SQL 线程正常

触发流程：
1. 警告提示（延时 X 分钟，建议重搭）
2. 检查主库 binlog 是否还在（如果主库已经 purge 了旧 binlog，则无法通过偏移量重建，只能用 GTID）
3. 提示用户选择重搭方式
   ├─ [1] 基于当前位置重建（SHOW SLAVE STATUS 记录的点位）
   └─ [2] 全量重搭（备份主库 → 清空备库 → 恢复 → 重建复制）
4. 选择 [1] 直接从当前位置继续复制（不用重建）
5. 选择 [2] 执行全量重搭流程（见 4c）
```

#### 4b. 备库故障（新机器 / 备库完全不可用）

```
条件：备库所在机器无法连接，或 MySQL 服务不可恢复

触发流程：
1. 确认新机器环境（OS、磁盘、网络）
2. 在新机器上安装 MySQL（调用 install 模块）
3. 执行全量重搭流程（见 4c）
```

#### 4c. 通用全量重搭流程（4a[2] 和 4b 共用）

```
1. 告知用户将执行的操作
   - 备库数据将被清空
   - 预计耗时（基于数据量估算）

2. 对主库做全量备份
   $ xtrabackup --user=root --password=xxx --backup --target-dir=/backup/path
   备份时间戳：XXXXX

3. 备份验证
   $ xtrabackup --prepare --target-dir=/backup/path/XXXXX
   确认无报错

4. 停止备库 MySQL

5. 备份备库现有数据（safety net）
   $ mv /var/lib/mysql /var/lib/mysql_old_$(date +%Y%m%d%H%M%S)

6. 在备库上恢复数据
   $ xtrabackup --copy-back /backup/path/XXXXX
   $ chown -R mysql:mysql /var/lib/mysql

7. 启动备库 MySQL

8. 获取新的复制点位
   - GTID 模式：从备份目录的 xtrabackup_binlog_info 读取 GTID 集合
   - 传统模式：从 xtrabackup_binlog_info 读取 File + Position

9. 重建复制链路
   CHANGE MASTER TO MASTER_AUTO_POSITION=1（GTID）
   或
   CHANGE MASTER TO MASTER_LOG_FILE='xxx', MASTER_LOG_POS=xxx（传统）

10. 启动复制
    START SLAVE

11. 验证
    SHOW SLAVE STATUS\G

12. 清理
    rm -rf /var/lib/mysql_old_*
```

---

## 三、公共模块设计（lib/）

### 3.1 前置检查（checker.py）

每个场景执行前统一调用，输出结构化报告：

```python
class CheckResult(NamedTuple):
    item: str       # 检查项名称
    status: str     # PASS / WARN / FAIL
    message: str    # 详细说明
    suggestion: str # 修复建议（FAIL 时提供）

def check_disk_space(path: str, required_gb: float) -> CheckResult
def check_port_available(port: int) -> CheckResult
def check_mysql_running(port: int) -> CheckResult
def check_xtrabackup_version() -> CheckResult
def check_server_id_unique(host: str, port: int, server_id: int) -> CheckResult
def run_preflight_checks(actions: list[str]) -> list[CheckResult]
```

### 3.2 配置生成（config_gen.py）

```python
def render_my_cnf(
    port: int = 3306,
    datadir: str = "/var/lib/mysql",
    logdir: str = "/var/log/mysql",
    server_id: int,
    binlog: bool = True,
    gtid_mode: bool = False,
    role: str = "master",  # master / slave
) -> str:
    """渲染 my.cnf Jinja2 模板，返回配置内容"""
```

### 3.3 日志（logger.py）

```python
def get_logger(name: str) -> logging.Logger:
    """返回带文件输出的 logger"""

# 日志格式：2026-04-29 10:00:00 [REPLICATE] [INFO] 主库连接成功
# 日志文件：~/.ops_db/logs/ops_db_YYYYMMDD.log
```

### 3.4 MySQL 连接封装（mysql_conn.py）

```python
import pymysql
from contextlib import contextmanager

@contextmanager
def get_conn(host: str, port: int, user: str, password: str, charset: str = "utf8mb4"):
    """上下文管理器，自动 commit/rollback """

def get_version(conn) -> str
def get_server_id(conn) -> int
def get_master_status(conn) -> dict  # {'file': 'xxx', 'position': 1234, 'gtid': 'xxx'}
def get_slave_status(conn) -> dict    # {'io_running': 'Yes', 'sql_running': 'Yes', 'lag': 0}
def create_repl_user(conn, user: str, password: str, host: str) -> None
def table_count(conn, database: str) -> int
```

### 3.5 系统探测（system_detect.py）

```python
def detect_os() -> dict:
    """
    返回：
    {
        'os': 'centos7' | 'ubuntu22' | 'debian12' | ...,
        'family': 'rhel' | 'debian',
        'version': '7.9' | '22.04' | ...,
        'arch': 'x86_64' | 'aarch64'
    }
    """

MYSQL_VERSION_MAP = {
    'centos7': {'default': '5.7', 'xtrabackup': '2.4'},
    'centos8': {'default': '8.0', 'xtrabackup': '8.0'},
    'ubuntu22': {'default': '8.0', 'xtrabackup': '8.0'},
    'ubuntu24': {'default': '8.4', 'xtrabackup': '8.2'},
    'debian12': {'default': '8.0', 'xtrabackup': '8.0'},
}
```

---

### 场景 5：恢复流程 — `ops_db restore`

**触发方式**

```bash
# 全量恢复
ops_db.py restore --type full --backup-dir /backup/full_20260429_100000 --host 192.168.1.11

# 指定时间点恢复（point-in-time，PITR）
ops_db.py restore --type pitr \
    --backup-dir /backup/full_20260429_100000 \
    --host 192.168.1.11 \
    --stop-datetime "2026-04-29 15:00:00" \
    --binlog-dir /var/lib/mysql/binlog

# 恢复指定库（只恢复某个数据库）
ops_db.py restore --type partial \
    --backup-dir /backup/full_20260429_100000 \
    --databases myapp \
    --host 192.168.1.11
```

**前置条件**

```
恢复前必须满足：
1. MySQL 服务已停止（restore 过程不能有写入）
2. 原 data 目录已备份（safety backup）
3. 备份目录可读（权限检查）
4. 磁盘空间充足（解压后数据大小 + 额外 20% 余量）
5. 如果是 PITR，需要主库的完整 binlog 文件链
```

**完整恢复流程（全量 + prepare）**

```
1. 确认恢复目标
   ├─ 目标机器：哪个 MySQL 实例
   ├─ 恢复模式：full / pitr / partial
   └─ 目标实例当前数据（提醒将覆盖）

2. Safety Backup（强制，对应备份里的 safety net）
   └─ 快照或 mv 当前 data 目录到 backup_old_YYYYMMDDHHMMSS

3. 停止 MySQL
   $ systemctl stop mysqld

4. 清空 data 目录（保留 mysql 系统库单独处理）
   $ rm -rf /var/lib/mysql/*
   （注意：有些场景只清空某个库，partial 模式见下方）

5. 执行 --prepare（xtrabackup 备份的 prepare）
   $ xtrabackup --prepare [--export] --target-dir=/backup/full_20260429_100000
   - 回放已提交的事务 + 回滚未提交的事务
   - PITR 模式在此步传入 --binlog 参数

6. 执行 --copy-back
   $ xtrabackup --copy-back --target-dir=/backup/full_20260429_100000
   $ chown -R mysql:mysql /var/lib/mysql

7. 启动 MySQL
   $ systemctl start mysqld

8. 验证
   ├─ mysql -e "SELECT COUNT(*) FROM myapp.xxx"
   ├─ 与备份时的数据量做对比（如果有记录）
   └─ 如果是 PITR，验证时间点前后数据是否正确

9. 清理
   └─ rm -rf /var/lib/mysql_old_*（确认恢复成功后）
```

**PITR 时间点恢复（增量恢复最核心场景）**

```
场景：今天 15:00 误删了一个表，需要恢复到 14:55 的状态

前置条件：
- 有一个全量备份（全量备份时间点：昨天 00:00）
- 全量备份之后到 14:55 之间的所有 binlog 文件都在

操作步骤：

1. 准备全量备份
   $ xtrabackup --prepare \
       --binlog-dir /var/lib/mysql/binlog \
       --target-dir /backup/full_20260428_000000

   --binlog-dir 指定 binlog 文件目录
   --to-latest   应用 binlog 直到最新（可用 --stop-datetime 停在指定时间）

   等价于手动：
   $ xtrabackup --prepare --binlog-dir /path/to/binlog \
       --stop-datetime="2026-04-29 14:55:00" \
       --target-dir=/backup/full_20260428_000000

2. copy-back 恢复（与全量恢复相同）

3. 验证数据

补充：binlog 时间与服务器时间可能不一致
- 用 --binlog-position 指定精准的 binlog position 定点
- 也可以用 mysqlbinlog --start-datetime 找到对应时间点的 position
```

**部分恢复（只恢复某个库）**

```
场景：某个库数据损坏，不需要恢复整个实例，只恢复该库

1. 对全量备份做 prepare（--export 模式）
   $ xtrabackup --prepare --export --target-dir=/backup/full_20260429_100000

2. 删除目标库（只删要恢复的库）
   $ mysql -e "DROP DATABASE myapp"

3. discard 掉表空间（如果是 innodb_file_per_table）
   $ mysql myapp -e "ALTER TABLE t1 DISCARD TABLESPACE"

4. 从备份目录 cp 对应的 .ibd 文件
   $ cp /backup/full_20260429_100000/myapp/*.ibd /var/lib/mysql/myapp/

5. import 表空间
   $ mysql myapp -e "ALTER TABLE t1 IMPORT TABLESPACE"

6. 重复步骤 4-5 直到所有表恢复完成
```

**误删除恢复锦囊（额外的辅助功能）**

```
场景：没有备份，但数据被 DELETE/DROP 了
前提：MySQL 开启了 binlog

$ ops_db.py restore --type binlog-replay \
    --host 192.168.1.10 \
    --binlog-file "mysql-bin.000123" \
    --start-position 1234 \
    --stop-position 5678 \
    --database myapp

等价于：
$ mysqlbinlog --start-position=1234 --stop-position=5678 \
    mysql-bin.000123 | mysql myapp

更安全的做法（先导出 SQL 审查）：
$ ops_db.py restore --type binlog-replay \
    --binlog-file mysql-bin.000123 \
    --start-position 1234 \
    --stop-position 5678 \
    --dry-run > /tmp/recover.sql
    # 审查后再执行：mysql myapp < /tmp/recover.sql
```

---

### 场景 6：数据库健康检查 — `ops_db check`

这个是补充场景，备份/恢复前都会调用，也可以单独跑：

```bash
ops_db.py check --host 192.168.1.10 --port 3306
```

```
检查项（逐项输出 PASS/WARN/FAIL）：

1. 连接性
   ├─ MySQL 能否登录
   ├─ 版本是否在支持列表内（太老的版本警告）

2. 复制状态（备库 / 主库）
   ├─ 主从线程是否 running
   ├─ 延时是否在可接受范围（> 5min 警告，> 30min 严重）
   ├─ 复制错误（Last_SQL_Error 有内容时报错）
   └─ 主从 GTID 模式一致性

3. 性能
   ├─ 慢查询数量（最近 24h 超过 1000 条警告）
   ├─ 连接数使用率（> 80% 警告）
   └─ 锁等待情况

4. 安全
   ├─ root 是否还在用空密码
   ├─ 是否允许远程 root 登录
   └─ 过期账户清理情况

5. 备份健康
   ├─ 最近一次全量备份距今是否超过 7 天（警告）
   └─ 备份文件是否可读
```

---

## 四、CLI 设计（argparse）

```python
import argparse

# 主命令
parser = argparse.ArgumentParser(description='ops_db — 数据库运维工具')
subparsers = parser.add_subparsers(dest='command')

# install
p_install = subparsers.add_parser('install', help='安装 MySQL')
p_install.add_argument('--version', choices=['5.7','8.0','8.4','9.0'], help='MySQL 版本')
p_install.add_argument('--type', choices=['single','master','slave'], help='实例角色')
p_install.add_argument('--port', type=int, default=3306)
p_install.add_argument('--password', help='root 密码（建议用环境变量更安全）')

# backup
p_backup = subparsers.add_parser('backup', help='备份 MySQL')
p_backup.add_argument('--type', choices=['full','incr','dump'], required=True)
p_backup.add_argument('--host', default='127.0.0.1')
p_backup.add_argument('--port', type=int, default=3306)
p_backup.add_argument('--user', default='root')
p_backup.add_argument('--password', help='建议用环境变量 MYSQL_PASSWORD')
p_backup.add_argument('--databases', nargs='+', help='dump 模式指定库')
p_backup.add_argument('--dest', help='备份目标路径')
p_backup.add_argument('--expire-days', type=int, help='保留天数')

# restore
p_restore = subparsers.add_parser('restore', help='恢复备份')
p_restore.add_argument('--type', choices=['full','pitr','partial','binlog-replay'], required=True)
p_restore.add_argument('--backup-dir', help='备份目录（xtrabackup 备份路径）')
p_restore.add_argument('--host', default='127.0.0.1')
p_restore.add_argument('--port', type=int, default=3306)
p_restore.add_argument('--user', default='root')
p_restore.add_argument('--password', help='建议用环境变量')
p_restore.add_argument('--databases', nargs='+', help='partial 模式：指定恢复的库')
p_restore.add_argument('--binlog-dir', help='PITR 模式：binlog 所在目录')
p_restore.add_argument('--stop-datetime', help='PITR 模式：停止时间，如 "2026-04-29 15:00:00"')
p_restore.add_argument('--binlog-file', help='binlog-replay 模式：binlog 文件名')
p_restore.add_argument('--start-position', type=int, help='binlog-replay 起始 position')
p_restore.add_argument('--stop-position', type=int, help='binlog-replay 终止 position')
p_restore.add_argument('--dry-run', action='store_true', help='binlog-replay：只生成 SQL 不执行')

# check
p_check = subparsers.add_parser('check', help='数据库健康检查')
p_check.add_argument('--host', default='127.0.0.1')
p_check.add_argument('--port', type=int, default=3306)
p_check.add_argument('--user', default='root')
p_check.add_argument('--password', help='建议用环境变量')
p_check.add_argument('--verbose', action='store_true', help='显示详细信息')

# ===== SSH 远程执行参数（install / backup / restore / rebuild 共用）=====
# 所有子命令共享同一套 SSH 参数，远程执行时通过 stdin 上传脚本到目标主机
SSH_ARGS = {
    '--ssh-host': {'help': '远程主机 IP 或主机名（省略则本地执行）'},
    '--ssh-user': {'help': 'SSH 用户名', 'default': 'root'},
    '--ssh-port': {'help': 'SSH 端口', 'type': int, 'default': 22},
    '--ssh-key': {'help': 'SSH 私钥路径，如 ~/.ssh/id_rsa'},
    '--ssh-password': {'help': 'SSH 密码（建议用环境变量 SSH_PASSWORD 更安全）'},
    '--ssh-password-env': {'help': '存放 SSH 密码的环境变量名', 'default': 'SSH_PASSWORD'},
}
for subparser in [p_install, p_backup, p_restore, p_check]:
    subparser.add_argument('--ssh-host')
    subparser.add_argument('--ssh-user', default='root')
    subparser.add_argument('--ssh-port', type=int, default=22)
    subparser.add_argument('--ssh-key')
    subparser.add_argument('--ssh-password')
```

---

## 五、安全设计

| 方面 | 设计 |
|---|---|
| 密码传递 | 优先读取环境变量 `MYSQL_PASSWORD` / `REPL_PASSWORD`，命令行参数打印警告 |
| 备份加密 | 预留 `--encrypt` 参数（调用 `openssl enc`），密钥通过环境变量传入 |
| 敏感日志脱敏 | 日志文件中密码替换为 `***` |
| 备份文件权限 | `chmod 600` 备份文件（防止其他用户读取） |

---

## 六、错误处理策略

```
每个模块函数签名：
def install(...) -> tuple[bool, str]:
    """返回 (是否成功, 消息)"""

错误级别：
├─ FATAL  → 直接退出（前置检查 FAIL、数据覆盖风险）
├─ ERROR   → 操作失败，记录日志，询问是否重试
├─ WARN    → 非致命，继续执行但提示用户
└─ INFO    → 正常流程信息
```

**回滚机制（关键操作前）**

```python
# 伪代码示例
def rebuild_slave(...):
    # 1. safety backup
    backup_dir = safety_backup(slave_host, slave_port)

    # 2. 如果后续任何步骤失败，自动回滚
    try:
        full_backup()
        stop_slave()
        clear_data_dir()
        restore_backup()
        start_slave()
    except Exception as e:
        log.error(f"重搭失败，尝试回滚: {e}")
        rollback(backup_dir)  # 恢复原始数据
        raise
```

---

## 七、目录结构设计

```
~/.ops_db/                      # 配置目录（用户级别，可选 ~/.ops_db.yml 覆盖）
├── config.yml                 # 全局配置（备份路径、日志级别等）
├── logs/                      # 日志目录
│   ├── ops_db_20260429.log
│   └── ops_db_20260430.log
├── backup/                    # 备份文件存储（可选集中存储）
│   ├── full_20260429_100000/
│   ├── incr_20260429_120000/
│   └── full_20260430_100000/
└── .password                  # 加密存储的密码（可选，pass 或 keyring）
```

---

## 八、完整功能一览

| 命令 | 功能 | 优先级 |
|---|---|---|
| `ops_db install` | MySQL 安装 + xtrabackup | P0 |
| `ops_db backup` | 全量/增量/逻辑备份 | P0 |
| `ops_db restore` | 全量/PITR/partial/binlog恢复 | P0 |
| `ops_db replicate` | 主从配置 | P1 |
| `ops_db rebuild` | 备库重搭（延时/故障/新机器） | P1 |
| `ops_db check` | 健康检查 | P1 |
| 定时备份 | cron / systemd timer 集成 | P2 |
| 备份压缩 + 加密 | `innobackup --compress --encrypt` | P2 |
| 备份 offsite | 备份到 OSS/S3 | P2 |
| 在线 DDL | `pt-online-schema-change` | P3 |
| 多实例支持 | 端口/socket 区分 | P2 |

---

## 九、可选增强方向（当前设计暂不覆盖）

| 功能 | 说明 | 优先级 |
|---|---|---|
| 定时备份 | 集成 cron 或 systemd timer | P1（最终要加）|
| 恢复流程 | `ops_db restore` 恢复到某时间点 | P1（备份必须有对应的恢复）|
| 多表空间恢复 | 只恢复特定表 | P2 |
| 备份压缩 + 加密 | `xtrabackup --compress --encrypt` | P2 |
| 备份到 OSS/S3 | 备份文件 offsite | P2 |
| 在线 DDL | pt-online-schema-change | P3 |
| 连接池 + 探活 | 长连接保活 | P3 |
