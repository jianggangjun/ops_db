# ops_db / audit_db 数据库管理系统

## 项目概述

本项目包含两个独立但相关的系统：

- **ops_db**：MySQL 运维工具（安装、备份、恢复、主从、重搭、健康检查）
- **audit_db**：MySQL 审计系统（SQL 审计、告警、合规报告）

### ops_db 核心功能

| 命令 | 功能 |
|---|---|
| `ops_db.py install` | MySQL 安装 + xtrabackup |
| `ops_db.py backup` | 全量/增量/逻辑备份 |
| `ops_db.py restore` | 全量/PITR/partial/binlog恢复 |
| `ops_db.py replicate` | 主从配置 |
| `ops_db.py rebuild` | 备库重搭（延时/故障/新机器） |
| `ops_db.py check` | 健康检查 |

### audit_db 核心功能

| 命令 | 功能 |
|---|---|
| `audit_db.py server` | 启动审计代理服务 |
| `audit_db.py query` | 查询审计日志 |
| `audit_db.py alert` | 查看告警记录 |
| `audit_db.py report` | 生成合规报告 |

详细设计文档：
- `/root/workspace/ops_db/DESIGN.md` — ops_db 完整设计
- `/root/workspace/ops_db/audit_system_DESIGN.md` — audit_db 完整设计

---

## 技术栈

- **Python >= 3.10**
- **MySQL 连接**：PyMySQL
- **配置模板**：Jinja2
- **日志**：structlog
- **调度**：apscheduler（定时备份）
- **审计存储**：Elasticsearch（推荐）/ MySQL / 文件

---

## 目录结构

```
ops_db/
├── ops_db.py              # CLI 主入口
├── config/
│   └── my.cnf.j2          # MySQL 配置模板
├── modules/
│   ├── __init__.py
│   ├── install.py         # MySQL 安装
│   ├── backup.py          # 备份（xtrabackup / mydumper）
│   ├── restore.py         # 恢复
│   ├── replicate.py       # 主从配置
│   ├── rebuild.py         # 备库重搭
│   └── check.py           # 健康检查
├── lib/
│   ├── __init__.py
│   ├── checker.py         # 前置检查
│   ├── config_gen.py      # my.cnf 渲染
│   ├── logger.py          # 日志
│   ├── mysql_conn.py      # MySQL 连接封装
│   └── system_detect.py   # OS 版本探测
└── requirements.txt

audit_db/
├── audit_db.py            # CLI 主入口
├── audit_server.py        # 审计代理服务
├── config/
│   └── audit_rules.yml    # 审计规则配置
├── modules/
│   ├── logger_adapter.py  # 审计日志格式适配
│   ├── parser.py          # 日志解析
│   ├── rule_engine.py     # 规则引擎
│   ├── report.py          # 合规报告
│   └── sensitive.py       # 敏感数据识别
├── storage/
│   ├── es_client.py       # Elasticsearch 存储
│   ├── mysql_storage.py   # MySQL 存储
│   └── file_storage.py    # 文件存储
└── requirements.txt
```

---

## 开发规范

### 代码风格
- **Python**: Black + isort，100 字符行宽
- **类型提示**: 所有函数必须有 `-> type` 返回类型注解
- **Docstring**: Google Style，每个 public 函数都要有

### 错误处理
- 使用 `Result[T]` 模式：`tuple[bool, str, T]` 返回 `(success, message, data)`
- FATAL 错误直接 `sys.exit(1)`，不抛异常
- WARN/ERROR 记录日志但不阻断执行

### 密码安全
- 优先读取环境变量 `MYSQL_PASSWORD` / `REPL_PASSWORD`
- 命令行参数打印警告：打印时显示 `***`
- 日志中密码替换为 `***`

### 破坏性操作
- DROP/TRUNCATE/清空数据目录 等操作前必须有用户确认（`--yes` 跳过）
- 操作前自动执行 safety backup

### 测试策略
- 每个模块独立的 unit test
- 使用 `unittest.mock` 模拟 MySQL 命令和 SSH
- 集成测试需要真实的 MySQL 实例（容器启动）

---

## 模块接口约定

### 每个模块函数签名

```python
def module_action(...) -> tuple[bool, str]:
    """返回 (是否成功, 消息摘要)"""
    # 所有日志通过 lib.logger 输出，不使用 print
    # 破坏性操作返回前再次确认
```

### Checker 结果格式

```python
@dataclass
class CheckResult:
    item: str        # 检查项
    status: str      # PASS / WARN / FAIL
    message: str    # 说明
    suggestion: str  # 修复建议（FAIL 时）

def run_preflight_checks(actions: list[str]) -> list[CheckResult]:
    """执行前置检查，返回所有结果"""
```

---

## 关键命令参考

### xtrabackup
```bash
# 全量备份
innobackupex --user=root --password=xxx --parallel=4 /backup/path

# 增量备份
innobackupex --incremental --incremental-basedir=/backup/full /backup/incr

# Prepare（恢复前必须）
innobackupex --apply-log /backup/full

# PITR prepare
innobackupex --apply-log --binlog-dir=/path/to/binlog \
    --stop-datetime="2026-04-29 15:00:00" /backup/full

# 恢复
innobackupex --copy-back /backup/full
```

### mysqldump
```bash
mysqldump --single-transaction --master-data=2 \
    --routines --triggers --events \
    --all-databases > full.sql
```

### MySQL 主从
```sql
-- 主库
SHOW MASTER STATUS;
CREATE USER 'repl'@'%' IDENTIFIED BY 'xxx';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';

-- 备库
CHANGE MASTER TO
    MASTER_HOST='xxx',
    MASTER_USER='repl',
    MASTER_PASSWORD='xxx',
    MASTER_LOG_FILE='xxx',
    MASTER_LOG_POS=xxx;
START SLAVE;
SHOW SLAVE STATUS\G
```

---

## 环境变量

```bash
# MySQL 密码（优先读取）
export MYSQL_PASSWORD="xxx"
export REPL_PASSWORD="xxx"

# 备份目标路径
export OPS_DB_BACKUP_DIR="/data/backup"

# 审计 ES 地址
export ES_HOST="http://localhost:9200"
```
