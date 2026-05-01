# 数据库审计系统设计（AuditDB）

## 一、系统定位

审计系统和 ops_db 是两条独立的产品线，共同服务于数据库运维与安全合规。

```
┌─────────────────────────────────────────────────┐
│              数据库审计系统 AuditDB               │
├─────────────────────────────────────────────────┤
│  审计范围：                                       │
│  ├─ SQL 操作审计（SELECT/INSERT/UPDATE/DELETE）    │
│  ├─ DDL 操作审计（CREATE/ALTER/DROP/TRUNCATE）    │
│  ├─ 会话管理（登录/登出/连接失败）                  │
│  ├─ 权限变更（GRANT/REVOKE/CREATE USER）          │
│  ├─ 配置变更（SET/GLOBAL 修改）                   │
│  └─ 敏感数据访问（指定表/字段的查询）               │
│                                                 │
│  合规支持：                                       │
│  ├─ GDPR（个人数据访问记录）                       │
│  ├─ 网络安全法（操作日志留存 ≥ 6个月）              │
│  └─ 行业监管（等保/SOX 等要求）                   │
└─────────────────────────────────────────────────┘
```

---

## 二、整体架构

```
audit_db/
├── audit_db.py              # CLI 主入口
├── audit_server.py          # 审计代理服务（长期运行）
├── config/
│   └── audit_rules.yml       # 审计规则配置
├── modules/
│   ├── __init__.py
│   ├── logger_adapter.py     # 适配不同 MySQL 审计日志格式
│   ├── parser.py             # 审计日志解析
│   ├── rule_engine.py        # 规则引擎（触发告警）
│   ├── report.py             # 合规报告生成
│   └── sensitive.py          # 敏感数据识别
├── storage/
│   ├── __init__.py
│   ├── es_client.py          # Elasticsearch 存储（推荐）
│   ├── mysql_storage.py       # MySQL 存储（备选）
│   └── file_storage.py        # 本地文件存储（fallback）
├── web/                      # 可选 Web UI
│   ├── app.py
│   ├── routes/
│   └── templates/
├── requirements.txt
└── README.md
```

**两种部署模式**

```
模式 A：Agent 旁路模式（推荐，生产环境）
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│  MySQL      │ ---> │ Audit Server │ ---> │ Elasticsearch│
│  Audit Log  │      │  (解析+过滤)  │      │  (存储+查询) │
└─────────────┘      └──────────────┘      └─────────────┘
                            │
                            v
                     ┌─────────────┐
                     │  Alert 告警  │
                     │ (飞书/邮件)   │
                     └─────────────┘

模式 B：MySQL Enterprise Audit（启用 MySQL 内置审计）
┌─────────────┐      ┌──────────────┐
│ MySQL       │ ---> │ audit_log插件│ ---> │ JSON 文件   │
│ (开启审计)   │      │              │      │ (定期采集)   │
└─────────────┘      └──────────────┘
```

---

## 三、审计范围与事件类型

### 3.1 事件分类

| 事件类型 | 触发条件 | 记录内容 | 风险级别 |
|---|---|---|---|
| `QUERY` | 任何 SQL 执行 | SQL文本、执行时间、影响行数 | 低 |
| `READ` | SELECT 语句 | 同 QUERY + 结果集字段 | 低 |
| `WRITE` | INSERT/UPDATE/DELETE | 同 QUERY + 变更前后的行数 | 中 |
| `DDL` | CREATE/ALTER/DROP | 同 QUERY + 对象类型 | 高 |
| `LOGIN` | 登录成功 | 用户、IP、时间、认证方式 | 中 |
| `LOGIN_FAILED` | 登录失败 | 用户、IP、时间、失败原因 | 高 |
| `LOGOUT` | 登出 | 用户、会话时长 | 低 |
| `GRANT` | 权限变更 | 用户、IP、授予的权限 | 高 |
| `CONFIG_CHANGE` | 系统变量修改 | 变量名、旧值、新值 | 高 |
| `SENSITIVE_ACCESS` | 访问敏感表/字段 | 同 QUERY + 命中规则 | 高 |

### 3.2 风险等级定义

```
HIGH（实时告警）：
  - 登录失败 ≥ 3 次 / 10分钟（同 IP）
  - root / admin 账户登录
  - DROP DATABASE / TRUNCATE TABLE
  - GRANT ALL PRIVILEGES
  - 批量删除（DELETE > 1000 行 / 单次）
  - 访问敏感数据表（如 user_info、account、password）

MEDIUM（每日汇总）：
  - DDL 操作（CREATE/ALTER TABLE）
  - 大量数据导出（SELECT > 10000 行）
  - 非白名单用户访问业务库

LOW（日志留存）：
  - 普通 SELECT 查询
  - 连接/断开会话
  - 配置查询（SHOW 命令）
```

---

## 四、审计日志格式（统一）

无论数据来源（audit_log 插件 / general_log / binlog / 旁路抓包），统一落地为以下 JSON 格式：

```json
{
  "event_id": "uuid-v4",
  "timestamp": "2026-04-29T10:30:15.123+08:00",
  "event_type": "WRITE",
  "risk_level": "MEDIUM",
  "user": "app_user",
  "host": "192.168.1.100",
  "db": "myapp",
  "query": "UPDATE orders SET status='paid' WHERE id=12345",
  "query_hash": "sha256(query)",
  "rows_affected": 1,
  "rows_returned": 0,
  "duration_ms": 12,
  "connection_id": 18432,
  "server_id": 1,
  "schema": "myapp",
  "table_name": "orders",
  "matched_rules": ["sensitive_data_access"],
  "client_ip": "192.168.1.100",
  "application": "order-service",
  "extra": {}
}
```

---

## 五、规则引擎（rule_engine.py）

### 5.1 规则配置格式（audit_rules.yml）

```yaml
rules:
  # 规则1：DROP/TRUNCATE 高危操作
  - id: rule_ddl_dangerous
    name: 高危 DDL 操作
    event_types: [DDL]
    risk_level: HIGH
    condition:
      query_pattern: "(?i)(DROP|TRUNCATE)\\s+(DATABASE|TABLE)"
    action:
      alert: true
      alert_channels: [feishu, email]
      block: false   # true = 告警后阻止执行（需要 agent 在执行前拦截）

  # 规则2：敏感表访问
  - id: rule_sensitive_access
    name: 敏感数据访问
    event_types: [READ, WRITE]
    risk_level: HIGH
    condition:
      table_pattern: "(?i)(user|account|password|credential|balance)"
    action:
      alert: true
      alert_channels: [feishu]
      log_full_query: true    # 完整记录这条 SQL（不加截断）

  # 规则3：登录失败告警
  - id: rule_login_failed
    name: 暴力破解检测
    event_types: [LOGIN_FAILED]
    risk_level: HIGH
    condition:
      threshold: 3            # 同一个 IP 10分钟内失败3次
      window_minutes: 10
    action:
      alert: true
      alert_channels: [feishu, email]

  # 规则4：大批量删除
  - id: rule_mass_delete
    name: 大批量删除
    event_types: [WRITE]
    risk_level: HIGH
    condition:
      query_pattern: "(?i)DELETE\\s+FROM"
      rows_threshold: 1000
    action:
      alert: true
      alert_channels: [feishu]

  # 规则5：权限变更
  - id: rule_privilege_change
    name: 权限变更
    event_types: [GRANT]
    risk_level: HIGH
    condition:
      query_pattern: "(?i)(GRANT|REVOKE|CREATE\\s+USER|DROP\\s+USER)"
    action:
      alert: true
      alert_channels: [feishu, email]
      log_full_query: true

  # 规则6：白名单用户（不告警）
  - id: rule_whitelist_user
    name: 白名单用户
    event_types: [QUERY, READ, WRITE, DDL]
    risk_level: LOW
    condition:
      user_pattern: "^(backup_user|dba_admin|monitor_user)$"
    action:
      alert: false
      log_level: DEBUG

  # 规则7：白名单 IP
  - id: rule_whitelist_ip
    name: 白名单 IP
    event_types: [QUERY, READ, WRITE, DDL]
    risk_level: LOW
    condition:
      ip_whitelist:
        - "192.168.1.0/24"
        - "10.0.0.1"
    action:
      alert: false
      log_level: DEBUG

  # 规则8：慢查询阈值
  - id: rule_slow_query
    name: 慢查询记录
    event_types: [QUERY, READ]
    risk_level: LOW
    condition:
      duration_ms_threshold: 5000   # 超过 5 秒
    action:
      alert: false
      log_level: INFO
```

### 5.2 规则匹配流程

```
收到审计事件
    │
    v
┌─────────────────────────────────────┐
│  1. 解析事件 → 提取 user/host/query  │
│  2. 匹配 whitelist（IP / 用户）      │ → 匹配 → 标记 LOW，直接存储
│  3. 按规则优先级逐一匹配              │
│     ├─ 命中 HIGH 规则 → 触发告警     │
│     ├─ 命中 MEDIUM 规则 → 汇总告警   │
│     └─ 命中 LOW 规则 → 仅记录        │
│  4. 写入审计存储（ES/MySQL/文件）    │
└─────────────────────────────────────┘
```

---

## 六、告警通道

```python
# lib/alert.py

ALERT_CHANNELS = {
    "feishu": FeishuWebhook,      # 飞书群机器人
    "email": EmailAlert,          # SMTP 邮件
    "webhook": GenericWebhook,   # 通用 HTTP POST
    "sms": SMSGateway,            # 短信（阿里云/腾讯云）
}

# 告警内容（飞书示例）
{
    "msg_type": "interactive",
    "card": {
        "header": {
            "title": "🔴 数据库高危操作告警",
            "template": "red"
        },
        "elements": [
            {"tag": "text", "text": "事件类型: DDL - DROP TABLE"},
            {"tag": "text", "text": "用户: root@192.168.1.50"},
            {"tag": "text", "text": "SQL: DROP TABLE orders;"},
            {"tag": "text", "text": "时间: 2026-04-29 10:30:15"},
            {"tag": "text", "text": "规则: rule_ddl_dangerous"}
        ]
    }
}
```

---

## 七、审计日志存储

### 7.1 Elasticsearch（推荐）

```
Index 命名：audit_db-YYYY.MM.DD

Mapping：
{
  "mappings": {
    "properties": {
      "timestamp": {"type": "date"},
      "event_type": {"type": "keyword"},
      "risk_level": {"type": "keyword"},
      "user": {"type": "keyword"},
      "host": {"type": "keyword"},
      "query": {"type": "text"},
      "query_hash": {"type": "keyword"},
      "db": {"type": "keyword"},
      "table_name": {"type": "keyword"},
      "rows_affected": {"type": "long"},
      "duration_ms": {"type": "long"},
      "connection_id": {"type": "long"},
      "client_ip": {"type": "ip"},
      "matched_rules": {"type": "keyword"}
    }
  }
}
```

**优点**：查询快、支持聚合、支持 Kibana 可视化、支持冷热分层存储
**缺点**：需要额外部署 ES 集群

### 7.2 MySQL 存储（备选）

```
表结构：audit_events
- id (BIGINT AUTO_INCREMENT PRIMARY KEY)
- event_id (VARCHAR 36)          # UUID
- timestamp (DATETIME(3))
- event_type (ENUM)
- risk_level (ENUM)
- user, host, db, query ...
- INDEX on (timestamp, event_type, user, risk_level)
- PARTITION BY RANGE on timestamp（按月分区）

优点：不需要额外部署
缺点：审计表本身也是 MySQL，查询慢，占用空间大
```

### 7.3 本地文件存储（Fallback）

```
路径：/var/log/audit_db/audit-YYYYMMDD.json

每天一个文件，每行一条 JSON
日志轮转：次日 00:00 关闭昨天文件，压缩后保留 180 天

优点：简单，MySQL 挂了也能记录
缺点：查询分析困难，需要额外工具
```

---

## 八、合规报告（report.py）

### 8.1 内置报告类型

| 报告 | 内容 | 触发方式 |
|---|---|---|
| `daily_summary` | 每日高危操作汇总 | 每日 08:00 自动生成 |
| `login_report` | 登录报表（成功/失败/异常） | 每周一生成 |
| `sensitive_access_report` | 敏感数据访问明细 | 每周一生成 |
| `privilege_change_report` | 权限变更历史 | 实时 + 每周汇总 |
| `compliance_report` | 等保/GDPR 合规报告 | 按需生成 |
| `custom_report` | 自定义 SQL 查询审计日志 | 管理员按需 |

### 8.2 合规报告格式示例（GDPR）

```
========================================
GDPR 数据访问合规报告
审计周期：2026-04-01 ~ 2026-04-29
数据库：myapp
========================================

1. 个人数据表访问统计
   - user_info 表：访问 12,450 次，涉及用户 8,234 人
   - account 表：访问 3,210 次，涉及账户 2,105 个

2. 异常访问记录
   - 同一用户 24h 内访问超过 1000 次：0 条
   - 非工作时间访问个人数据：23 条

3. 敏感字段查询明细（示例）
   | 时间 | 用户 | IP | 查询内容 |
   | 2026-04-15 03:12 | app_svc | 192.168.1.10 | SELECT mobile FROM user_info... |

4. 权限变更记录
   - 4月共 5 次权限变更，均已审批

签名：________________  日期：________________
```

---

## 九、敏感数据识别（sensitive.py）

### 9.1 识别策略

```python
SENSITIVE_RULES = [
    # 按表名
    {"table": "(?i)(user|account|customer|client)", "level": "HIGH"},

    # 按列名
    {"column": "(?i)(password|passwd|secret|token|credential|key)", "level": "HIGH"},
    {"column": "(?i)(mobile|cell|phone|email|address|birth)", "level": "MEDIUM"},
    {"column": "(?i)(balance|amount|money|card|bank)", "level": "MEDIUM"},

    # 按正则匹配内容（脱敏前的数据）
    {"content": "\\d{11}", "desc": "手机号"},      # 11位数字
    {"content": "\\d{18}", "desc": "身份证"},       # 18位身份证
    {"content": "\\d{16,19}", "desc": "银行卡号"},  # 信用卡号
]

def classify_sensitivity(query: str, tables: list, columns: list) -> str:
    """返回：HIGH / MEDIUM / LOW"""
```

### 9.2 脱敏策略（落盘时）

```
落盘的敏感字段处理：
- password / token / secret    → 脱敏为 "***"
- mobile / email / id_card     → 脱敏为 "138****1234"
- balance / amount             → 保留数值但脱敏标识

注意：FULL_QUERY 不脱敏（合规要求），脱敏只用于展示/告警摘要
```

---

## 十、部署与集成

### 10.1 审计代理服务（audit_server.py）

```
部署方式：systemd 服务，长期运行

职责：
1. 监听 MySQL audit_log 文件变化（inotify / tail -F）
2. 实时解析每行日志
3. 送入规则引擎
4. 触发告警（异步，不阻塞主流程）
5. 写入存储后端

性能指标：
- 单台 MySQL（QPS 1万）→ 审计代理峰值 ~5000 条/秒
- 建议 ES 集群接收
```

### 10.2 与 MySQL 的集成方式

```
方式 1：audit_log 插件（MySQL Enterprise / 社区版需加载）
- MySQL 5.7 + audit_log plugin
- 配置 audit_log_format = JSON
- audit_log_file = /var/lib/mysql/audit.log

方式 2：general_log（不推荐生产）
- 性能损耗大，约 10-15% QPS
- 适合测试/临时开启

方式 3：binlog 解析（适合增量同步）
- 只能获取 WRITE 类操作
- 无法获取 SELECT（读操作）
- 适合数据变更审计，不适合操作审计

方式 4：旁路抓包（最通用，但最重）
- tcpdump 抓 3306 端口流量
- 解析 MySQL 协议
- 适合无法改 MySQL 配置的场景
```

---

## 十一、CLI 设计（audit_db.py）

```python
parser = argparse.ArgumentParser(description='audit_db — 数据库审计工具')
subparsers = parser.add_parser('server', help='启动审计代理服务')
subparsers.add_argument('--config', default='config/audit_rules.yml')
subparsers.add_argument('--log-level', choices=['DEBUG','INFO','WARN'])

p_query = subparsers.add_parser('query', help='查询审计日志')
p_query.add_argument('--event-type', choices=['QUERY','DDL','LOGIN','GRANT',...])
p_query.add_argument('--user')
p_query.add_argument('--host')
p_query.add_argument('--start-time')
p_query.add_argument('--end-time')
p_query.add_argument('--risk-level', choices=['HIGH','MEDIUM','LOW'])
p_query.add_argument('--limit', type=int, default=100)
p_query.add_argument('--output', choices=['json','table','csv'])

p_alert = subparsers.add_parser('alert', help='查看告警记录')
p_alert.add_argument('--since', default='24h')
p_alert.add_argument('--resolved', action='store_true')

p_report = subparsers.add_parser('report', help='生成合规报告')
p_report.add_argument('--type', choices=['daily_summary','login','gdpr','custom'])
p_report.add_argument('--start-date')
p_report.add_argument('--end-date')
p_report.add_argument('--output', choices=['html','pdf','txt'])
```

---

## 十二、与 ops_db 的关系

```
ops_db（运维）← 独立工具链 → audit_db（审计）

集成点：
1. ops_db install    → 自动开启 audit_log 插件（如果是 MySQL Enterprise）
2. ops_db backup    → 备份前后记录审计日志（谁在什么时间备份了什么）
3. ops_db replicate → 主从配置变更记录到审计日志
4. ops_db restore   → 恢复操作必须记录（敏感操作，需要审批流）
5. ops_db check     → 健康检查的结果可以写审计日志
```

---

## 十三、技术依赖

```
Python >= 3.10

核心库：
- pymysql / mysql-connector-python  # MySQL 连接
- elasticsearch                       # ES 存储（推荐）
- PyYAML                             # 规则文件解析
- jinja2                             # 报告模板
- weasyprint / reportlab             # PDF 报告生成（可选）
- apscheduler                        # 定时任务（报告生成）
- structlog                          # 结构化日志

可选库：
- aiomysql                          # 异步 MySQL 连接
- python-lawinstruct                 # 告警规则表达式（可选）
```

---

## 十四、优先级

| 阶段 | 内容 | 优先级 |
|---|---|---|
| **Phase 1** | Agent 日志采集 + ES 存储 + HIGH 规则告警 | P0 |
| **Phase 2** | CLI query 查询 + daily_summary 报告 | P1 |
| **Phase 3** | 登录报表 + 敏感数据识别 | P1 |
| **Phase 4** | GDPR 合规报告 + 飞书告警 | P2 |
| **Phase 5** | Web UI + 可视化看板 | P3 |
| **Phase 6** | 告警自动拦截（高危 SQL 确认后执行） | P3 |
