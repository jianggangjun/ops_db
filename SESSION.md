# ops_db 项目交接文档 (Claude Code Hand-off)

## 基本信息
- **用户**：gangjun（姜刚俊），运维工程师
- **本地路径**：`/Users/jianggangjun/shark/workspace/work/projects/ops_db/`
- **GitHub**：`git@github.com:jianggangjun/ops_db.git`
- **云端路径**：`root@120.26.100.54:/root/workspace/ops_db/`
- **本地 VM**：`root@192.168.56.7`（Ubuntu 22.04，已安装 MySQL 8.0 主库）
- **备库 VM**：`root@192.168.56.8`（ubuntu2204-2，待安装 MySQL）

## 项目概述
MySQL 运维工具，支持：
- `install` - MySQL 安装（单机/主库/备库）
- `backup` - 全量/增量/逻辑备份（xtrabackup / mysqldump）
- `restore` - 全量/PITR/partial/binlog-replay 恢复
- `replicate` - 主从配置
- `rebuild` - 备库重搭
- `check` - 健康检查

## 已完成功能（本地测试通过）

### 备份模块 ✅
| 功能 | 状态 | 说明 |
|------|------|------|
| 全量备份 xtrabackup | ✅ | `--type full` |
| 增量备份 xtrabackup | ✅ | `--type incr`，Bug #3 修复 |
| 逻辑备份 mysqldump | ✅ | `--type dump --all-databases`，Bug #6 #7 修复 |

### 恢复模块 ✅
| 功能 | 状态 | 说明 |
|------|------|------|
| 全量恢复 | ✅ | `--type full` |
| PITR 恢复 | ✅ | `--type pitr` |
| binlog-replay | ✅ | `--type binlog-replay --dry-run`，Bug #9 修复 |
| partial 单库恢复 | ✅ | `--type partial`，Bug #10~#15 完整修复链 |

## 已修复的 Bug

| Bug # | 问题 | 修复内容 |
|-------|------|----------|
| #3 | xtrabackup v8.0 `--incremental` 参数移除 | 改用 `--incremental-basedir=<dir>` |
| #6 | mysqldump 缺少 database 参数 | 补全参数 |
| #7 | 逻辑备份验证失败返回 exit code 0 | 修复返回值判断 |
| #8 | backup_full() 收到不需要的参数 | 过滤 irrelevant params |
| #9 | binlog-replay 收到不需要的 datadir 参数 | 过滤 irrelevant params |
| #10 | partial 恢复 SQL 语法错误 | Import after discard |
| #11 | partial 恢复需要 table DDL 或 .cfg | 修复流程 |
| #12 | 表定义缺失 | 设计限制说明 |
| #13 | 先删库后获取定义 | 修复获取顺序 |
| #14 | No database selected | IMPORT 前加 USE mydb |
| #15 | Tablespace exists | 清理残留文件 |
| #16 | tarfile.GZIP Python 3.12 不存在 | 移除 `format=tarfile.GZIP`，用 `mode="w:gz"` |
| #17 | paramiko.SSHClient 无 getpeername() | connect() 时保存 self._host，替换 getpeername() 调用 |

## 关键发现（开发需知）

1. **xtrabackup 8.0 语法变化**：
   - ❌ 旧：`--backup --incremental --incremental-basedir=<dir>`
   - ✅ 新：`--backup --incremental-basedir=<dir>`（无 `--incremental` 标志）

2. **partial 单库恢复正确流程**：
   ```
   CREATE TABLE → DISCARD TABLESPACE → chown → IMPORT TABLESPACE
   ```

3. **CLI 注意**：`--yes` 是顶级参数，必须放在 subcommand 前
   ```bash
   ops_db restore --yes --type full ...
   ```

4. **密码安全**：优先读环境变量 `MYSQL_PASSWORD`，命令行参数会打印警告

5. **SSH 远程部署**：通过 stdin 发送 tar.gz 包到远程，解压执行

## 待完成功能

### 高优先级
1. **主从配置 replicate 模块** - 代码已有，但远程安装备库失败（Bug #16 #17 刚修复，需重测）
2. **增量备份完整链条测试** - 全量→incr1→incr2→PITR恢复
3. **单表级 partial** - `--tables` 参数支持

### 中优先级
4. **带 binlog 的完整 PITR 测试** - 当前 PITR 测试 binlog skip
5. **备库重搭 rebuild 模块** - 代码已有，待测试
6. **健康检查 check 模块** - 代码已有，待测试

## 测试环境

**VM ubuntu2 (192.168.56.7)**：
- OS: Ubuntu 22.04
- MySQL: 8.0.45，密码 `Aa123456%`
- 数据：`mydb.t1 (id=1/haha, id=2/hehhe)`
- 备份目录：`/data/backup/`
- 代理：`192.168.56.101:7890`

**VM ubuntu2204-2 (192.168.56.8)**：
- OS: Ubuntu 22.04
- 待用：作为备库目标机

**测试命令**：
```bash
# 远程安装备库（刚修复完 Bug #16 #17，需重测）
ssh root@192.168.56.7 \
  'cd /root/workspace/ops_db && \
   export MYSQL_PASSWORD="Aa123456%"; \
   export SSH_PASSWORD="123456"; \
   python3 -m ops_db install --type slave --version 8.0 --ssh-host 192.168.56.8'

# 主从配置
ssh root@192.168.56.7 \
  'cd /root/workspace/ops_db && \
   export MYSQL_PASSWORD="Aa123456%"; \
   python3 -m ops_db replicate --master-host 192.168.56.7 --slave-host 192.168.56.8'
```

## 本地 Git 同步
```bash
# 拉取云端最新代码到本地
rsync -avz --delete --exclude='__pycache__' --exclude='*.pyc' \
  root@120.26.100.54:/root/workspace/ops_db/ \
  /Users/jianggangjun/shark/workspace/work/projects/ops_db/

# 推送本地修复到云端
rsync -avz --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' \
  /Users/jianggangjun/shark/workspace/work/projects/ops_db/ \
  root@120.26.100.54:/root/workspace/ops_db/
```

## 交接说明
- 本地代码已同步到云端最新（origin/main）
- VM（`192.168.56.7`）需从云端拉取最新代码
- **开发建议**：Python >= 3.10，需装 `pip install paramiko pymysql jinja2 structlog`
- **调试建议**：优先在 VM 本地测试，SSH 远程问题复杂
- **测试原则**：只测不修，发现 bug 记录后反馈
