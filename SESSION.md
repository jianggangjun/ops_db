# ops_db 会话记录

## 基本信息
- 用户：gangjun（姜刚俊）
- 项目：ops_db 数据库运维脚本（Python）
- 路径：/root/workspace/ops_db/
- 本地测试：虚拟机 Ubuntu 22.04
- 远程服务器：120.26.100.54

## 背景
原来用 shell 写脚本，文件太大有语法错误，改用 Python 重写

## 测试进度（2026-04-29）
✅ 本地虚拟机：安装、备份、恢复全流程通过
✅ 修复1：backup.py 回滚逻辑加 os.path.exists(safety_dir) 判断
✅ 修复2：backup.py MySQL 服务名兼容 Ubuntu(mysql) 和 CentOS(mysqld)

⏳ 待测试：远程 SSH 主机安装/备份/恢复、增量备份、主从配置

## 同步命令
```bash
rsync -avz --delete --exclude='__pycache__' --exclude='*.pyc' \
  root@120.26.100.54:/root/workspace/ops_db/ ~/ops_db/
```

## Session 管理约定
- 新会话开始先读 /root/workspace/sessions/*.md
- 项目进度写到 /root/workspace/sessions/session-*.md
- 上下文快满（约剩 20%）时主动提醒用户总结
