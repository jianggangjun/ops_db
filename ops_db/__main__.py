#!/usr/bin/env python3
"""ops_db — MySQL 运维工具 CLI 主入口。"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# 确保 lib 和 modules 可导入

from ops_db.lib.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# SSH 参数组（install/backup/restore/rebuild 都可能用到）
# ---------------------------------------------------------------------------

def _add_ssh_args(parser: argparse.ArgumentParser) -> None:
    """给 parser 添加 SSH 远程执行参数组。"""
    ssh = parser.add_argument_group("SSH 远程执行（可选）")
    ssh.add_argument("--ssh-host", metavar="HOST",
                     help="远程目标主机（默认本地），支持 IP 或 hostname")
    ssh.add_argument("--ssh-port", type=int, default=22, help="SSH 端口（默认 22）")
    ssh.add_argument("--ssh-user", default="root", help="SSH 用户（默认 root）")
    ssh.add_argument("--ssh-password", metavar="PASS",
                     help="SSH 密码（建议用环境变量 SSH_PASSWORD）")
    ssh.add_argument("--ssh-key", metavar="FILE",
                     help="SSH 私钥路径（默认 ~/.ssh/id_rsa）")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="ops_db — MySQL 运维工具（安装 / 备份 / 恢复 / 主从 / 重搭 / 检查）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例：

  本地执行：
    python3 ops_db.py install --version 8.0
    python3 ops_db.py backup --type full

  远程执行（通过 SSH）：
    python3 ops_db.py install --version 8.0 \\
        --ssh-host 192.168.1.10 --ssh-user root --ssh-password xxx

    python3 ops_db.py backup --type full \\
        --ssh-host 192.168.1.10 --ssh-user root --ssh-key ~/.ssh/id_rsa

  环境变量：
    export MYSQL_PASSWORD=xxx       # MySQL 密码
    export SSH_PASSWORD=xxx         # SSH 密码
    export OPS_DB_BACKUP_DIR=/data/backup
""",
    )
    parser.add_argument("--yes", "-y", action="store_true",
                        help="跳过所有确认提示")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="显示详细输出")

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # ── install ──────────────────────────────────────────────────────────────
    p_install = subparsers.add_parser("install", help="安装 MySQL")
    p_install.add_argument("--version", choices=["5.7", "8.0", "8.4", "9.0"],
                             help="MySQL 版本")
    p_install.add_argument("--type", dest="role", choices=["single", "master", "slave"],
                             default="single", help="实例角色")
    p_install.add_argument("--port", type=int, default=3306, help="端口（默认 3306）")
    p_install.add_argument("--datadir", default="/var/lib/mysql", help="数据目录")
    p_install.add_argument("--logdir", help="日志目录（默认 datadir/log）")
    p_install.add_argument("--server-id", type=int, help="server-id（默认自动生成）")
    p_install.add_argument("--password", help="root 密码（建议用环境变量 MYSQL_PASSWORD）")
    p_install.add_argument("--mirror", choices=["tencent", "aliyun", "tsinghua", "official", "intranet"],
                             default="tencent", help="MySQL 镜像源（默认 tencent），intranet 从环境变量读取")
    _add_ssh_args(p_install)

    # ── backup ───────────────────────────────────────────────────────────────
    p_backup = subparsers.add_parser("backup", help="备份 MySQL")
    p_backup.add_argument("--type", choices=["full", "incr", "dump"],
                            default="full", help="备份类型")
    p_backup.add_argument("--host", default="127.0.0.1")
    p_backup.add_argument("--port", type=int, default=3306)
    p_backup.add_argument("--user", default="root")
    p_backup.add_argument("--password", help="建议用环境变量 MYSQL_PASSWORD")
    p_backup.add_argument("--dest", help="备份目标路径（默认 /data/backup）")
    p_backup.add_argument("--parallel", type=int, default=4, help="并行线程数")
    p_backup.add_argument("--compress", action="store_true", help="压缩备份")
    p_backup.add_argument("--encrypt", action="store_true", help="加密备份（需要 xtrabackup 加密支持）")
    p_backup.add_argument("--encrypt-key-file", metavar="FILE",
                          help="加密密钥文件路径（加密时必须）")
    p_backup.add_argument("--socket", help="MySQL socket 文件路径（用于本地连接）")
    p_backup.add_argument("--expire-days", type=int, default=7,
                          help="备份保留天数（默认 7 天）")
    p_backup.add_argument("--databases", nargs="+", help="dump 模式：指定库")
    p_backup.add_argument("--all-databases", action="store_true", help="dump 模式：所有库")
    _add_ssh_args(p_backup)

    # ── restore ─────────────────────────────────────────────────────────────
    p_restore = subparsers.add_parser("restore", help="恢复备份")
    p_restore.add_argument("--type", choices=["full", "pitr", "pitr-chain", "partial", "binlog-replay"],
                             default="full", help="恢复类型")
    p_restore.add_argument("--backup-dir", help="备份目录")
    p_restore.add_argument("--host", default="127.0.0.1")
    p_restore.add_argument("--port", type=int, default=3306)
    p_restore.add_argument("--user", default="root")
    p_restore.add_argument("--password", help="建议用环境变量")
    p_restore.add_argument("--datadir", default="/var/lib/mysql", help="恢复目标 data 目录")
    p_restore.add_argument("--decrypt-key-file", metavar="FILE",
                          help="解密密钥文件（恢复加密备份时需要）")
    p_restore.add_argument("--binlog-dir", help="PITR：binlog 目录")
    p_restore.add_argument("--stop-datetime", help="PITR：停止时间")
    p_restore.add_argument("--binlog-file", help="binlog-replay：文件名")
    p_restore.add_argument("--start-position", type=int)
    p_restore.add_argument("--stop-position", type=int)
    p_restore.add_argument("--database", help="binlog-replay/partial：数据库名")
    p_restore.add_argument("--databases", nargs="+", help="partial：多个数据库")
    p_restore.add_argument("--dest", help="binlog-replay：SQL 输出路径")
    p_restore.add_argument("--dry-run", action="store_true", help="binlog-replay：只生成 SQL")
    _add_ssh_args(p_restore)

    # ── replicate ───────────────────────────────────────────────────────────
    p_replicate = subparsers.add_parser("replicate", help="配置主从复制")
    p_replicate.add_argument("--master-host", required=True)
    p_replicate.add_argument("--master-port", type=int, default=3306)
    p_replicate.add_argument("--slave-host", required=True)
    p_replicate.add_argument("--slave-port", type=int, default=3306)
    p_replicate.add_argument("--master-user", default="root")
    p_replicate.add_argument("--master-password", help="建议用环境变量")
    p_replicate.add_argument("--slave-user", default="root")
    p_replicate.add_argument("--slave-password", help="建议用环境变量")
    p_replicate.add_argument("--repl-user", default="repl")
    p_replicate.add_argument("--repl-password", help="复制账户密码（建议用环境变量）")
    p_replicate.add_argument("--repl-host", default="%", help="复制账户允许的 host")
    _add_ssh_args(p_replicate)

    # ── rebuild ──────────────────────────────────────────────────────────────
    p_rebuild = subparsers.add_parser("rebuild", help="备库重搭")
    p_rebuild.add_argument("--reason", choices=["lag", "crash", "newhost"], required=True,
                             help="重搭原因：lag=延时大, crash=备库故障, newhost=新机器")
    p_rebuild.add_argument("--master-host", required=True)
    p_rebuild.add_argument("--master-port", type=int, default=3306)
    p_rebuild.add_argument("--slave-host", required=True)
    p_rebuild.add_argument("--slave-port", type=int, default=3306)
    p_rebuild.add_argument("--master-user", default="root")
    p_rebuild.add_argument("--master-password", help="建议用环境变量")
    p_rebuild.add_argument("--slave-user", default="root")
    p_rebuild.add_argument("--slave-password", help="建议用环境变量")
    _add_ssh_args(p_rebuild)

    # ── check ────────────────────────────────────────────────────────────────
    p_check = subparsers.add_parser("check", help="数据库健康检查")
    p_check.add_argument("--host", default="127.0.0.1")
    p_check.add_argument("--port", type=int, default=3306)
    p_check.add_argument("--user", default="root")
    p_check.add_argument("--password", help="建议用环境变量")
    p_check.add_argument("--no-replication", action="store_true",
                         help="跳过复制状态检查")
    p_check.add_argument("--no-performance", action="store_true",
                         help="跳过性能检查（慢查询/连接数/锁/磁盘）")
    p_check.add_argument("--verbose", "-v", action="store_true")
    _add_ssh_args(p_check)

    # ── schedule ───────────────────────────────────────────────────────────
    p_schedule = subparsers.add_parser("schedule", help="定时备份调度管理")
    sub_sp = p_schedule.add_subparsers(dest="schedule_action", help="调度操作")

    p_add = sub_sp.add_parser("add", help="添加定时调度")
    p_add.add_argument("--name", required=True, help="调度名称（唯一标识）")
    p_add.add_argument("--cron", required=True, help="cron 表达式，如 '0 2 * * *'")
    p_add.add_argument("backup_cmd", nargs="+", help="备份命令（不含 cron 前缀）")
    _add_ssh_args(p_add)

    sub_sp.add_parser("list", help="查看定时调度列表")

    p_rm = sub_sp.add_parser("remove", help="删除定时调度")
    p_rm.add_argument("--name", required=True, help="要删除的调度名称")
    _add_ssh_args(p_rm)

    return parser


# ---------------------------------------------------------------------------
# SSH 入口路由
# ---------------------------------------------------------------------------

def _is_remote(args: argparse.Namespace) -> bool:
    """判断是否走 SSH 远程模式。"""
    return bool(getattr(args, "ssh_host", None))


def _build_ssh_kwargs(args: argparse.Namespace) -> dict:
    """从 args 中提取 SSH 参数。"""
    kwargs: dict = {
        "host": args.ssh_host,
        "port": args.ssh_port,
        "user": args.ssh_user,
    }

    # 优先从 args 取，没有则从环境变量取
    ssh_password = getattr(args, "ssh_password", None) or os.getenv("SSH_PASSWORD")
    ssh_key = getattr(args, "ssh_key", None) or os.path.expanduser("~/.ssh/id_rsa")

    if ssh_password:
        kwargs["password"] = ssh_password
    elif ssh_key and os.path.exists(ssh_key):
        kwargs["key_file"] = ssh_key

    return kwargs


def _run_remote(
    args: argparse.Namespace,
    module: str,
    module_kwargs: dict,
) -> tuple[bool, str]:
    """
    将 ops_db 打包上传到远程主机，执行后返回结果。

    :param args: 命令行参数（含 SSH 配置）
    :param module: 模块名（install/backup/restore）
    :param module_kwargs: 传给模块函数的参数字典
    """
    from ops_db.lib.ssh_client import (
        SSHClient,
        deploy_and_run_on_remote,
        SSHPool,
        SSHConnectionError,
        PARAMIKO_AVAILABLE,
    )

    if not PARAMIKO_AVAILABLE:
        print("❌ Paramiko 未安装，无法使用 SSH 远程功能")
        print("   安装方法：pip install paramiko")
        print("   或：pip install ops_db[remote]")
        return False, "Paramiko 未安装"

    ssh_kwargs = _build_ssh_kwargs(args)
    remote_dir = "/tmp/ops_db_remote"

    try:
        client = SSHClient()
        client.connect(**ssh_kwargs)

        # 过滤掉 SSH 自身的参数和顶级参数，只传模块需要的
        # yes 是顶级参数，需要在 subcommand 之前传递
        remote_kwargs = {}
        yes_flag = False
        for k, v in module_kwargs.items():
            if k.startswith("ssh_"):
                continue
            if k == "yes":
                yes_flag = bool(v)
                continue
            if k == "decrypt_key_file":
                continue
            remote_kwargs[k] = v

        # 如果是 restore 且 backup-dir 是本地路径，先上传备份目录到远程
        remote_backup_dir = None
        if module == "restore" and "backup_dir" in remote_kwargs:
            local_backup = remote_kwargs["backup_dir"]
            if local_backup and local_backup.startswith("/"):  # 本地绝对路径
                import os
                if os.path.isdir(local_backup):
                    remote_backup_dir = f"/tmp/ops_db_remote/backup/{os.path.basename(local_backup)}"
                    logger.info(f"上传备份目录到远程: {local_backup} → {remote_backup_dir}")
                    client.put_directory(local_backup, remote_backup_dir)
                    remote_kwargs["backup_dir"] = remote_backup_dir

        # 如果是 restore 且 binlog-dir 是本地路径，也上传
        if module == "restore" and "binlog_dir" in remote_kwargs:
            local_binlog = remote_kwargs["binlog_dir"]
            if local_binlog and local_binlog.startswith("/"):  # 本地绝对路径
                import os
                if os.path.isdir(local_binlog):
                    remote_binlog_dir = f"/tmp/ops_db_remote/binlog/{os.path.basename(local_binlog)}"
                    logger.info(f"上传 binlog 目录到远程: {local_binlog} → {remote_binlog_dir}")
                    client.put_directory(local_binlog, remote_binlog_dir)
                    remote_kwargs["binlog_dir"] = remote_binlog_dir

        result = deploy_and_run_on_remote(
            ssh_client=client,
            remote_work_dir=remote_dir,
            module=module,
            module_args=remote_kwargs,
            yes=yes_flag,
        )

        # 打印远程输出
        if args.verbose or True:
            if result.stdout:
                print(result.stdout)
            if result.stderr and not result.success:
                print(result.stderr, file=sys.stderr)

        client.disconnect()
        return result.success, result.stdout or result.stderr

    except SSHConnectionError as e:
        return False, f"SSH 连接失败: {e}"
    except Exception as e:
        logger.exception(f"远程执行异常: {e}")
        return False, str(e)


# ---------------------------------------------------------------------------
# 命令分发
# ---------------------------------------------------------------------------

def _dispatch(args: argparse.Namespace) -> int:
    """根据子命令分发到对应模块。"""

    # 密码环境变量兜底
    if hasattr(args, "password") and args.password:
        logger.warning("建议使用环境变量 MYSQL_PASSWORD 代替命令行参数传递密码")
    if hasattr(args, "ssh_password") and args.ssh_password:
        logger.warning("建议使用环境变量 SSH_PASSWORD 代替命令行参数传递密码")

    # ── install ──────────────────────────────────────────────────────────────
    if args.command == "install":
        module_kwargs = {
            "version": args.version,
            "port": args.port,
            "datadir": args.datadir,
            "logdir": args.logdir,
            "server_id": args.server_id,
            "role": args.role,
            "root_password": args.password,
            "yes": args.yes,
            "mirror": args.mirror,
            "ssh_host": getattr(args, "ssh_host", None),
            "ssh_port": getattr(args, "ssh_port", 22),
            "ssh_user": getattr(args, "ssh_user", "root"),
            "ssh_password": getattr(args, "ssh_password", None),
            "ssh_key": getattr(args, "ssh_key", None),
        }

        if _is_remote(args):
            success, msg = _run_remote(args, "install", module_kwargs)
        else:
            from ops_db.modules.install import install_mysql
            # 去掉 SSH 参数
            local_kwargs = {k: v for k, v in module_kwargs.items()
                            if k.startswith(("version", "port", "datadir", "logdir",
                                              "server_id", "role", "root_password",
                                              "yes", "mirror"))}
            success, msg = install_mysql(**local_kwargs)

    # ── backup ───────────────────────────────────────────────────────────────
    elif args.command == "backup":
        module_kwargs = {
            "type": args.type,
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "dest": args.dest,
            "parallel": args.parallel,
            "compress": args.compress,
            "encrypt": getattr(args, "encrypt", False),
            "encrypt_key_file": getattr(args, "encrypt_key_file", None),
            "socket": args.socket,
            "expire_days": args.expire_days,
            "databases": args.databases,
            "all_databases": args.all_databases,
            "yes": args.yes,
            "ssh_host": getattr(args, "ssh_host", None),
            "ssh_port": getattr(args, "ssh_port", 22),
            "ssh_user": getattr(args, "ssh_user", "root"),
            "ssh_password": getattr(args, "ssh_password", None),
            "ssh_key": getattr(args, "ssh_key", None),
        }

        if _is_remote(args):
            success, msg = _run_remote(args, "backup", module_kwargs)
        else:
            from ops_db.modules.backup import backup_full, backup_incr, backup_dump

            # 公共参数
            common_kwargs = {
                k: v for k, v in module_kwargs.items()
                if not k.startswith("ssh_")
            }

            if args.type == "full":
                # full 不支持 type / databases / all_databases 参数
                full_kwargs = {k: v for k, v in common_kwargs.items()
                               if k not in ("type", "databases", "all_databases")}
                success, msg = backup_full(**full_kwargs)
            elif args.type == "incr":
                # incr 不支持 databases / all_databases
                incr_kwargs = {k: v for k, v in common_kwargs.items()
                               if k not in ("type", "databases", "all_databases")}
                success, msg = backup_incr(**incr_kwargs)
            else:
                # dump 不支持 databases/all_databases 已在函数内处理
                dump_kwargs = {k: v for k, v in common_kwargs.items()
                               if k not in ("type", "databases", "all_databases")}
                success, msg = backup_dump(**dump_kwargs)

    # ── restore ─────────────────────────────────────────────────────────────
    elif args.command == "restore":
        if not args.backup_dir and args.type in ("full", "pitr", "partial"):
            print("--backup-dir 参数必填")
            return 1
        if args.type == "binlog-replay":
            if not args.binlog_file:
                print("--binlog-file 参数必填")
                return 1
            if not args.start_position:
                print("--start-position 参数必填")
                return 1

        module_kwargs = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "datadir": args.datadir,
            "backup_dir": args.backup_dir,
            "yes": args.yes,
            "decrypt_key_file": getattr(args, "decrypt_key_file", None),
            "ssh_host": getattr(args, "ssh_host", None),
            "ssh_port": getattr(args, "ssh_port", 22),
            "ssh_user": getattr(args, "ssh_user", "root"),
            "ssh_password": getattr(args, "ssh_password", None),
            "ssh_key": getattr(args, "ssh_key", None),
        }

        if _is_remote(args):
            success, msg = _run_remote(args, "restore", module_kwargs)
        else:
            from ops_db.modules.restore import (
                restore_binlog_replay,
                restore_full,
                restore_partial,
                restore_pitr,
                restore_pitr_chain,
            )
            local_kwargs = {k: v for k, v in module_kwargs.items()
                            if not k.startswith("ssh_") and k not in ("decrypt_key_file", "type",)}

            if args.type == "full":
                local_kwargs["backup_dir"] = args.backup_dir
                success, msg = restore_full(**local_kwargs)
            elif args.type == "pitr":
                local_kwargs["backup_dir"] = args.backup_dir
                local_kwargs["stop_datetime"] = args.stop_datetime or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                local_kwargs["binlog_dir"] = args.binlog_dir or "/var/lib/mysql/binlog"
                success, msg = restore_pitr(**local_kwargs)
            elif args.type == "pitr-chain":
                local_kwargs["backup_dir"] = args.backup_dir
                local_kwargs["stop_datetime"] = args.stop_datetime
                local_kwargs["binlog_dir"] = args.binlog_dir or "/var/lib/mysql/binlog"
                success, msg = restore_pitr_chain(**local_kwargs)
            elif args.type == "partial":
                local_kwargs["backup_dir"] = args.backup_dir
                local_kwargs["databases"] = getattr(args, "databases", None)
                if not local_kwargs["databases"]:
                    print("--databases 参数必填")
                    return 1
                success, msg = restore_partial(**local_kwargs)
            elif args.type == "binlog-replay":
                # binlog-replay 不需要 datadir/host/port/user/password
                binlog_kwargs = {
                    "binlog_file": args.binlog_file,
                    "start_position": args.start_position,
                    "stop_position": args.stop_position,
                    "binlog_dir": args.binlog_dir or "/var/lib/mysql/binlog",
                    "database": getattr(args, "database", None),
                    "dest": getattr(args, "dest", None),
                    "dry_run": args.dry_run,
                    "yes": args.yes,
                }
                success, msg = restore_binlog_replay(**binlog_kwargs)
            else:
                print(f"不支持的恢复类型: {args.type}")
                return 1

    # ── replicate ───────────────────────────────────────────────────────────
    elif args.command == "replicate":
        from ops_db.modules.replicate import setup_replication

        module_kwargs = {
            "master_host": args.master_host,
            "slave_host": args.slave_host,
            "master_port": args.master_port,
            "slave_port": args.slave_port,
            "master_user": args.master_user,
            "master_password": args.master_password,
            "slave_user": args.slave_user,
            "slave_password": args.slave_password,
            "repl_user": args.repl_user,
            "repl_password": args.repl_password,
            "repl_host": args.repl_host,
            "yes": args.yes,
            "ssh_host": getattr(args, "ssh_host", None),
            "ssh_port": getattr(args, "ssh_port", 22),
            "ssh_user": getattr(args, "ssh_user", "root"),
            "ssh_password": getattr(args, "ssh_password", None),
            "ssh_key": getattr(args, "ssh_key", None),
        }
        success, msg = setup_replication(**module_kwargs)

    # ── rebuild ─────────────────────────────────────────────────────────────
    elif args.command == "rebuild":
        from ops_db.modules.rebuild import rebuild

        module_kwargs = {
            "reason": args.reason,
            "master_host": args.master_host,
            "master_port": args.master_port,
            "slave_host": args.slave_host,
            "slave_port": args.slave_port,
            "master_user": args.master_user,
            "master_password": args.master_password,
            "slave_user": args.slave_user,
            "slave_password": args.slave_password,
            "ssh_host": getattr(args, "ssh_host", None),
            "ssh_port": getattr(args, "ssh_port", 22),
            "ssh_user": getattr(args, "ssh_user", "root"),
            "ssh_password": getattr(args, "ssh_password", None),
            "ssh_key": getattr(args, "ssh_key", None),
        }
        success, msg = rebuild(**module_kwargs)

    # ── check ───────────────────────────────────────────────────────────────
    elif args.command == "check":
        from ops_db.modules.check import check

        module_kwargs = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "check_replication": not getattr(args, "no_replication", False),
            "check_performance": not getattr(args, "no_performance", False),
        }
        success, msg = check(**module_kwargs)

    # ── schedule ───────────────────────────────────────────────────────────────
    elif args.command == "schedule":
        from ops_db.modules.schedule import schedule_add, schedule_list, schedule_remove

        if args.schedule_action == "add":
            # backup_cmd 是 list，需要拼接成字符串
            backup_cmd = "python3 -m ops_db " + " ".join(args.backup_cmd)
            success, msg = schedule_add(
                name=args.name,
                cron=args.cron,
                backup_cmd=backup_cmd,
                ssh_host=getattr(args, "ssh_host", None),
                ssh_port=getattr(args, "ssh_port", 22),
                ssh_user=getattr(args, "ssh_user", "root"),
                ssh_password=getattr(args, "ssh_password", None),
                ssh_key=getattr(args, "ssh_key", None),
            )
        elif args.schedule_action == "list":
            success, msg = schedule_list(
                ssh_host=getattr(args, "ssh_host", None),
                ssh_port=getattr(args, "ssh_port", 22),
                ssh_user=getattr(args, "ssh_user", "root"),
                ssh_password=getattr(args, "ssh_password", None),
                ssh_key=getattr(args, "ssh_key", None),
            )
        elif args.schedule_action == "remove":
            success, msg = schedule_remove(
                name=args.name,
                ssh_host=getattr(args, "ssh_host", None),
                ssh_port=getattr(args, "ssh_port", 22),
                ssh_user=getattr(args, "ssh_user", "root"),
                ssh_password=getattr(args, "ssh_password", None),
                ssh_key=getattr(args, "ssh_key", None),
            )
        else:
            # 无子命令，显示 schedule 帮助
            from ops_db.modules.schedule import schedule_list
            success, msg = schedule_list()

    else:
        parser = _build_parser()
        parser.print_help()
        return 0

    if success:
        print(f"\n✅ {msg}")
        return 0
    else:
        print(f"\n❌ {msg}")
        return 1


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    try:
        return _dispatch(args)
    except KeyboardInterrupt:
        print("\n\n操作已取消")
        return 130
    except Exception as e:
        logger.exception(f"未处理的异常: {e}")
        print(f"\n❌ 发生错误: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())