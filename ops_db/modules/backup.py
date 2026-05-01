"""MySQL 备份模块 — 支持全量/增量/逻辑备份。"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..lib.checker import (
    CheckResult,
    PreflightReport,
    check_disk_space,
    check_mysql_client,
    check_root,
    check_xtrabackup,
)
from ..lib.logger import get_logger
from ..lib.mysql_conn import (
    get_conn,
    get_data_size,
    get_datadir,
    get_master_status,
)
from ..lib.system_detect import detect_os

logger = get_logger(__name__)

# 默认备份存储根目录
DEFAULT_BACKUP_ROOT = os.environ.get("OPS_DB_BACKUP_DIR", "/data/backup")


def run_command(
    cmd: str,
    timeout: int = 3600,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    """执行 shell 命令。"""
    logger.info(f"执行命令: {cmd[:80]}...")
    cp = subprocess.run(
        cmd,
        shell=True,
        timeout=timeout,
        capture_output=capture_output,
        text=True,
    )
    if check and cp.returncode != 0:
        logger.error(f"命令失败 [{cp.returncode}]: {cp.stderr[:500]}")
        raise RuntimeError(f"备份命令执行失败:\n{cp.stderr}")
    return cp


def _mask_password(cmd: str, password: Optional[str]) -> str:
    """脱敏命令中的密码。"""
    if not password:
        return cmd
    return cmd.replace(password, "***")


# ---------------------------------------------------------------------------
# 备份元数据记录
# ---------------------------------------------------------------------------

def _get_backup_meta(
    backup_dir: str,
    backup_type: str,  # full / incr / dump
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> dict:
    """收集备份元数据（binlog position、GTID、数据大小等）。"""
    meta = {
        "backup_dir": backup_dir,
        "backup_type": backup_type,
        "timestamp": datetime.now().isoformat(),
        "host": host,
        "port": port,
        "user": user,
    }

    try:
        # 获取 binlog 位置
        status = get_master_status(host, port, user, password)
        meta["binlog_file"] = status.get("file", "")
        meta["binlog_position"] = status.get("position", 0)
        meta["gtid"] = status.get("gtid", "")

        # 获取数据目录大小
        try:
            size_bytes = get_data_size(host, port, user, password)
            meta["data_size_bytes"] = size_bytes
            meta["data_size_gb"] = round(size_bytes / (1024**3), 2)
        except Exception as e:
            logger.warning(f"获取数据大小失败: {e}")

    except Exception as e:
        logger.warning(f"获取备份元数据失败: {e}")

    return meta


def _write_backup_meta(backup_dir: str, meta: dict) -> None:
    """将元数据写入备份目录。"""
    meta_file = os.path.join(backup_dir, ".backup_meta.json")
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    logger.info(f"备份元数据已写入: {meta_file}")


def _read_backup_meta(backup_dir: str) -> dict:
    """读取备份元数据。"""
    meta_file = os.path.join(backup_dir, ".backup_meta.json")
    if not os.path.exists(meta_file):
        return {}
    with open(meta_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_backup_timestamp(backup_dir: str) -> Optional[str]:
    """从备份目录名解析时间戳。"""
    # 目录名格式: full_20260429_100000 或 incr_20260429_120000
    basename = os.path.basename(backup_dir)
    m = re.search(r"_(\d{8}_\d{6})$", basename)
    if m:
        return m.group(1)
    return None


# ---------------------------------------------------------------------------
# 全量备份
# ---------------------------------------------------------------------------

def backup_full(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    dest: Optional[str] = None,
    parallel: int = 4,
    compress: bool = False,
    yes: bool = False,
) -> tuple[bool, str]:
    """
    全量备份（xtrabackup）。

    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码（建议用环境变量 MYSQL_PASSWORD）
    :param dest: 备份目标根目录，默认 /data/backup
    :param parallel: 并行备份线程数
    :param compress: 是否压缩（需要 qpress）
    :param yes: 跳过确认
    :return (success, message)
    """
    dest = dest or DEFAULT_BACKUP_ROOT
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = os.path.join(dest, f"full_{timestamp}")

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
        check_xtrabackup(),
        check_disk_space(dest, required_gb=1.0),  # 最少 1GB 空间
    ])

    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 确认
    meta_preview = _get_backup_meta(backup_dir, "full", host, port, user, password)
    print_preview("全量备份", backup_dir, meta_preview)

    if not yes:
        confirm = input("\n确认开始全量备份？[y/N]: ").strip().lower()
        if confirm != "y":
            return False, "用户取消"

    # 创建目录
    os.makedirs(backup_dir, exist_ok=True)

    # 构造 xtrabackup 备份命令（8.0 格式）
    cmd_parts = [
        "xtrabackup",
        "--backup",
        f"--user={user}",
        f"--host={host}",
        f"--port={port}",
        f"--parallel={parallel}",
    ]
    if password:
        cmd_parts.append(f"--password={password}")
    if compress:
        cmd_parts.append("--compress")
    cmd_parts.append(f"--target-dir={backup_dir}")

    cmd = " ".join(cmd_parts)
    logger.info(f"执行全量备份: {_mask_password(cmd, password)}")

    try:
        cp = run_command(cmd, timeout=7200)
        logger.info(f"全量备份输出: {cp.stdout[-500:]}")
    except Exception as e:
        logger.error(f"全量备份失败: {e}")
        return False, str(e)

    # --prepare（恢复前必须 prepare）
    logger.info("执行 --apply-log（prepare）...")
    try:
        cp = run_command(f"xtrabackup --prepare --target-dir={backup_dir}", timeout=1800)
    except Exception as e:
        logger.error(f"prepare 失败: {e}")
        return False, f"备份完成但 prepare 失败: {e}"

    # 写元数据
    meta = _get_backup_meta(backup_dir, "full", host, port, user, password)
    _write_backup_meta(backup_dir, meta)

    # 清理过期备份（默认保留 7 个全量）
    _cleanup_old_backups(dest, keep_full=7)

    size_gb = meta.get("data_size_gb", "?")
    print_success(backup_dir, "全量备份", f"数据大小参考: {size_gb}GB")
    return True, f"全量备份成功: {backup_dir}"


# ---------------------------------------------------------------------------
# 增量备份
# ---------------------------------------------------------------------------

def backup_incr(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    dest: Optional[str] = None,
    parallel: int = 4,
    yes: bool = False,
) -> tuple[bool, str]:
    """
    增量备份（xtrabackup --incremental）。

    依赖：必须有一个全量备份作为 basedir。

    :return (success, message)
    """
    dest = dest or DEFAULT_BACKUP_ROOT

    # 查找最新的全量备份
    latest_full = _find_latest_full_backup(dest)
    if not latest_full:
        logger.warning("未找到全量备份，增量备份需要先有全量备份。")
        return False, "未找到全量备份，请先执行全量备份"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    incr_dir = os.path.join(dest, f"incr_{timestamp}")

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
        check_xtrabackup(),
        check_disk_space(dest, required_gb=0.5),
    ])

    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    meta_preview = _get_backup_meta(incr_dir, "incr", host, port, user, password)
    print_preview("增量备份", incr_dir, meta_preview)
    print(f"  基于全量备份: {latest_full}")

    if not yes:
        confirm = input("\n确认开始增量备份？[y/N]: ").strip().lower()
        if confirm != "y":
            return False, "用户取消"

    os.makedirs(incr_dir, exist_ok=True)

    cmd = " ".join([
        "xtrabackup",
        "--backup",
        "--incremental",
        f"--user={user}",
        f"--host={host}",
        f"--port={port}",
        f"--parallel={parallel}",
        f"--incremental-basedir={latest_full}",
        f"--target-dir={incr_dir}",
    ])
    if password:
        cmd += f" --password={password}"

    logger.info(f"执行增量备份: {_mask_password(cmd, password)}")

    try:
        cp = run_command(cmd, timeout=3600)
    except Exception as e:
        return False, f"增量备份失败: {e}"

    # 写元数据
    meta = _get_backup_meta(incr_dir, "incr", host, port, user, password)
    meta["basedir"] = latest_full
    _write_backup_meta(incr_dir, meta)

    print_success(incr_dir, "增量备份", f"基于: {latest_full}")
    return True, f"增量备份成功: {incr_dir}"


def _find_latest_full_backup(dest: str) -> Optional[str]:
    """查找最新的全量备份目录。"""
    if not os.path.exists(dest):
        return None
    dirs = [
        os.path.join(dest, d)
        for d in os.listdir(dest)
        if os.path.isdir(os.path.join(dest, d)) and d.startswith("full_")
    ]
    if not dirs:
        return None
    # 按修改时间排序
    dirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    return dirs[0]


# ---------------------------------------------------------------------------
# 逻辑备份（mysqldump）
# ---------------------------------------------------------------------------

def backup_dump(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    dest: Optional[str] = None,
    databases: Optional[list[str]] = None,
    all_databases: bool = False,
    extra_args: str = "",
    yes: bool = False,
) -> tuple[bool, str]:
    """
    逻辑备份（mysqldump）。

    :param databases: 指定库列表，None 表示所有表
    :param all_databases: 是否备份所有库
    :param extra_args: 额外 mysqldump 参数，如 "--single-transaction"
    :return (success, message)
    """
    dest = dest or DEFAULT_BACKUP_ROOT
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if all_databases:
        dump_file = os.path.join(dest, f"dump_all_{timestamp}.sql")
    elif databases and len(databases) == 1:
        dump_file = os.path.join(dest, f"dump_{databases[0]}_{timestamp}.sql")
    else:
        dump_file = os.path.join(dest, f"dump_custom_{timestamp}.sql")

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
        check_disk_space(dest, required_gb=0.5),
    ])

    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 构造 mysqldump 命令
    cmd_parts = [
        "mysqldump",
        f"--user={user}",
        f"--host={host}",
        f"--port={port}",
        "--single-transaction",
        "--master-data=2",
        "--routines",
        "--triggers",
        "--events",
        "--hex-blob",
        "--complete-insert",
    ]
    if password:
        cmd_parts.append(f"--password={password}")
    if all_databases:
        cmd_parts.append("--all-databases")
    elif databases:
        cmd_parts.extend(databases)
    if extra_args:
        cmd_parts.extend(extra_args.split())

    cmd = " ".join(cmd_parts) + f" > {dump_file}"
    logger.info(f"执行逻辑备份: {_mask_password(cmd, password)}")

    try:
        cp = run_command(cmd, timeout=7200)
    except Exception as e:
        return False, f"逻辑备份失败: {e}"

    # 压缩
    gzip_file = dump_file + ".gz"
    try:
        run_command(f"gzip {dump_file}")
        dump_file = gzip_file
    except Exception as e:
        logger.warning(f"压缩失败，跳过: {e}")

    file_size = os.path.getsize(dump_file) / (1024**2)
    meta = {
        "backup_type": "dump",
        "timestamp": datetime.now().isoformat(),
        "file": dump_file,
        "size_mb": round(file_size, 2),
        "host": host,
        "port": port,
        "databases": databases or "all",
    }
    meta_file = dump_file + ".meta.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print_success(dump_file, "逻辑备份", f"文件大小: {file_size:.1f}MB")
    return True, f"逻辑备份成功: {dump_file}"


# --------------------------------------------------------------------------
# MySQL 服务管理（适配不同 OS）
# --------------------------------------------------------------------------

def _mysql_service_name(family: str) -> tuple[str, str]:
    """根据 OS family 返回 (start_cmd, stop_cmd)。"""
    if family == "debian":
        # Debian/Ubuntu: 服务名通常为 mysql
        return "systemctl start mysql", "systemctl stop mysql"
    else:
        # RHEL/CentOS: 服务名通常为 mysqld
        return "systemctl start mysqld", "systemctl stop mysqld"


def _wait_mysql_ready(host: str, port: int, timeout: int = 30) -> bool:
    """等待 MySQL 服务真正可连接（检测端口）。"""
    import socket, time
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((host, port))
            sock.close()
            return True
        except (socket.error, OSError):
            time.sleep(1)
    return False


# --------------------------------------------------------------------------
# 备份恢复（从全量备份恢复）
# --------------------------------------------------------------------------

def restore_full(
    backup_dir: str,
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    datadir: str = "/var/lib/mysql",
    yes: bool = False,
) -> tuple[bool, str]:
    """
    从全量备份恢复（xtrabackup --copy-back）。

    注意：此操作会清空 datadir，执行前必须确认！

    :param backup_dir: 全量备份目录（prepare 后的）
    :param host: MySQL 主机（当前实例）
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param datadir: 恢复目标 data 目录
    :param yes: 跳过确认
    """
    if not os.path.exists(backup_dir):
        return False, f"备份目录不存在: {backup_dir}"

    # 读取元数据
    meta = _read_backup_meta(backup_dir)

    print("\n" + "=" * 50)
    print("🔴  危险操作：全量恢复")
    print("=" * 50)
    print(f"  备份目录 : {backup_dir}")
    print(f"  恢复至   : {datadir}")
    print(f"  MySQL    : {host}:{port}")
    print("=" * 50)
    print("⚠️  此操作将覆盖 datadir 下所有数据！")
    if not yes:
        confirm = input("确认继续？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # 检测 OS 类型，适配服务名
    os_info = detect_os()
    start_cmd, stop_cmd = _mysql_service_name(os_info.family)
    logger.info(f"检测到 OS family: {os_info.family}，使用服务命令: {start_cmd}")

    # 1. 停止 MySQL
    logger.info("停止 MySQL 服务...")
    run_command(f"{stop_cmd} || true", timeout=30)

    # 2. Safety backup（移动现有数据）
    safety_dir = datadir + f"_old_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    if os.path.exists(datadir) and os.listdir(datadir):
        logger.info(f"移动现有数据到 safety backup: {safety_dir}")
        run_command(f"mv {datadir} {safety_dir}")

    # 3. 清空/重建 datadir
    os.makedirs(datadir, exist_ok=True)

    # 4. --copy-back
    logger.info("执行 xtrabackup --copy-back...")
    try:
        cp = run_command(
            f"xtrabackup --copy-back --target-dir={backup_dir} --datadir={datadir}",
            timeout=1800,
        )
    except Exception as e:
        logger.error(f"copy-back 失败: {e}")
        if os.path.exists(safety_dir):
            logger.info("尝试回滚：从 safety backup 恢复...")
            run_command(f"rm -rf {datadir} && mv {safety_dir} {datadir}")
        else:
            logger.warning("safety backup 不存在，跳过回滚")
        return False, f"copy-back 失败，已回滚: {e}"

    # 5. 设置权限
    logger.info("设置权限...")
    run_command(f"chown -R mysql:mysql {datadir}")

    # 6. 启动 MySQL
    logger.info(f"启动 MySQL 服务: {start_cmd}")
    run_command(start_cmd, timeout=30)

    # 7. 等待 MySQL 就绪（检测端口）
    if not _wait_mysql_ready(host, port, timeout=60):
        logger.warning(f"MySQL 端口 {port} 未就绪，服务可能未正常启动")
    else:
        logger.info(f"MySQL 已就绪，可连接 {host}:{port}")

    print_success(datadir, "全量恢复", f"原始数据已移至: {safety_dir}")
    return True, f"全量恢复成功，原始数据在: {safety_dir}"


# ---------------------------------------------------------------------------
# 过期清理
# ---------------------------------------------------------------------------

def _cleanup_old_backups(dest: str, keep_full: int = 7) -> None:
    """清理超过保留数量的全量备份。"""
    dirs = [
        os.path.join(dest, d)
        for d in os.listdir(dest)
        if os.path.isdir(os.path.join(dest, d)) and d.startswith("full_")
    ]
    dirs.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    for old_dir in dirs[keep_full:]:
        logger.info(f"清理过期全量备份: {old_dir}")
        try:
            run_command(f"rm -rf {old_dir}", timeout=60)
        except Exception as e:
            logger.warning(f"清理失败: {e}")


# ---------------------------------------------------------------------------
# 辅助输出
# ---------------------------------------------------------------------------

def print_preview(operation: str, path: str, meta: dict) -> None:
    """打印操作预览。"""
    print(f"\n{'=' * 50}")
    print(f"📋  {operation}")
    print(f"{'=' * 50}")
    print(f"  目标路径 : {path}")
    if meta.get("binlog_file"):
        print(f"  Binlog   : {meta['binlog_file']} @ {meta['binlog_position']}")
    if meta.get("gtid"):
        print(f"  GTID     : {meta['gtid'][:40]}...")
    if meta.get("data_size_gb"):
        print(f"  数据大小 : ~{meta['data_size_gb']}GB")
    print(f"{'=' * 50}\n")


def print_success(path: str, operation: str, extra: str = "") -> None:
    """打印操作成功信息。"""
    print(f"\n✅  {operation}成功!")
    print(f"   路径: {path}")
    if extra:
        print(f"   {extra}")
