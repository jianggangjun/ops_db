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

import pymysql

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
# 备份权限检查
# ---------------------------------------------------------------------------

def _check_backup_privileges(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> tuple[bool, str]:
    """
    检查 MySQL 用户是否具备备份所需权限。

    所需权限: RELOAD, LOCK TABLES, REPLICATION CLIENT

    :return (success, message)
    """
    required_privs = {"RELOAD", "LOCK TABLES", "REPLICATION CLIENT"}
    granted_privs = set()
    missing_privs = set()

    try:
        import os as _os
        pwd = password or _os.getenv("MYSQL_PASSWORD")
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            charset="utf8mb4",
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW GRANTS")
                rows = cur.fetchall()
                logger.debug(f"当前用户权限: {rows}")

                # 解析权限列表
                for row in rows:
                    grant_sql = row[0]
                    # 匹配 PRIVILEGES LIKE 'Reload' 等格式
                    import re as _re
                    priv_matches = _re.findall(r"GRANT (.+?) ON", grant_sql, _re.IGNORECASE)
                    for priv_str in priv_matches:
                        # 去除 ALL PRIVILEGES 特殊处理
                        if "ALL PRIVILEGES" in priv_str.upper():
                            granted_privs.update({"RELOAD", "LOCK TABLES", "REPLICATION CLIENT"})
                        else:
                            # 提取各权限名
                            for p in required_privs:
                                if p.upper().replace(" ", "_") in priv_str.upper().replace(" ", "_"):
                                    granted_privs.add(p)
        finally:
            conn.close()
    except pymysql.err.OperationalError as e:
        return False, f"无法连接 MySQL [{host}:{port}]: {e}"

    missing_privs = required_privs - granted_privs
    if missing_privs:
        grant_sql = (
            f"GRANT {', '.join(sorted(required_privs))} ON *.* TO '{user}'@'%"
            if user != "root"
            else f"GRANT {', '.join(sorted(required_privs))} ON *.* TO '{user}'@'localhost'"
        )
        msg = (
            f"权限不足！缺少以下权限: {', '.join(sorted(missing_privs))}\n"
            f"\n请执行以下 SQL 授权后重试:\n"
            f"  {grant_sql};\n"
            f"  FLUSH PRIVILEGES;"
        )
        logger.error(msg)
        return False, msg

    logger.info(f"权限检查通过，用户 '{user}' 具备所需备份权限")
    return True, "权限检查通过"


# ---------------------------------------------------------------------------
# 备份验证
# ---------------------------------------------------------------------------

def _verify_backup(
    backup_dir: str,
    backup_type: str,  # full / incr / dump
) -> tuple[bool, str]:
    """
    验证备份完整性。

    - full: 使用 xtrabackup --prepare 验证（prepare 后才能恢复）
    - incr: 依次 prepare base + incr 验证
    - dump: 使用 grep 统计 SQL 文件结构标记

    :return (success, message)
    """
    if backup_type in ("full", "incr"):
        # 读取元数据获取 basedir（增量备份需要）
        meta = _read_backup_meta(backup_dir)
        basedir = meta.get("basedir")

        if backup_type == "incr" and basedir:
            # 增量备份验证：先 prepare base，再 apply-log incr
            logger.info(f"验证增量备份，先 prepare 全量: {basedir}")
            cmd_base = f"xtrabackup --prepare --apply-log-only --target-dir={basedir}"
            logger.info(f"执行: {cmd_base}")
            try:
                cp = subprocess.run(cmd_base, shell=True, timeout=600, capture_output=True, text=True)
                if cp.returncode != 0:
                    return False, f"全量备份 prepare 失败: {cp.stderr[:500]}"
            except Exception as e:
                return False, f"全量备份 prepare 异常: {e}"

            logger.info(f"验证增量备份 apply-log: {backup_dir}")
            cmd_incr = f"xtrabackup --prepare --target-dir={basedir} --incremental-dir={backup_dir}"
            logger.info(f"执行: {cmd_incr}")
            try:
                cp = subprocess.run(cmd_incr, shell=True, timeout=600, capture_output=True, text=True)
                if cp.returncode != 0:
                    return False, f"增量备份 apply-log 失败: {cp.stderr[:500]}"
            except Exception as e:
                return False, f"增量备份 apply-log 异常: {e}"
        else:
            # 全量备份验证：直接 prepare
            cmd = f"xtrabackup --prepare --target-dir={backup_dir}"
            logger.info(f"验证备份完整性: {cmd}")
            try:
                cp = subprocess.run(cmd, shell=True, timeout=600, capture_output=True, text=True)
                if cp.returncode != 0:
                    return False, f"备份验证失败: {cp.stderr[:500]}"
            except subprocess.TimeoutExpired:
                return False, "备份验证超时"
            except Exception as e:
                return False, f"备份验证异常: {e}"

        logger.info("备份完整性验证通过")
        return True, "备份验证通过"

    elif backup_type == "dump":
        # 逻辑备份验证：检查 SQL 文件是否包含必要的 DDL
        sql_file = backup_dir
        # 如果传入的是目录，尝试找到对应的 .sql 或 .sql.gz 文件
        if os.path.isdir(backup_dir):
            candidates = [
                os.path.join(backup_dir, f)
                for f in os.listdir(backup_dir)
                if f.endswith(".sql") or f.endswith(".sql.gz")
            ]
            if not candidates:
                return False, f"未找到 SQL 文件在目录: {backup_dir}"
            sql_file = candidates[0]

        if sql_file.endswith(".gz"):
            grep_cmd = f"zgrep -c 'DROP TABLE\\|CREATE TABLE' {sql_file}"
        else:
            grep_cmd = f"grep -c 'DROP TABLE\\|CREATE TABLE' {sql_file}"

        try:
            cp = subprocess.run(
                grep_cmd,
                shell=True,
                timeout=60,
                capture_output=True,
                text=True,
            )
            count = int(cp.stdout.strip()) if cp.stdout.strip().isdigit() else 0
            if count == 0:
                return False, f"SQL 文件验证失败：未找到 DROP TABLE 或 CREATE TABLE 语句"
            logger.info(f"SQL 文件验证通过，包含 {count} 条 DDL 语句")
            return True, f"SQL 文件验证通过 ({count} 条 DDL 语句)"
        except Exception as e:
            return False, f"SQL 文件验证异常: {e}"

    return False, f"未知备份类型: {backup_type}"


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
    socket: Optional[str] = None,
    expire_days: int = 7,
) -> tuple[bool, str]:
    """
    全量备份（xtrabackup --backup）。

    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码（建议用环境变量 MYSQL_PASSWORD）
    :param dest: 备份目标根目录，默认 /data/backup
    :param parallel: 并行备份线程数
    :param compress: 是否压缩（需要 qpress）
    :param yes: 跳过确认
    :param socket: MySQL socket 文件路径
    :param expire_days: 备份保留天数
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

    # 权限检查
    ok, priv_msg = _check_backup_privileges(host, port, user, password)
    if not ok:
        logger.error(f"权限检查失败: {priv_msg}")
        return False, f"权限检查失败: {priv_msg}"
    logger.info("备份权限检查通过")

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
    if socket:
        cmd_parts.append(f"--socket={socket}")
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

    # 备份验证
    ok, verify_msg = _verify_backup(backup_dir, "full")
    if not ok:
        logger.warning(f"备份验证未通过: {verify_msg}")
    else:
        logger.info(f"备份验证通过: {verify_msg}")

    # 写元数据
    meta = _get_backup_meta(backup_dir, "full", host, port, user, password)
    _write_backup_meta(backup_dir, meta)

    # 清理过期备份（按天数）
    _cleanup_old_backups(dest, expire_days=expire_days)

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
    compress: bool = False,
    yes: bool = False,
    socket: Optional[str] = None,
    expire_days: int = 7,
) -> tuple[bool, str]:
    """
    增量备份（xtrabackup --backup --incremental-basedir）。

    依赖：必须有一个全量备份作为 basedir。

    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param dest: 备份目标根目录
    :param parallel: 并行线程数
    :param compress: 是否压缩（需要 qpress）
    :param yes: 跳过确认
    :param socket: MySQL socket 文件路径
    :param expire_days: 备份保留天数
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

    # 权限检查
    ok, priv_msg = _check_backup_privileges(host, port, user, password)
    if not ok:
        logger.error(f"权限检查失败: {priv_msg}")
        return False, f"权限检查失败: {priv_msg}"
    logger.info("备份权限检查通过")

    meta_preview = _get_backup_meta(incr_dir, "incr", host, port, user, password)
    print_preview("增量备份", incr_dir, meta_preview)
    print(f"  基于全量备份: {latest_full}")

    if not yes:
        confirm = input("\n确认开始增量备份？[y/N]: ").strip().lower()
        if confirm != "y":
            return False, "用户取消"

    os.makedirs(incr_dir, exist_ok=True)

    cmd_parts = [
        "xtrabackup",
        "--backup",
        f"--target-dir={incr_dir}",
        f"--incremental-basedir={latest_full}",
        f"--user={user}",
        f"--host={host}",
        f"--port={port}",
        f"--parallel={parallel}",
    ]
    if password:
        cmd_parts.append(f"--password={password}")
    if socket:
        cmd_parts.append(f"--socket={socket}")
    if compress:
        cmd_parts.append("--compress")

    cmd = " ".join(cmd_parts)
    logger.info(f"执行增量备份: {_mask_password(cmd, password)}")

    try:
        cp = run_command(cmd, timeout=3600)
    except Exception as e:
        return False, f"增量备份失败: {e}"

    # 备份验证
    ok, verify_msg = _verify_backup(incr_dir, "incr")
    if not ok:
        logger.warning(f"备份验证未通过: {verify_msg}")
    else:
        logger.info(f"备份验证通过: {verify_msg}")

    # 写元数据
    meta = _get_backup_meta(incr_dir, "incr", host, port, user, password)
    meta["basedir"] = latest_full
    _write_backup_meta(incr_dir, meta)

    # 清理过期备份（按天数）
    _cleanup_old_backups(dest, expire_days=expire_days)

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
    socket: Optional[str] = None,
    parallel: int = 4,
) -> tuple[bool, str]:
    """
    逻辑备份（mysqldump）。

    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param dest: 备份目标目录
    :param databases: 指定库列表，None 表示所有表
    :param all_databases: 是否备份所有库
    :param extra_args: 额外 mysqldump 参数，如 "--single-transaction"
    :param yes: 跳过确认
    :param socket: MySQL socket 文件路径
    :param parallel: 并行压缩线程数（需要 pigz），默认 4
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
    if socket:
        cmd_parts.append(f"--socket={socket}")
    if all_databases:
        cmd_parts.append("--all-databases")
    elif databases:
        cmd_parts.extend(databases)
    else:
        logger.error("逻辑备份必须指定数据库（--databases）或使用 --all-databases")
        print("错误：未指定要备份的数据库，请使用 --databases <db1,db2,...> 或 --all-databases")
        return False, "未指定要备份的数据库"
    if extra_args:
        cmd_parts.extend(extra_args.split())

    cmd = " ".join(cmd_parts) + f" > {dump_file}"
    logger.info(f"执行逻辑备份: {_mask_password(cmd, password)}")

    try:
        cp = run_command(cmd, timeout=7200)
    except Exception as e:
        return False, f"逻辑备份失败: {e}"

    # 备份验证
    ok, verify_msg = _verify_backup(dump_file, "dump")
    if not ok:
        logger.warning(f"备份验证未通过: {verify_msg}")
    else:
        logger.info(f"备份验证通过: {verify_msg}")

    # 压缩（优先使用 pigz 实现并行压缩）
    gzip_file = dump_file + ".gz"
    if parallel > 1:
        # 尝试使用 pigz 并行压缩
        try:
            run_command(f"pigz -p {parallel} {dump_file}")
            dump_file = gzip_file
            logger.info(f"使用 pigz 并行压缩（-p {parallel}）")
        except Exception:
            # pigz 不可用，降级为普通 gzip
            logger.warning("pigz 未安装，降级为普通 gzip")
            try:
                run_command(f"gzip {dump_file}")
                dump_file = gzip_file
            except Exception as e:
                logger.warning(f"压缩失败，跳过: {e}")
    else:
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

def _cleanup_old_backups(dest: str, expire_days: int = 7) -> None:
    """
    清理超过指定天数的全量备份目录。

    :param dest: 备份根目录
    :param expire_days: 保留天数（默认 7 天）
    """
    import time as _time

    if not os.path.exists(dest):
        return

    cutoff = _time.time() - expire_days * 86400
    dirs = [
        os.path.join(dest, d)
        for d in os.listdir(dest)
        if os.path.isdir(os.path.join(dest, d)) and d.startswith("full_")
    ]

    for old_dir in dirs:
        mtime = os.path.getmtime(old_dir)
        if mtime < cutoff:
            logger.info(f"清理过期全量备份（超过 {expire_days} 天）: {old_dir}")
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
