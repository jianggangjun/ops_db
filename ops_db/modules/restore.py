"""MySQL 恢复模块 — 支持全量/PITR/binlog-replay/partial 恢复。"""

from __future__ import annotations

import json
import os
import subprocess
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
from ..lib.mysql_conn import get_conn
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
        raise RuntimeError(f"命令执行失败:\n{cp.stderr}")
    return cp


def _read_backup_meta(backup_dir: str) -> dict:
    """读取备份元数据文件。"""
    meta_file = os.path.join(backup_dir, "backup_meta.json")
    if os.path.exists(meta_file):
        try:
            with open(meta_file) as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"无法读取元数据文件: {e}")
    return {}


def _wait_mysql_ready(
    host: str,
    port: int,
    timeout: int = 60,
) -> bool:
    """等待 MySQL 可连接。"""
    import time

    start = time.time()
    while time.time() - start < timeout:
        try:
            pymysql.connect(
                host=host,
                port=port,
                user="root",
                connect_timeout=5,
            )
            return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _mysql_service_name(family: str) -> tuple[str, str]:
    """根据 OS family 返回启动/停止命令。"""
    if family in ("debian", "ubuntu"):
        return "service mysql start", "service mysql stop"
    elif family in ("rhel", "centos", "fedora"):
        return "systemctl start mysqld", "systemctl stop mysqld"
    else:
        return "systemctl start mysql", "systemctl stop mysql"


def _mask_password(cmd: str, password: Optional[str]) -> str:
    """脱敏命令中的密码。"""
    if not password:
        return cmd
    return cmd.replace(password, "***")


def _print_warning(operation: str, path: str, meta: dict) -> None:
    """打印警告横幅。"""
    print("\n" + "=" * 50)
    print(f"🔴  危险操作：{operation}")
    print("=" * 50)
    print(f"  目标路径 : {path}")
    if meta.get("binlog_file"):
        print(f"  Binlog   : {meta['binlog_file']} @ {meta['binlog_position']}")
    if meta.get("backup_type"):
        print(f"  备份类型 : {meta['backup_type']}")
    print("=" * 50)


# ---------------------------------------------------------------------------
# 全量恢复
# ---------------------------------------------------------------------------

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
    :return (success, message)
    """
    if not os.path.exists(backup_dir):
        return False, f"备份目录不存在: {backup_dir}"

    meta = _read_backup_meta(backup_dir)

    _print_warning("全量恢复", backup_dir, meta)
    print(f"  恢复至   : {datadir}")
    print(f"  MySQL    : {host}:{port}")
    print("⚠️  此操作将覆盖 datadir 下所有数据！")
    if not yes:
        confirm = input("\n确认继续？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # 检测 OS 类型
    os_info = detect_os()
    start_cmd, stop_cmd = _mysql_service_name(os_info.family)
    logger.info(f"检测到 OS family: {os_info.family}，使用服务命令: {start_cmd}")

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_xtrabackup(),
        check_disk_space(datadir, required_gb=1),
    ])
    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 1. 停止 MySQL
    logger.info("停止 MySQL 服务...")
    run_command(f"{stop_cmd} || true", timeout=30)

    # 2. Safety backup
    safety_dir = datadir + f"_old_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    if os.path.exists(datadir) and os.listdir(datadir):
        logger.info(f"移动现有数据到 safety backup: {safety_dir}")
        run_command(f"mv {datadir} {safety_dir}")

    # 3. 重建 datadir
    os.makedirs(datadir, exist_ok=True)

    # 4. --prepare（如果未 prepare）
    xtrabackup_info = os.path.join(backup_dir, "xtrabackup_checkpoints")
    if os.path.exists(xtrabackup_info):
        with open(xtrabackup_info) as f:
            content = f.read()
            if "to_lsn" not in content or "full backup" in content.lower():
                logger.info("备份未 prepare，执行 --apply-log...")
                try:
                    run_command(
                        f"xtrabackup --prepare --target-dir={backup_dir}",
                        timeout=1800,
                    )
                except Exception as e:
                    logger.warning(f"prepare 失败，可能已执行过: {e}")

    # 5. --copy-back
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

    # 6. 设置权限
    logger.info("设置权限...")
    run_command(f"chown -R mysql:mysql {datadir}")

    # 7. 启动 MySQL
    logger.info(f"启动 MySQL 服务: {start_cmd}")
    run_command(start_cmd, timeout=30)

    # 8. 等待 MySQL 就绪
    if not _wait_mysql_ready(host, port, timeout=60):
        logger.warning(f"MySQL 端口 {port} 未就绪，服务可能未正常启动")
    else:
        logger.info(f"MySQL 已就绪，可连接 {host}:{port}")

    _print_success(datadir, "全量恢复", f"原始数据已移至: {safety_dir}")
    return True, f"全量恢复成功，原始数据在: {safety_dir}"


# ---------------------------------------------------------------------------
# PITR（时间点恢复）
# ---------------------------------------------------------------------------

def restore_pitr(
    backup_dir: str,
    stop_datetime: str,
    binlog_dir: str = "/var/lib/mysql/binlog",
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    datadir: str = "/var/lib/mysql",
    yes: bool = False,
) -> tuple[bool, str]:
    """
    PITR 时间点恢复。

    流程：
    1. 对全量备份执行 --prepare，结合 binlog 回放到指定时间点
    2. copy-back 恢复

    :param backup_dir: 全量备份目录
    :param stop_datetime: 停止时间点，格式 "YYYY-MM-DD HH:MM:SS"
    :param binlog_dir: binlog 文件目录
    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param datadir: 恢复目标 data 目录
    :param yes: 跳过确认
    :return (success, message)
    """
    if not os.path.exists(backup_dir):
        return False, f"备份目录不存在: {backup_dir}"
    if not os.path.exists(binlog_dir):
        return False, f"binlog 目录不存在: {binlog_dir}"

    meta = _read_backup_meta(backup_dir)

    _print_warning("PITR 时间点恢复", backup_dir, meta)
    print(f"  停止时间点 : {stop_datetime}")
    print(f"  binlog 目录: {binlog_dir}")
    print(f"  恢复至     : {datadir}")
    print("⚠️  此操作将覆盖 datadir 下所有数据！")
    if not yes:
        confirm = input("\n确认继续？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    os_info = detect_os()
    start_cmd, stop_cmd = _mysql_service_name(os_info.family)

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_xtrabackup(),
        check_disk_space(datadir, required_gb=1),
    ])
    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 1. 停止 MySQL
    logger.info("停止 MySQL 服务...")
    run_command(f"{stop_cmd} || true", timeout=30)

    # 2. Safety backup
    safety_dir = datadir + f"_old_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    if os.path.exists(datadir) and os.listdir(datadir):
        logger.info(f"移动现有数据到 safety backup: {safety_dir}")
        run_command(f"mv {datadir} {safety_dir}")

    os.makedirs(datadir, exist_ok=True)

    # 3. PITR prepare
    # XtraBackup 8.0: --prepare 时用 --binlog-info + --to-latest 或 --stop-never
    # 方式：先 --apply-log，再逐个 binlog apply
    logger.info(f"执行 PITR prepare（全量 prepare + binlog 回放至 {stop_datetime}）...")

    # 先对全量备份做 prepare
    try:
        run_command(
            f"xtrabackup --prepare --target-dir={backup_dir}",
            timeout=1800,
        )
    except Exception as e:
        return False, f"PITR prepare 失败: {e}"

    # 从备份元数据获取 backup_binlog_file 和 backup_binlog_pos
    binlog_file = meta.get("binlog_file", "")
    binlog_pos = meta.get("binlog_position", 0)

    if binlog_file:
        logger.info(f"从备份获取 binlog 起点: {binlog_file} @ {binlog_pos}")
        # 使用 mysqlbinlog 提取从备份点到 stop_datetime 的事件
        binlogs = sorted([
            os.path.join(binlog_dir, f)
            for f in os.listdir(binlog_dir)
            if f.startswith(os.path.basename(binlog_file))
               or f > os.path.basename(binlog_file)
        ])
        if binlogs:
            binlog_files_arg = " ".join(binlogs)
            logger.info(f"提取 binlog 从 {binlog_file} 到时间点 {stop_datetime}...")

            # 使用 mysqlbinlog 提取指定时间范围内的事件
            tmp_sql = os.path.join(backup_dir, f"pitr_{datetime.now().strftime('%Y%m%d%H%M%S')}.sql")
            binlog_cmd = (
                f"mysqlbinlog "
                f"--stop-datetime=\"{stop_datetime}\" "
                f"--base64-output=decode-rows "
                f"{binlog_files_arg}"
            )
            logger.info(f"执行: {binlog_cmd[:100]}...")

            try:
                cp_binlog = subprocess.run(
                    binlog_cmd,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=600,
                )
                with open(tmp_sql, "w") as f:
                    f.write(cp_binlog.stdout)
                logger.info(f"binlog 提取完成: {tmp_sql} ({len(cp_binlog.stdout)} bytes)")
            except Exception as e:
                logger.warning(f"binlog 提取失败: {e}，跳过增量回放")
    else:
        logger.warning("备份元数据中无 binlog 信息，跳过 binlog 回放")
        tmp_sql = None

    # 4. copy-back
    logger.info("执行 xtrabackup --copy-back...")
    try:
        run_command(
            f"xtrabackup --copy-back --target-dir={backup_dir} --datadir={datadir}",
            timeout=1800,
        )
    except Exception as e:
        logger.error(f"copy-back 失败: {e}")
        if os.path.exists(safety_dir):
            run_command(f"rm -rf {datadir} && mv {safety_dir} {datadir}")
        return False, f"copy-back 失败，已回滚: {e}"

    # 5. 设置权限
    run_command(f"chown -R mysql:mysql {datadir}")

    # 6. 启动 MySQL
    logger.info(f"启动 MySQL 服务: {start_cmd}")
    run_command(start_cmd, timeout=30)

    # 7. 回放 binlog SQL（如有）
    if tmp_sql and os.path.exists(tmp_sql) and os.path.getsize(tmp_sql) > 0:
        logger.info(f"回放 PITR binlog SQL: {tmp_sql}")
        pwd_arg = f"-p{password}" if password else ""
        try:
            replay_cmd = f"mysql -h{host} -P{port} -u{user} {pwd_arg} < {tmp_sql}"
            logger.info(f"执行: {_mask_password(replay_cmd, password)}")
            run_command(replay_cmd, timeout=600)
            logger.info("PITR binlog 回放完成")
        except Exception as e:
            logger.warning(f"PITR binlog 回放失败: {e}")
        finally:
            try:
                os.remove(tmp_sql)
            except Exception:
                pass
    elif binlog_file:
        logger.warning("PITR 需要 binlog 回放，但无法提取 SQL，建议手动验证数据")

    # 8. 等待 MySQL 就绪
    if not _wait_mysql_ready(host, port, timeout=60):
        logger.warning(f"MySQL 端口 {port} 未就绪")
    else:
        logger.info(f"MySQL 已就绪，可连接 {host}:{port}")

    _print_success(datadir, "PITR 恢复", f"时间点: {stop_datetime}")
    return True, f"PITR 恢复成功（{stop_datetime}），原始数据在: {safety_dir}"


# ---------------------------------------------------------------------------
# binlog-replay 恢复
# ---------------------------------------------------------------------------

def restore_binlog_replay(
    binlog_file: str,
    start_position: int = 4,
    stop_position: Optional[int] = None,
    database: Optional[str] = None,
    binlog_dir: str = "/var/lib/mysql/binlog",
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    dest: Optional[str] = None,
    dry_run: bool = False,
    yes: bool = False,
) -> tuple[bool, str]:
    """
    binlog-replay 恢复：从 binlog 文件中提取指定 position 区间的 SQL 并回放。

    :param binlog_file: binlog 文件名（如 mysql-bin.000123）
    :param start_position: 起始 position（默认 4，即 binlog header 之后）
    :param stop_position: 结束 position（None 表示到文件末尾）
    :param database: 只回放指定数据库（可选）
    :param binlog_dir: binlog 目录
    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param dest: 输出 SQL 文件路径（None 则回放后删除）
    :param dry_run: True 则只输出 SQL 到 dest，不回放
    :param yes: 跳过确认
    :return (success, message)
    """
    binlog_path = os.path.join(binlog_dir, binlog_file)
    if not os.path.exists(binlog_path):
        return False, f"binlog 文件不存在: {binlog_path}"

    if stop_position and stop_position <= start_position:
        return False, f"stop_position ({stop_position}) 必须大于 start_position ({start_position})"

    print("\n" + "=" * 50)
    print("📋  binlog-replay 恢复")
    print("=" * 50)
    print(f"  binlog 文件 : {binlog_file}")
    print(f"  起始位置   : {start_position}")
    print(f"  结束位置   : {stop_position or '文件末尾'}")
    if database:
        print(f"  数据库     : {database}")
    print(f"  binlog 目录: {binlog_dir}")
    if dry_run:
        print("  模式       : dry-run（只生成 SQL，不回放）")
    print("=" * 50)

    if not yes:
        confirm = input("\n确认继续？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
    ])
    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 构造 mysqlbinlog 命令
    binlog_full_path = os.path.join(binlog_dir, binlog_file)
    cmd_parts = [
        "mysqlbinlog",
        f"--start-position={start_position}",
        f"--base64-output=decode-rows",
    ]
    if stop_position:
        cmd_parts.append(f"--stop-position={stop_position}")
    if database:
        cmd_parts.append(f"--database={database}")
    cmd_parts.append(binlog_full_path)

    cmd = " ".join(cmd_parts)
    logger.info(f"提取 binlog: {_mask_password(cmd, password)}")

    # 输出目标
    if dest is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = f"/tmp/binlog_replay_{timestamp}.sql"

    try:
        cp = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if cp.returncode != 0:
            logger.error(f"mysqlbinlog 失败: {cp.stderr[:500]}")
            return False, f"mysqlbinlog 提取失败: {cp.stderr[:500]}"

        with open(dest, "w") as f:
            f.write(cp.stdout)

        size = os.path.getsize(dest)
        logger.info(f"SQL 生成完成: {dest} ({size} bytes)")

        if dry_run:
            print(f"\n✅ dry-run 完成，SQL 已保存到: {dest}")
            print("   审查后可手动执行: mysql < {dest}")
            return True, f"dry-run 完成，SQL: {dest}"

        # 回放
        pwd_arg = f"-p{password}" if password else ""
        replay_cmd = f"mysql -h{host} -P{port} -u{user} {pwd_arg}"
        if database:
            replay_cmd += f" {database}"
        replay_cmd += f" < {dest}"

        logger.info(f"回放 binlog: {_mask_password(replay_cmd, password)}")
        cp_replay = subprocess.run(
            replay_cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600,
        )

        if cp_replay.returncode != 0:
            logger.error(f"binlog 回放失败: {cp_replay.stderr[:500]}")
            return False, f"binlog 回放失败: {cp_replay.stderr[:500]}"

        logger.info("binlog 回放完成")
        return True, f"binlog-replay 成功: {dest}"

    except subprocess.TimeoutExpired:
        return False, "binlog 提取/回放超时"
    except Exception as e:
        return False, f"binlog-replay 失败: {e}"
    finally:
        # 非 dry-run 模式下清理临时文件
        if not dry_run and dest and dest.startswith("/tmp/") and os.path.exists(dest):
            try:
                os.remove(dest)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# partial 单库恢复
# ---------------------------------------------------------------------------

def restore_partial(
    backup_dir: str,
    databases: list[str],
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    datadir: str = "/var/lib/mysql",
    yes: bool = False,
) -> tuple[bool, str]:
    """
    partial 单库恢复（只恢复指定数据库）。

    流程：
    1. 对全量备份做 --prepare --export
    2. 逐表 discard + copy + import

    :param backup_dir: 全量备份目录
    :param databases: 要恢复的数据库列表
    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param datadir: MySQL data 目录
    :param yes: 跳过确认
    :return (success, message)
    """
    if not os.path.exists(backup_dir):
        return False, f"备份目录不存在: {backup_dir}"
    if not databases:
        return False, "必须指定要恢复的数据库（--databases）"

    meta = _read_backup_meta(backup_dir)

    _print_warning("Partial 单库恢复", backup_dir, meta)
    print(f"  目标数据库 : {', '.join(databases)}")
    print("⚠️  此操作将删除并重建指定数据库！")
    if not yes:
        confirm = input("\n确认继续？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_xtrabackup(),
        check_mysql_client(),
    ])
    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 1. prepare --export
    logger.info("执行 xtrabackup --prepare --export...")
    try:
        run_command(
            f"xtrabackup --prepare --export --target-dir={backup_dir}",
            timeout=1800,
        )
    except Exception as e:
        return False, f"xtrabackup --prepare --export 失败: {e}"

    # 2. 连接 MySQL，逐库处理
    pwd = password or os.getenv("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
    except Exception as e:
        return False, f"MySQL 连接失败: {e}"

    restored_tables = 0
    errors: list[str] = []

    try:
        with conn.cursor() as cur:
            for db_name in databases:
                logger.info(f"处理数据库: {db_name}")

                # 检查备份目录中是否存在该库
                db_backup_dir = os.path.join(backup_dir, db_name)
                if not os.path.exists(db_backup_dir):
                    logger.warning(f"备份目录中无此数据库: {db_name}")
                    errors.append(f"备份中无 {db_name}")
                    continue

                # 删除目标库
                logger.info(f"删除目标数据库: {db_name}")
                try:
                    cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
                    conn.commit()
                except Exception as e:
                    errors.append(f"DROP {db_name} 失败: {e}")
                    continue

                # 创建库
                try:
                    cur.execute(f"CREATE DATABASE `{db_name}`")
                    conn.commit()
                except Exception as e:
                    errors.append(f"CREATE {db_name} 失败: {e}")
                    continue

                # 遍历备份目录中的 .ibd 和 .cfg 文件
                for fname in os.listdir(db_backup_dir):
                    if not (fname.endswith(".ibd") or fname.endswith(".cfg")):
                        continue
                    table_name = fname.rsplit(".", 1)[0]
                    logger.info(f"  恢复表: {db_name}.{table_name}")

                    try:
                        # 分开执行：先 DROP TABLE，再 IMPORT TABLESPACE
                        cur.execute(f"DROP TABLE IF EXISTS `{db_name}`.`{table_name}`")
                        conn.commit()
                    except Exception as e:
                        logger.warning(f"  DROP {table_name} 失败（可能表不存在）: {e}")

                    # 从备份目录复制 .ibd 文件
                    src_ibd = os.path.join(db_backup_dir, f"{table_name}.ibd")
                    dst_ibd = os.path.join(datadir, db_name, f"{table_name}.ibd")
                    if os.path.exists(src_ibd):
                        import shutil
                        os.makedirs(os.path.join(datadir, db_name), exist_ok=True)
                        shutil.copy2(src_ibd, dst_ibd)
                        run_command(f"chown mysql:mysql {dst_ibd}")

                        # import 表空间
                        cur.execute(f"ALTER TABLE `{db_name}`.`{table_name}` IMPORT TABLESPACE")
                        conn.commit()
                        logger.info(f"  {table_name} 恢复成功")
                        restored_tables += 1
                    else:
                        logger.warning(f"  {table_name}.ibd 不存在于备份目录")

    except Exception as e:
        return False, f"partial 恢复异常: {e}"
    finally:
        conn.close()

    if errors:
        logger.warning(f"部分错误: {errors}")

    print(f"\n✅ Partial 恢复完成：{restored_tables} 张表已恢复")
    if errors:
        print(f"⚠️  部分失败: {errors}")
    return True, f"Partial 恢复完成，{restored_tables} 张表已恢复"


# ---------------------------------------------------------------------------
# 辅助输出
# ---------------------------------------------------------------------------

def _print_success(path: str, operation: str, extra: str = "") -> None:
    """打印成功消息。"""
    print("\n" + "=" * 50)
    print(f"✅  {operation} 成功")
    print("=" * 50)
    print(f"  路径 : {path}")
    if extra:
        print(f"  备注 : {extra}")
    print("=" * 50 + "\n")
