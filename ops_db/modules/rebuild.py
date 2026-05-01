"""MySQL 备库重搭模块 — 支持 lag/crash/newhost 三种场景。"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime
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
from ..lib.mysql_conn import get_conn, get_master_status, get_slave_status
from ..lib.system_detect import detect_os

logger = get_logger(__name__)

# 默认备份存储根目录
DEFAULT_BACKUP_ROOT = os.environ.get("OPS_DB_BACKUP_DIR", "/data/backup")

# 复制延迟阈值（秒）
LAG_THRESHOLD_SECONDS = 300  # 5 分钟


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


def _mask_password(cmd: str, password: Optional[str]) -> str:
    """脱敏命令中的密码。"""
    if not password:
        return cmd
    return cmd.replace(password, "***")


def _mysql_service_name(family: str) -> tuple[str, str]:
    """根据 OS family 返回启动/停止命令。"""
    if family in ("debian", "ubuntu"):
        return "service mysql start", "service mysql stop"
    elif family in ("rhel", "centos", "fedora"):
        return "systemctl start mysqld", "systemctl stop mysqld"
    else:
        return "systemctl start mysql", "systemctl stop mysql"


def get_replication_lag(
    slave_host: str,
    slave_port: int,
    slave_user: str,
    slave_password: Optional[str] = None,
) -> int:
    """
    查询备库复制延迟秒数（Seconds_Behind_Master）。

    :return: 延迟秒数，-1 表示无法获取
    """
    pwd = slave_password or os.getenv("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=pwd,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW SLAVE STATUS")
                row = cur.fetchone()
                if not row:
                    return -1
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
                lag = result.get("Seconds_Behind_Master")
                if lag is None:
                    return -1
                return int(lag) if lag != "NULL" else -1
        finally:
            conn.close()
    except pymysql.err.OperationalError as e:
        logger.warning(f"连接备库失败: {e}")
        return -1
    except Exception as e:
        logger.warning(f"获取复制延迟失败: {e}")
        return -1


def wait_catch_up(
    slave_host: str,
    slave_port: int,
    slave_user: str,
    slave_password: Optional[str] = None,
    max_wait: int = 300,
    threshold: int = 5,
) -> tuple[bool, int]:
    """
    等待备库追上主库（延迟 < threshold 秒）。

    :param slave_host: 备库主机
    :param slave_port: 备库端口
    :param slave_user: 备库用户
    :param slave_password: 备库密码
    :param max_wait: 最大等待秒数
    :param threshold: 认为"追上"的延迟阈值（秒）
    :return: (是否追上, 最终延迟秒数)
    """
    logger.info(f"等待备库追上主库（阈值 {threshold}s，最大等待 {max_wait}s）...")
    start = time.time()
    last_lag = -1

    while time.time() - start < max_wait:
        lag = get_replication_lag(slave_host, slave_port, slave_user, slave_password)
        last_lag = lag

        if lag == -1:
            logger.warning("无法获取复制状态，等待重试...")
            time.sleep(5)
            continue

        if lag < threshold:
            elapsed = int(time.time() - start)
            logger.info(f"备库已追上主库！延迟 {lag}s，耗时 {elapsed}s")
            return True, lag

        elapsed = int(time.time() - start)
        logger.info(f"当前延迟: {lag}s（已等待 {elapsed}s）")
        time.sleep(5)

    logger.warning(f"等待超时，最终延迟: {last_lag}s")
    return False, last_lag


def _ensure_repl_user(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    repl_user: str = "repl",
    repl_password: Optional[str] = None,
) -> tuple[bool, str]:
    """确保主库有复制账户。"""
    pwd = password or os.getenv("MYSQL_PASSWORD")
    repl_pwd = repl_password or os.getenv("REPL_PASSWORD")

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        try:
            with conn.cursor() as cur:
                # 检查是否已存在
                cur.execute(
                    f"SELECT 1 FROM mysql.user WHERE User='{repl_user}' AND Host='%'"
                )
                existing = cur.fetchone()

                if not existing:
                    if not repl_pwd:
                        return False, f"复制账户不存在，请提供 --repl-password"
                    cur.execute(
                        f"CREATE USER '{repl_user}'@'%' "
                        f"IDENTIFIED BY '{repl_pwd}'"
                    )
                    cur.execute(
                        f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'%'"
                    )
                    conn.commit()
                    logger.info(f"已创建复制账户 {repl_user}@'%'")
                else:
                    logger.info(f"复制账户 {repl_user}@'%' 已存在")
        finally:
            conn.close()
        return True, f"复制账户就绪: {repl_user}@'%'"
    except Exception as e:
        return False, f"创建复制账户失败: {e}"


def rebuild_from_master(
    slave_host: str,
    slave_port: int,
    slave_user: str,
    slave_password: Optional[str],
    master_host: str,
    master_port: int,
    master_user: str,
    master_password: Optional[str],
    repl_user: str = "repl",
    repl_password: Optional[str] = None,
    backup_dir: Optional[str] = None,
    datadir: str = "/var/lib/mysql",
    parallel: int = 4,
) -> tuple[bool, str]:
    """
    从主库获取最新数据重建备库。

    流程：
    1. 在主库执行 FLUSH TABLES WITH READ LOCK 获取一致性快照
    2. 获取 binlog position（SHOW MASTER STATUS）
    3. 用 xtrabackup 从主库拉取全量备份
    4. 将备份恢复到备库
    5. 配置复制链路（CHANGE MASTER TO）
    6. 启动复制（START SLAVE）
    7. 验证复制状态

    :param slave_host: 备库主机
    :param slave_port: 备库端口
    :param slave_user: 备库用户
    :param slave_password: 备库密码
    :param master_host: 主库主机
    :param master_port: 主库端口
    :param master_user: 主库用户
    :param master_password: 主库密码
    :param repl_user: 复制账户用户名
    :param repl_password: 复制账户密码
    :param backup_dir: 备份存储目录
    :param datadir: 备库数据目录
    :param parallel: xtrabackup 并行线程数
    :return: (success, message)
    """
    backup_dir = backup_dir or DEFAULT_BACKUP_ROOT
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_backup = os.path.join(backup_dir, f"rebuild_{timestamp}")
    os.makedirs(local_backup, exist_ok=True)

    master_pwd = master_password or os.getenv("MYSQL_PASSWORD")
    slave_pwd = slave_password or os.getenv("MYSQL_PASSWORD")
    repl_pwd = repl_password or os.getenv("REPL_PASSWORD")

    os_info = detect_os()
    start_cmd, stop_cmd = _mysql_service_name(os_info.family)

    # 步骤 1: 确保复制账户存在
    print("\n📝 步骤 1/7: 配置复制账户...")
    ok, msg = _ensure_repl_user(
        master_host, master_port, master_user, master_pwd,
        repl_user, repl_pwd
    )
    if not ok:
        return False, f"复制账户配置失败: {msg}"
    print(f"✅ {msg}")

    # 步骤 2: 获取主库 binlog 位置（带锁）
    print("\n📝 步骤 2/7: 获取主库复制坐标...")
    master_conn = None
    binlog_file = ""
    binlog_pos = 0
    gtid_enabled = False

    try:
        master_conn = pymysql.connect(
            host=master_host,
            port=master_port,
            user=master_user,
            password=master_pwd,
            connect_timeout=10,
        )

        with master_conn.cursor() as cur:
            # 获取 GTID 模式
            cur.execute("SELECT @@GLOBAL.gtid_mode AS gtid_mode")
            gtid_row = cur.fetchone()
            gtid_enabled = gtid_row and gtid_row[0] == "ON"

            # 获取 binlog 位置
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
            if row:
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
                binlog_file = result.get("File", "")
                binlog_pos = result.get("Position", 0)
                logger.info(f"主库 binlog: {binlog_file} @ {binlog_pos}")

        print(f"  binlog 文件: {binlog_file}")
        print(f"  binlog 位置: {binlog_pos}")
        print(f"  GTID 模式  : {'是' if gtid_enabled else '否'}")

    except Exception as e:
        if master_conn:
            master_conn.close()
        return False, f"获取主库状态失败: {e}"

    # 步骤 3: xtrabackup 备份（无锁方式，使用 --backup）
    print("\n📝 步骤 3/7: 从主库拉取全量备份（xtrabackup）...")
    print(f"  备份目录: {local_backup}")

    xtrabackup_cmd = [
        "xtrabackup",
        "--backup",
        f"--host={master_host}",
        f"--port={master_port}",
        f"--user={master_user}",
        f"--parallel={parallel}",
        f"--target-dir={local_backup}",
    ]
    if master_pwd:
        xtrabackup_cmd.append(f"--password={master_pwd}")

    cmd_str = " ".join(xtrabackup_cmd)
    logger.info(f"执行: {_mask_password(cmd_str, master_pwd)}")

    try:
        cp = run_command(cmd_str, timeout=7200)
        logger.info(f"备份输出: {cp.stdout[-500:]}")
    except Exception as e:
        return False, f"xtrabackup 备份失败: {e}"

    # 步骤 4: --prepare
    print("\n📝 步骤 4/7: 执行备份 prepare...")
    try:
        cp = run_command(
            f"xtrabackup --prepare --target-dir={local_backup}",
            timeout=1800,
        )
    except Exception as e:
        return False, f"prepare 失败: {e}"
    print("✅ prepare 完成")

    # 步骤 5: 恢复备库数据
    print("\n📝 步骤 5/7: 恢复备库数据...")

    # 5.1 停止 MySQL
    logger.info("停止备库 MySQL...")
    run_command(f"{stop_cmd} || true", timeout=30)

    # 5.2 Safety backup
    safety_dir = f"{datadir}_old_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    if os.path.exists(datadir) and os.listdir(datadir):
        logger.info(f"移动现有数据到 safety backup: {safety_dir}")
        run_command(f"mv {datadir} {safety_dir}")

    # 5.3 重建 datadir
    os.makedirs(datadir, exist_ok=True)

    # 5.4 --copy-back
    try:
        cp = run_command(
            f"xtrabackup --copy-back --target-dir={local_backup} --datadir={datadir}",
            timeout=1800,
        )
    except Exception as e:
        logger.error(f"copy-back 失败: {e}")
        if os.path.exists(safety_dir):
            logger.info("尝试回滚...")
            run_command(f"rm -rf {datadir} && mv {safety_dir} {datadir}")
        return False, f"copy-back 失败，已回滚: {e}"

    # 5.5 设置权限
    run_command(f"chown -R mysql:mysql {datadir}")

    # 5.6 启动 MySQL
    logger.info(f"启动备库 MySQL: {start_cmd}")
    run_command(start_cmd, timeout=30)

    # 等待 MySQL 就绪
    print("  等待备库启动...")
    time.sleep(5)

    # 步骤 6: 配置复制链路
    print("\n📝 步骤 6/7: 配置备库复制链路...")
    slave_conn = None

    try:
        slave_conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=slave_pwd,
            connect_timeout=30,
        )

        with slave_conn.cursor() as cur:
            # 停止现有复制
            try:
                cur.execute("STOP SLAVE")
                cur.execute("RESET SLAVE ALL")
                logger.info("已停止并重置现有复制")
            except Exception:
                pass  # 可能没有现有复制

            # 构造 CHANGE MASTER TO
            if gtid_enabled:
                change_master = (
                    f"CHANGE MASTER TO "
                    f"MASTER_HOST='{master_host}', "
                    f"MASTER_PORT={master_port}, "
                    f"MASTER_USER='{repl_user}', "
                    f"MASTER_PASSWORD='{repl_pwd}', "
                    f"MASTER_AUTO_POSITION=1"
                )
            else:
                change_master = (
                    f"CHANGE MASTER TO "
                    f"MASTER_HOST='{master_host}', "
                    f"MASTER_PORT={master_port}, "
                    f"MASTER_USER='{repl_user}', "
                    f"MASTER_PASSWORD='{repl_pwd}', "
                    f"MASTER_LOG_FILE='{binlog_file}', "
                    f"MASTER_LOG_POS={binlog_pos}"
                )

            masked_cmd = _mask_password(change_master, repl_pwd)
            logger.info(f"执行: {masked_cmd[:80]}...")
            cur.execute(change_master)
            slave_conn.commit()
            print("✅ CHANGE MASTER TO 执行成功")

            # 启动复制
            cur.execute("START SLAVE")
            slave_conn.commit()
            print("✅ START SLAVE 执行成功")

    except Exception as e:
        if slave_conn:
            slave_conn.close()
        return False, f"配置复制失败: {e}"

    # 步骤 7: 验证复制状态
    print("\n📝 步骤 7/7: 验证复制状态...")
    time.sleep(3)

    if slave_conn:
        slave_conn.close()

    # 检查复制状态
    final_status = get_slave_status(slave_host, slave_port, slave_user, slave_pwd)
    io_running = final_status.get("io_running", "No") if final_status else "No"
    sql_running = final_status.get("sql_running", "No") if final_status else "No"
    lag = final_status.get("lag", -1) if final_status else -1

    print("\n" + "=" * 60)
    print("🔄  备库重搭完成")
    print("=" * 60)
    print(f"  主库 : {master_host}:{master_port}")
    print(f"  备库 : {slave_host}:{slave_port}")
    print(f"  备份 : {local_backup}")
    print(f"  Safety: {safety_dir}")
    print("=" * 60)
    print("  复制状态:")
    io_icon = "✅" if io_running == "Yes" else "❌"
    sql_icon = "✅" if sql_running == "Yes" else "❌"
    print(f"    {io_icon} IO 线程 : {io_running}")
    print(f"    {sql_icon} SQL 线程: {sql_running}")
    if lag >= 0:
        lag_icon = "✅" if lag < 5 else "⚠️ "
        print(f"    {lag_icon} 延时   : {lag} 秒")
    print("=" * 60 + "\n")

    if io_running == "Yes" and sql_running == "Yes":
        return True, f"备库重搭成功，复制已启动"

    last_error = final_status.get("last_error", "未知错误") if final_status else "未知"
    return False, f"复制启动失败: {last_error}"


def rebuild(
    reason: str,
    master_host: str,
    slave_host: str,
    master_port: int = 3306,
    slave_port: int = 3306,
    master_user: str = "root",
    master_password: Optional[str] = None,
    slave_user: str = "root",
    slave_password: Optional[str] = None,
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> tuple[bool, str]:
    """
    MySQL 备库重搭主入口。

    :param reason: 重搭原因（lag/crash/newhost）
    :param master_host: 主库主机
    :param slave_host: 备库主机
    :param master_port: 主库端口
    :param slave_port: 备库端口
    :param master_user: 主库用户
    :param master_password: 主库密码
    :param slave_user: 备库用户
    :param slave_password: 备库密码
    :param ssh_host: SSH 远程主机（可选）
    :param ssh_port: SSH 端口
    :param ssh_user: SSH 用户
    :param ssh_password: SSH 密码
    :param ssh_key: SSH 私钥路径
    :return: (success, message)
    """
    reason_desc = {
        "lag": "复制延迟过大",
        "crash": "备库崩溃恢复",
        "newhost": "迁移到新主机",
    }

    print("\n" + "=" * 60)
    print("🔧  MySQL 备库重搭")
    print("=" * 60)
    print(f"  原因   : {reason} — {reason_desc.get(reason, reason)}")
    print(f"  主库   : {master_host}:{master_port}")
    print(f"  备库   : {slave_host}:{slave_port}")
    print("=" * 60)

    # SSH 远程模式
    if ssh_host:
        return _rebuild_remote(
            reason=reason,
            master_host=master_host,
            slave_host=slave_host,
            master_port=master_port,
            slave_port=slave_port,
            master_user=master_user,
            master_password=master_password,
            slave_user=slave_user,
            slave_password=slave_password,
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            ssh_key=ssh_key,
        )

    # 本地模式
    return _rebuild_local(
        reason=reason,
        master_host=master_host,
        slave_host=slave_host,
        master_port=master_port,
        slave_port=slave_port,
        master_user=master_user,
        master_password=master_password,
        slave_user=slave_user,
        slave_password=slave_password,
    )


def _rebuild_local(
    reason: str,
    master_host: str,
    slave_host: str,
    master_port: int,
    slave_port: int,
    master_user: str,
    master_password: Optional[str],
    slave_user: str,
    slave_password: Optional[str],
) -> tuple[bool, str]:
    """本地模式重搭。"""
    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
        check_xtrabackup(),
    ])

    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 根据 reason 执行不同逻辑
    if reason == "lag":
        return _rebuild_for_lag(
            master_host=master_host,
            slave_host=slave_host,
            master_port=master_port,
            slave_port=slave_port,
            master_user=master_user,
            master_password=master_password,
            slave_user=slave_user,
            slave_password=slave_password,
        )
    elif reason == "crash":
        return _rebuild_for_crash(
            master_host=master_host,
            slave_host=slave_host,
            master_port=master_port,
            slave_port=slave_port,
            master_user=master_user,
            master_password=master_password,
            slave_user=slave_user,
            slave_password=slave_password,
        )
    elif reason == "newhost":
        return _rebuild_for_newhost(
            master_host=master_host,
            slave_host=slave_host,
            master_port=master_port,
            slave_port=slave_port,
            master_user=master_user,
            master_password=master_password,
            slave_user=slave_user,
            slave_password=slave_password,
        )
    else:
        return False, f"不支持的重搭原因: {reason}"


def _rebuild_for_lag(
    master_host: str,
    slave_host: str,
    master_port: int,
    slave_port: int,
    master_user: str,
    master_password: Optional[str],
    slave_user: str,
    slave_password: Optional[str],
) -> tuple[bool, str]:
    """因复制延迟过大而重搭。"""
    print("\n📋  场景: 复制延迟过大")

    # 先检查当前延迟
    lag = get_replication_lag(slave_host, slave_port, slave_user, slave_password)
    print(f"  当前延迟: {lag}s（阈值: {LAG_THRESHOLD_SECONDS}s）")

    if lag < LAG_THRESHOLD_SECONDS:
        print("  延迟未超过阈值，无需重搭")
        return True, f"延迟 {lag}s < {LAG_THRESHOLD_SECONDS}s，无需重搭"

    # 尝试先等待追上
    print("  尝试等待备库追上...")
    caught_up, final_lag = wait_catch_up(
        slave_host, slave_port, slave_user, slave_password,
        max_wait=600, threshold=10
    )

    if caught_up:
        print("✅ 备库已追上主库，无需重搭")
        return True, f"延迟已恢复到 {final_lag}s"

    # 延迟仍然过大，执行重建
    print("\n⚠️  延迟仍然过大，执行备库重建...")
    return rebuild_from_master(
        slave_host=slave_host,
        slave_port=slave_port,
        slave_user=slave_user,
        slave_password=slave_password,
        master_host=master_host,
        master_port=master_port,
        master_user=master_user,
        master_password=master_password,
    )


def _rebuild_for_crash(
    master_host: str,
    slave_host: str,
    master_port: int,
    slave_port: int,
    master_user: str,
    master_password: Optional[str],
    slave_user: str,
    slave_password: Optional[str],
) -> tuple[bool, str]:
    """备库崩溃后重建。"""
    print("\n📋  场景: 备库崩溃恢复")

    # 检查备库是否可连接
    slave_pwd = slave_password or os.getenv("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=slave_pwd,
            connect_timeout=5,
        )
        conn.close()
        print("  备库可连接，尝试重启复制...")
    except Exception:
        print("  备库不可连接，需要完全重建")

    return rebuild_from_master(
        slave_host=slave_host,
        slave_port=slave_port,
        slave_user=slave_user,
        slave_password=slave_password,
        master_host=master_host,
        master_port=master_port,
        master_user=master_user,
        master_password=master_password,
    )


def _rebuild_for_newhost(
    master_host: str,
    slave_host: str,
    master_port: int,
    slave_port: int,
    master_user: str,
    master_password: Optional[str],
    slave_user: str,
    slave_password: Optional[str],
) -> tuple[bool, str]:
    """迁移到新主机。"""
    print("\n📋  场景: 迁移到新主机")
    print(f"  新备库: {slave_host}:{slave_port}")

    # 前置检查
    os_info = detect_os()
    start_cmd, stop_cmd = _mysql_service_name(os_info.family)

    # 确保新备库的 MySQL 已安装
    try:
        check_conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=slave_password or os.getenv("MYSQL_PASSWORD"),
            connect_timeout=5,
        )
        check_conn.close()
        print("  ✅ 新备库 MySQL 已安装")
    except Exception as e:
        print(f"  ❌ 新备库 MySQL 未安装或无法连接: {e}")
        return False, f"新备库 MySQL 未安装: {e}"

    return rebuild_from_master(
        slave_host=slave_host,
        slave_port=slave_port,
        slave_user=slave_user,
        slave_password=slave_password,
        master_host=master_host,
        master_port=master_port,
        master_user=master_user,
        master_password=master_password,
    )


def _rebuild_remote(
    reason: str,
    master_host: str,
    slave_host: str,
    master_port: int,
    slave_port: int,
    master_user: str,
    master_password: Optional[str],
    slave_user: str,
    slave_password: Optional[str],
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: Optional[str],
    ssh_key: Optional[str],
) -> tuple[bool, str]:
    """SSH 远程模式重搭。"""
    from ..lib.ssh_client import SSHClient, PARAMIKO_AVAILABLE

    if not PARAMIKO_AVAILABLE:
        return False, "Paramiko 未安装，无法使用 SSH 远程功能"

    print(f"\n📡  通过 SSH 连接到远程主机: {ssh_host}")

    try:
        client = SSHClient()
        client.connect(
            host=ssh_host,
            port=ssh_port,
            user=ssh_user,
            password=ssh_password,
            key_file=ssh_key,
        )

        # 构造远程命令
        remote_cmd = [
            "python3 -c",
            f"\"from ops_db.modules.rebuild import rebuild; ",
            f"rebuild(",
            f"reason='{reason}',",
            f"master_host='{master_host}',",
            f"master_port={master_port},",
            f"slave_host='{slave_host}',",
            f"slave_port={slave_port},",
            f"master_user='{master_user}',",
            f"slave_user='{slave_user}',",
            f")\"",
        ]

        result = client.exec_command(" ".join(remote_cmd), timeout=7200)

        if result.success:
            return True, f"远程重搭完成"
        else:
            return False, f"远程重搭失败: {result.stderr}"

    except Exception as e:
        return False, f"SSH 远程执行失败: {e}"
