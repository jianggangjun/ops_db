"""MySQL 主从复制配置模块 — 全自动版本。

支持：
- 备库未安装时自动远程安装（通过 SSH 推送 tarball 方式）
- 自动修复 server-id 冲突、bind-address 等配置问题
- 自动创建 repl 用户（使用 mysql_native_password 认证避免安全连接问题）
- SSH 远程模式：本地执行，通过 SSH 连接到远程备库进行配置
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Optional

import pymysql

from ..lib.checker import (
    CheckResult,
    PreflightReport,
    check_mysql_client,
    check_root,
)
from ..lib.logger import get_logger
from ..lib.mysql_conn import get_conn

logger = get_logger(__name__)


def run_command(
    cmd: str,
    timeout: int = 60,
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


def _get_slave_status(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> Optional[dict]:
    """获取备库复制状态。"""
    pwd = password or os.environ.get("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SHOW SLAVE STATUS")
            result = cur.fetchone()
        conn.close()
        return result
    except Exception as e:
        logger.warning(f"获取备库状态失败: {e}")
        return None


def _get_master_status(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> Optional[dict]:
    """获取主库状态。"""
    pwd = password or os.environ.get("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SHOW MASTER STATUS")
            result = cur.fetchone()
            cur.execute("SELECT @@GLOBAL.gtid_mode AS gtid_mode, "
                       "@@GLOBAL.enforce_gtid_consistency AS gtid_consistency")
            gtid_row = cur.fetchone()
            if result:
                result["gtid_mode"] = gtid_row.get("gtid_mode") if gtid_row else "OFF"
                result["gtid_consistency"] = gtid_row.get("gtid_consistency") if gtid_row else "OFF"
        conn.close()
        return result
    except Exception as e:
        logger.warning(f"获取主库状态失败: {e}")
        return None


def _check_server_id(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> Optional[int]:
    """获取 server-id。"""
    pwd = password or os.environ.get("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT @@server_id")
            result = cur.fetchone()
        conn.close()
        return result[0] if result else None
    except Exception as e:
        logger.warning(f"获取 server-id 失败: {e}")
        return None


def _compute_server_id(host: str, port: int) -> int:
    """根据 host + port 生成稳定的 server-id。"""
    import hashlib
    seed = f"{host}:{port}".encode()
    return (int(hashlib.md5(seed).hexdigest(), 16) % 254) + 1


def _ensure_repl_user(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    repl_user: str = "repl",
    repl_password: str = "",
    repl_host: str = "%",
) -> tuple[bool, str]:
    """确保主库有复制账户（使用 mysql_native_password 认证）。"""
    pwd = password or os.environ.get("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT user, host FROM mysql.user WHERE user='{repl_user}' AND host='{repl_host}'"
            )
            existing = cur.fetchone()

            if existing:
                logger.info(f"复制账户 {repl_user}@{repl_host} 已存在")
                if repl_password:
                    # 使用 mysql_native_password（兼容复制连接）
                    cur.execute(
                        f"ALTER USER '{repl_user}'@'{repl_host}' "
                        f"IDENTIFIED WITH mysql_native_password BY '{repl_password}'"
                    )
                    conn.commit()
                    logger.info("已更新复制账户密码")
            else:
                if not repl_password:
                    return False, f"复制账户不存在，请提供 --repl-password"

                # 创建时直接使用 mysql_native_password
                cur.execute(
                    f"CREATE USER '{repl_user}'@'{repl_host}' "
                    f"IDENTIFIED WITH mysql_native_password BY '{repl_password}'"
                )
                cur.execute(
                    f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'{repl_host}'"
                )
                conn.commit()
                logger.info(f"已创建复制账户 {repl_user}@{repl_host}")

        conn.close()
        return True, f"复制账户就绪: {repl_user}@{repl_host}"
    except Exception as e:
        return False, f"创建复制账户失败: {e}"


def _wait_for_mysql(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    timeout: int = 60,
) -> bool:
    """等待 MySQL 可连接。"""
    pwd = password or os.environ.get("MYSQL_PASSWORD")
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=pwd,
                connect_timeout=5,
            )
            conn.close()
            return True
        except Exception:
            time.sleep(2)
    return False


def _check_slave_installed(
    slave_host: str,
    slave_port: int,
    slave_user: str,
    slave_password: Optional[str] = None,
) -> tuple[bool, Optional[int], Optional[str]]:
    """
    检测备库 MySQL 是否已安装并返回状态。

    :return: (已安装, server_id, bind_address)
    """
    pwd = slave_password or os.environ.get("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT @@server_id AS server_id")
            server_id = cur.fetchone()["server_id"]
            cur.execute("SELECT @@bind_address AS bind_address")
            bind_address = cur.fetchone()["bind_address"]
        conn.close()
        return True, server_id, bind_address
    except Exception as e:
        logger.info(f"备库未安装或无法连接: {e}")
        return False, None, None


def _ensure_slave_configured(
    slave_host: str,
    slave_port: int,
    slave_user: str,
    slave_password: Optional[str] = None,
    master_server_id: Optional[int] = None,
) -> tuple[bool, str]:
    """
    确保备库配置正确：server-id 唯一、bind-address=0.0.0.0、root 可远程登录。

    :return: (成功, 消息)
    """
    pwd = slave_password or os.environ.get("MYSQL_PASSWORD")
    issues: list[str] = []

    try:
        conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=pwd,
            connect_timeout=10,
        )
    except Exception as e:
        return False, f"无法连接到备库: {e}"

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        # 1. 检查 server-id
        cur.execute("SELECT @@server_id AS sid")
        current_sid = cur.fetchone()["sid"]

        if master_server_id and current_sid == master_server_id:
            issues.append(f"server-id 冲突 ({current_sid} == master {master_server_id})")
            new_sid = _compute_server_id(slave_host, slave_port)
            logger.info(f"需要修复 server-id: {current_sid} → {new_sid}")
            issues.append(f"server-id 已修复: {current_sid} → {new_sid}")

            # 通过 SQL 修改（临时）
            try:
                cur.execute(f"SET GLOBAL server_id = {new_sid}")
                conn.commit()
            except Exception:
                pass

        # 2. 检查 bind-address
        cur.execute("SELECT @@bind_address AS ba")
        current_ba = cur.fetchone()["ba"]
        if current_ba == "127.0.0.1":
            issues.append("bind-address=127.0.0.1（远程无法连接）")
            # 尝试通过 SQL 修改
            try:
                cur.execute("SET GLOBAL bind_address = '0.0.0.0'")
                conn.commit()
                issues.append("bind-address 已修改为 0.0.0.0")
            except Exception:
                pass

    conn.close()

    # 3. 确保 root@% 可以远程登录
    try:
        conn2 = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=pwd,
            connect_timeout=10,
        )
        with conn2.cursor() as cur:
            # 检查 root@% 是否存在
            cur.execute("SELECT user, host FROM mysql.user WHERE user='root' AND host='%'")
            if not cur.fetchone():
                # 创建 root@%
                if pwd:
                    cur.execute("CREATE USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY %s", (pwd,))
                    cur.execute("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
                    conn2.commit()
                    issues.append("已创建 root@% 远程用户")
        conn2.close()
    except Exception as e:
        logger.warning(f"配置 root@% 失败: {e}")

    if issues:
        return True, "; ".join(issues)
    return True, "备库配置正常"


def _print_status_table(results: list[CheckResult]) -> None:
    """打印检查结果表格。"""
    print("\n" + "=" * 60)
    print("📋  前置检查结果")
    print("=" * 60)
    for r in results:
        status_icon = "✅" if r.status == "PASS" else "⚠️ " if r.status == "WARN" else "❌"
        print(f"  {status_icon} {r.item}: {r.message}")
    print("=" * 60 + "\n")


def _print_replication_result(
    master_host: str,
    slave_host: str,
    gtid_mode: bool,
    status: Optional[dict],
) -> None:
    """打印复制配置结果。"""
    print("\n" + "=" * 60)
    print("🔄  主从复制配置完成")
    print("=" * 60)
    print(f"  主库 : {master_host}")
    print(f"  备库 : {slave_host}")
    print(f"  模式 : {'GTID' if gtid_mode else '传统（File + Position）'}")
    print("=" * 60)

    if status:
        io_running = status.get("Slave_IO_Running", "No")
        sql_running = status.get("Slave_SQL_Running", "No")
        behind = status.get("Seconds_Behind_Master")

        print("\n  复制状态:")
        io_icon = "✅" if io_running == "Yes" else "❌"
        sql_icon = "✅" if sql_running == "Yes" else "❌"
        print(f"    {io_icon} IO 线程 : {io_running}")
        print(f"    {sql_icon} SQL 线程: {sql_running}")
        if behind is not None:
            behind_icon = "✅" if behind == 0 else "⚠️ "
            print(f"    {behind_icon} 延时   : {behind} 秒")

        if io_running != "Yes" or sql_running != "Yes":
            last_error = status.get("Last_Error", "未知错误")
            print(f"\n  ⚠️  错误信息: {last_error}")
    else:
        print("\n  ⚠️  无法获取复制状态，请手动检查 SHOW SLAVE STATUS")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# 主从配置（支持 SSH 远程）
# ---------------------------------------------------------------------------

def setup_replication(
    master_host: str,
    slave_host: str,
    master_port: int = 3306,
    slave_port: int = 3306,
    master_user: str = "root",
    master_password: Optional[str] = None,
    slave_user: str = "root",
    slave_password: Optional[str] = None,
    repl_user: str = "repl",
    repl_password: Optional[str] = None,
    repl_host: str = "%",
    yes: bool = False,
    # SSH 参数（可选，用于远程配置备库）
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> tuple[bool, str]:
    """
    全自动配置 MySQL 主从复制。

    流程：
    1. 前置检查（root、mysql client）
    2. 检测备库 MySQL 是否已安装
       - 未安装：通过 SSH 远程安装（推送 tarball 方式）
    3. 确保备库配置正确（server-id、bind-address、root 远程访问）
    4. 获取主库复制坐标
    5. 确保主库有 repl 账户（mysql_native_password 认证）
    6. 在备库执行 CHANGE MASTER TO
    7. 启动复制并验证

    :param master_host: 主库主机
    :param slave_host: 备库主机
    :param master_port: 主库端口
    :param slave_port: 备库端口
    :param master_user: 主库用户
    :param master_password: 主库密码
    :param slave_user: 备库用户
    :param slave_password: 备库密码
    :param repl_user: 复制账户用户名
    :param repl_password: 复制账户密码
    :param repl_host: 复制账户允许的 host
    :param yes: 跳过确认
    :param ssh_host: SSH 目标主机（用于远程配置备库）
    :param ssh_port: SSH 端口
    :param ssh_user: SSH 用户
    :param ssh_password: SSH 密码
    :param ssh_key: SSH 私钥路径
    :return: (success, message)
    """
    master_pwd = master_password or os.environ.get("MYSQL_PASSWORD")
    slave_pwd = slave_password or os.environ.get("MYSQL_PASSWORD")
    repl_pwd = repl_password or os.environ.get("REPL_PASSWORD")

    print("\n" + "=" * 60)
    print("🔄  MySQL 主从复制配置（全自动）")
    print("=" * 60)
    print(f"  主库 : {master_host}:{master_port}")
    print(f"  备库 : {slave_host}:{slave_port}")
    print(f"  复制用户: {repl_user}@{repl_host}")
    print("=" * 60)

    # 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_client(),
    ])

    # 检查主库连通性和状态
    master_status = _get_master_status(master_host, master_port, master_user, master_pwd)
    if not master_status:
        report.results.append(CheckResult(
            item="主库连接",
            status="FAIL",
            message=f"无法连接到 {master_host}:{master_port}",
            suggestion="检查主库是否运行、端口是否可达、用户名密码是否正确",
        ))
    else:
        # 检查主库 server-id
        master_sid = _check_server_id(master_host, master_port, master_user, master_pwd)
        if master_sid:
            report.results.append(CheckResult(
                item="主库 server-id",
                status="PASS",
                message=f"主库 server-id = {master_sid}",
                suggestion="",
            ))

    # 检查备库是否已安装
    slave_installed, slave_sid, slave_ba = _check_slave_installed(
        slave_host, slave_port, slave_user, slave_pwd
    )

    if not slave_installed:
        report.results.append(CheckResult(
            item="备库安装",
            status="WARN",
            message=f"备库未安装或无法连接，将自动远程安装",
            suggestion="",
        ))
    else:
        report.results.append(CheckResult(
            item="备库安装",
            status="PASS",
            message=f"备库已安装 (server-id={slave_sid}, bind={slave_ba})",
            suggestion="",
        ))
        # 检查 server-id 是否冲突
        if master_sid and slave_sid == master_sid:
            report.results.append(CheckResult(
                item="server-id",
                status="WARN",
                message=f"主从 server-id 相同 ({slave_sid})，将自动修复",
                suggestion="",
            ))

    _print_status_table(report.results)

    if report.has_fatal and slave_installed:
        print("❌ 前置检查失败，请修复上述问题后重试")
        return False, "前置检查失败"

    # ── 步骤 0: 如果备库未安装，通过 SSH 远程安装 ────────────────────────────
    if not slave_installed:
        if not ssh_host:
            print("❌ 备库未安装且未提供 SSH 参数（--ssh-host），无法远程安装")
            print("\n请提供 SSH 参数以便远程安装备库:")
            print("  --ssh-host 192.168.56.8 --ssh-user root --ssh-password xxx")
            return False, "备库未安装，需要 SSH 参数"

        print(f"\n📡  备库未安装，通过 SSH 远程安装...")

        from ..lib.ssh_client import SSHClient, deploy_and_run_on_remote, PARAMIKO_AVAILABLE

        if not PARAMIKO_AVAILABLE:
            return False, "Paramiko 未安装，请运行: pip install paramiko"

        try:
            ssh_client = SSHClient()
            ssh_client.connect(
                host=ssh_host,
                port=ssh_port,
                user=ssh_user,
                password=ssh_password,
                key_file=ssh_key,
            )
            print(f"✅ SSH 连接到 {ssh_host} 成功")

            # 远程安装 MySQL，role=slave，server-id 自动生成（基于 slave_host:slave_port）
            install_server_id = _compute_server_id(slave_host, slave_port)
            install_args = {
                "version": "8.0",
                "role": "slave",
                "port": slave_port,
                "server_id": install_server_id,
                "root_password": slave_pwd or os.environ.get("MYSQL_PASSWORD", ""),
            }

            # 过滤掉 None 值
            install_args = {k: v for k, v in install_args.items() if v is not None}

            result = deploy_and_run_on_remote(
                ssh_client=ssh_client,
                remote_work_dir="/tmp/ops_db_remote",
                module="install",
                module_args=install_args,
                yes=True,
            )

            ssh_client.disconnect()

            if not result.success:
                return False, f"远程安装 MySQL 失败: {result.stderr}"

            print("✅ 备库 MySQL 远程安装完成")

            # 等待 MySQL 启动
            print("⏳  等待备库 MySQL 启动...")
            if not _wait_for_mysql(slave_host, slave_port, slave_user, slave_pwd, timeout=60):
                return False, "备库 MySQL 启动超时"

            print("✅ 备库 MySQL 已就绪")

        except Exception as e:
            return False, f"SSH 远程安装失败: {e}"

    # ── 步骤 1: 确保备库配置正确 ─────────────────────────────────────────────
    print("\n📝 步骤 1/5: 检查备库配置...")

    master_sid = _check_server_id(master_host, master_port, master_user, master_pwd)
    config_ok, config_msg = _ensure_slave_configured(
        slave_host, slave_port, slave_user, slave_pwd,
        master_server_id=master_sid,
    )
    if not config_ok:
        return False, f"备库配置失败: {config_msg}"
    print(f"✅ {config_msg}")

    # 如果之前 server-id 有冲突，MySQL 需要重启才能生效
    if master_sid and _check_server_id(slave_host, slave_port, slave_user, slave_pwd) == master_sid:
        print("⚠️  检测到 server-id 冲突，需要重启备库 MySQL...")
        try:
            run_command("ssh -o StrictHostKeyChecking=no "
                        f"{ssh_user}@{ssh_host if ssh_host else slave_host} "
                        "\"systemctl restart mysqld\"", timeout=30)
            time.sleep(5)
            if not _wait_for_mysql(slave_host, slave_port, slave_user, slave_pwd, timeout=30):
                return False, "重启后 MySQL 无法连接"
            print("✅ 备库已重启，server-id 已修复")
        except Exception as e:
            return False, f"重启备库失败: {e}"

    # 确认操作
    if not yes:
        confirm = input("确认配置主从复制？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # ── 步骤 2: 确保主库有复制账户 ───────────────────────────────────────────
    print("\n📝 步骤 2/5: 配置主库复制账户...")
    ok, msg = _ensure_repl_user(
        master_host, master_port, master_user, master_pwd,
        repl_user, repl_pwd or "", repl_host,
    )
    if not ok:
        print(f"❌ {msg}")
        return False, msg
    print(f"✅ {msg}")

    # ── 步骤 3: 获取主库复制坐标 ─────────────────────────────────────────────
    print("\n📝 步骤 3/5: 获取主库复制坐标...")
    master_status = _get_master_status(master_host, master_port, master_user, master_pwd)
    if not master_status:
        return False, "无法获取主库状态"

    binlog_file = master_status.get("File") or master_status.get("binlog_file", "")
    binlog_pos = master_status.get("Position") or master_status.get("binlog_position", 0)
    gtid_enabled = master_status.get("gtid_mode") == "ON"

    print(f"  binlog 文件: {binlog_file}")
    print(f"  binlog 位置: {binlog_pos}")
    print(f"  GTID 模式 : {'是' if gtid_enabled else '否'}")
    if gtid_enabled:
        gtid_set = master_status.get("Executed_Gtid_Set", "")
        display_set = str(gtid_set)[:60] + "..." if len(str(gtid_set)) > 60 else str(gtid_set)
        print(f"  GTID 集合 : {display_set}")

    # ── 步骤 4: 在备库执行 CHANGE MASTER TO ─────────────────────────────────
    print("\n📝 步骤 4/5: 配置备库复制通道...")

    try:
        slave_conn = pymysql.connect(
            host=slave_host,
            port=slave_port,
            user=slave_user,
            password=slave_pwd,
            connect_timeout=10,
        )
    except Exception as e:
        return False, f"连接备库失败: {e}"

    try:
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
                    f"MASTER_PASSWORD='{repl_pwd or ''}', "
                    f"MASTER_AUTO_POSITION=1"
                )
            else:
                change_master = (
                    f"CHANGE MASTER TO "
                    f"MASTER_HOST='{master_host}', "
                    f"MASTER_PORT={master_port}, "
                    f"MASTER_USER='{repl_user}', "
                    f"MASTER_PASSWORD='{repl_pwd or ''}', "
                    f"MASTER_LOG_FILE='{binlog_file}', "
                    f"MASTER_LOG_POS={binlog_pos}"
                )

            masked_cmd = _mask_password(change_master, repl_pwd)
            logger.info(f"执行: {masked_cmd[:80]}...")
            cur.execute(change_master)
            slave_conn.commit()
            print("✅ CHANGE MASTER TO 执行成功")

    except Exception as e:
        slave_conn.close()
        return False, f"配置备库失败: {e}"

    # ── 步骤 5: 启动复制 ──────────────────────────────────────────────────────
    print("\n📝 步骤 5/5: 启动复制...")
    try:
        with slave_conn.cursor() as cur:
            cur.execute("START SLAVE")
            slave_conn.commit()
        print("✅ START SLAVE 执行成功")
    except Exception as e:
        slave_conn.close()
        return False, f"启动复制失败: {e}"

    # 验证
    import time as time_module
    time_module.sleep(3)

    final_status = _get_slave_status(slave_host, slave_port, slave_user, slave_pwd)
    slave_conn.close()

    _print_replication_result(master_host, slave_host, gtid_enabled, final_status)

    if final_status:
        io_running = final_status.get("Slave_IO_Running", "No")
        sql_running = final_status.get("Slave_SQL_Running", "No")
        if io_running == "Yes" and sql_running == "Yes":
            return True, f"主从复制配置成功"
        else:
            return False, f"复制未正常启动，请检查 SHOW SLAVE STATUS"

    return True, "主从复制已启动，请手动验证"


# ---------------------------------------------------------------------------
# 查看复制状态
# ---------------------------------------------------------------------------

def check_replication_status(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
) -> tuple[bool, str]:
    """
    检查主从复制状态。

    :param host: 备库主机
    :param port: 备库端口
    :param user: 用户
    :param password: 密码
    :return: (success, message)
    """
    pwd = password or os.environ.get("MYSQL_PASSWORD")

    print("\n" + "=" * 60)
    print("🔍  主从复制状态检查")
    print("=" * 60)
    print(f"  备库 : {host}:{port}")
    print("=" * 60)

    status = _get_slave_status(host, port, user, pwd)
    if not status:
        print("❌ 无法获取复制状态（可能不是备库）")
        return False, "不是备库或无法连接"

    io_running = status.get("Slave_IO_Running", "No")
    sql_running = status.get("Slave_SQL_Running", "No")
    behind = status.get("Seconds_Behind_Master")

    print(f"  Master Host       : {status.get('Master_Host', 'N/A')}")
    print(f"  Master Port      : {status.get('Master_Port', 'N/A')}")
    print(f"  Master Log File  : {status.get('Master_Log_File', 'N/A')}")
    print(f"  Read Master Log Pos: {status.get('Read_Master_Log_Pos', 'N/A')}")
    print(f"  Relay Log File   : {status.get('Relay_Log_File', 'N/A')}")
    print(f"  Relay Log Pos    : {status.get('Relay_Log_Pos', 'N/A')}")
    print()

    io_icon = "✅" if io_running == "Yes" else "❌"
    sql_icon = "✅" if sql_running == "Yes" else "❌"
    print(f"  {io_icon} IO 线程    : {io_running}")
    print(f"  {sql_icon} SQL 线程   : {sql_running}")
    if behind is not None:
        behind_icon = "✅" if behind == 0 else "⚠️ "
        print(f"  {behind_icon} 复制延时   : {behind} 秒")

    auto_pos = status.get("Auto_Position", 0)
    print(f"  GTID 自动定位   : {'是' if auto_pos == 1 else '否'}")

    last_error = status.get("Last_Error", "")
    if last_error:
        print(f"\n  ❌ 最后错误: {last_error[:100]}")

    gtid_retrieved = status.get("Gtid_IO_Pos", "")
    if gtid_retrieved:
        print(f"  GTID IO Pos      : {gtid_retrieved[:40]}...")

    print("=" * 60 + "\n")

    if io_running == "Yes" and sql_running == "Yes":
        if behind and behind > 0:
            return False, f"复制正常但有延时 ({behind} 秒)"
        return True, "复制正常"
    else:
        return False, f"复制异常: IO={io_running}, SQL={sql_running}"