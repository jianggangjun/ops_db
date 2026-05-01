"""MySQL 主从复制配置模块。"""

from __future__ import annotations

import subprocess
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
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")
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
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")
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
            # 获取 GTID 模式
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
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")
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


def _ensure_repl_user(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    repl_user: str = "repl",
    repl_password: str = "",
    repl_host: str = "%",
) -> tuple[bool, str]:
    """确保主库有复制账户。"""
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            connect_timeout=10,
        )
        with conn.cursor() as cur:
            # 检查是否存在
            cur.execute(
                f"SELECT user, host FROM mysql.user WHERE user='{repl_user}' AND host='{repl_host}'"
            )
            existing = cur.fetchone()

            if existing:
                logger.info(f"复制账户 {repl_user}@{repl_host} 已存在")
                # 更新密码
                if repl_password:
                    cur.execute(
                        f"ALTER USER '{repl_user}'@'{repl_host}' IDENTIFIED BY '{repl_password}'"
                    )
                    conn.commit()
                    logger.info(f"已更新复制账户密码")
            else:
                # 创建新账户
                if not repl_password:
                    return False, f"复制账户不存在，请提供 --repl-password"

                cur.execute(
                    f"CREATE USER '{repl_user}'@'{repl_host}' "
                    f"IDENTIFIED BY '{repl_password}'"
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
    status: dict,
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
# 主从配置
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
) -> tuple[bool, str]:
    """
    配置 MySQL 主从复制。

    流程：
    1. 前置检查（版本、server-id、端口连通性）
    2. 如果备库未安装，先安装
    3. 获取主库复制坐标
    4. 确保复制账户存在
    5. 在备库执行 CHANGE MASTER TO
    6. 启动复制并验证

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
    :return (success, message)
    """
    master_pwd = master_password or __import__("os").getenv("MYSQL_PASSWORD")
    slave_pwd = slave_password or __import__("os").getenv("MYSQL_PASSWORD")
    repl_pwd = repl_password or __import__("os").getenv("REPL_PASSWORD")

    print("\n" + "=" * 60)
    print("🔄  MySQL 主从复制配置")
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

    # 检查备库连通性
    slave_status = _get_slave_status(slave_host, slave_port, slave_user, slave_pwd)
    check_slave_conn = _check_server_id(slave_host, slave_port, slave_user, slave_pwd)
    if not check_slave_conn:
        report.results.append(CheckResult(
            item="备库连接",
            status="FAIL",
            message=f"无法连接到 {slave_host}:{slave_port}",
            suggestion="检查备库是否运行、端口是否可达、用户名密码是否正确",
        ))

    # 检查 server-id 唯一性
    if master_status:
        master_server_id = _check_server_id(master_host, master_port, master_user, master_pwd)
        slave_server_id = check_slave_conn
        if master_server_id and slave_server_id:
            if master_server_id == slave_server_id:
                report.results.append(CheckResult(
                    item="server-id",
                    status="FAIL",
                    message=f"主从 server-id 相同 ({master_server_id})",
                    suggestion="修改备库 server-id，确保两库不重复",
                ))
            else:
                report.results.append(CheckResult(
                    item="server-id",
                    status="PASS",
                    message=f"主库={master_server_id}, 备库={slave_server_id}",
                    suggestion="",
                ))

    # 检查 binlog 是否开启
    if master_status:
        binlog_file = master_status.get("File") or master_status.get("binlog_file")
        if binlog_file:
            report.results.append(CheckResult(
                item="binlog",
                status="PASS",
                message=f"binlog 已开启: {binlog_file}",
                suggestion="",
            ))
        else:
            report.results.append(CheckResult(
                item="binlog",
                status="WARN",
                message="主库 binlog 未开启",
                suggestion="建议开启 binlog，否则无法做主从复制",
            ))

        # GTID 模式
        gtid_mode = master_status.get("gtid_mode", "OFF")
        gtid_consistency = master_status.get("gtid_consistency", "OFF")
        gtid_enabled = gtid_mode == "ON" and gtid_consistency == "ON"
        report.results.append(CheckResult(
            item="GTID 模式",
            status="PASS" if gtid_enabled else "INFO",
            message=f"gtid_mode={gtid_mode}, enforce_gtid_consistency={gtid_consistency}",
            suggestion="",
        ))

    _print_status_table(report.results)

    if report.has_fatal:
        print("❌ 前置检查失败，请修复上述问题后重试")
        return False, "前置检查失败"

    # 如果备库未安装，提示先安装
    if not check_slave_conn:
        print("❌ 备库未安装或无法连接")
        print(f"\n请先安装备库 MySQL:")
        print(f"  ops_db.py install --version 8.0 \\")
        print(f"    --host {slave_host} \\")
        print(f"    --port {slave_port} \\")
        print(f"    --role slave")
        return False, "备库未安装，请先执行 install 命令"

    # 确认操作
    if not yes:
        confirm = input("确认配置主从复制？输入 'yes' 确认: ").strip()
        if confirm != "yes":
            return False, "用户取消"

    # 1. 确保主库有复制账户
    print("\n📝 步骤 1/4: 配置复制账户...")
    ok, msg = _ensure_repl_user(
        master_host, master_port, master_user, master_pwd,
        repl_user, repl_pwd or "", repl_host
    )
    if not ok:
        print(f"❌ {msg}")
        return False, msg
    print(f"✅ {msg}")

    # 2. 获取主库复制坐标
    print("\n📝 步骤 2/4: 获取主库复制坐标...")
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
        print(f"  GTID 集合 : {gtid_set[:60]}..." if len(str(gtid_set)) > 60 else f"  GTID 集合 : {gtid_set}")

    # 3. 在备库执行 CHANGE MASTER TO
    print("\n📝 步骤 3/4: 配置备库复制...")

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

    # 4. 启动复制
    print("\n📝 步骤 4/4: 启动复制...")
    try:
        with slave_conn.cursor() as cur:
            cur.execute("START SLAVE")
            slave_conn.commit()
        print("✅ START SLAVE 执行成功")
    except Exception as e:
        slave_conn.close()
        return False, f"启动复制失败: {e}"

    # 验证
    import time
    time.sleep(2)  # 等待复制启动

    final_status = _get_slave_status(slave_host, slave_port, slave_user, slave_pwd)
    slave_conn.close()

    _print_replication_result(master_host, slave_host, gtid_enabled, final_status)

    if final_status:
        io_running = final_status.get("Slave_IO_Running", "No")
        sql_running = final_status.get("Slave_SQL_Running", "No")
        if io_running == "Yes" and sql_running == "Yes":
            return True, f"主从复制配置成功"
        else:
            return False, "复制未正常启动，请检查 SHOW SLAVE STATUS"

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
    :return (success, message)
    """
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")

    print("\n" + "=" * 60)
    print("🔍  主从复制状态检查")
    print("=" * 60)
    print(f"  备库 : {host}:{port}")
    print("=" * 60)

    status = _get_slave_status(host, port, user, pwd)
    if not status:
        print("❌ 无法获取复制状态（可能不是备库）")
        return False, "不是备库或无法连接"

    # 打印详细信息
    io_running = status.get("Slave_IO_Running", "No")
    sql_running = status.get("Slave_SQL_Running", "No")
    behind = status.get("Seconds_Behind_Master")

    print(f"  Master Host    : {status.get('Master_Host', 'N/A')}")
    print(f"  Master Port   : {status.get('Master_Port', 'N/A')}")
    print(f"  Master Log File: {status.get('Master_Log_File', 'N/A')}")
    print(f"  Read Master Log: {status.get('Read_Master_Log_Pos', 'N/A')}")
    print(f"  Relay Log File : {status.get('Relay_Log_File', 'N/A')}")
    print(f"  Relay Log Pos  : {status.get('Relay_Log_Pos', 'N/A')}")
    print()

    io_icon = "✅" if io_running == "Yes" else "❌"
    sql_icon = "✅" if sql_running == "Yes" else "❌"
    print(f"  {io_icon} IO 线程   : {io_running}")
    print(f"  {sql_icon} SQL 线程  : {sql_running}")
    if behind is not None:
        behind_icon = "✅" if behind == 0 else "⚠️ "
        print(f"  {behind_icon} 复制延时  : {behind} 秒")

    # GTID 模式
    auto_pos = status.get("Auto_Position", 0)
    print(f"  GTID 自动定位 : {'是' if auto_pos == 1 else '否'}")

    # 错误信息
    last_error = status.get("Last_Error", "")
    if last_error:
        print(f"\n  ❌ 最后错误: {last_error[:100]}")

    # GTID 状态
    gtid_retrieved = status.get("Gtid_IO_Pos", "")
    if gtid_retrieved:
        print(f"  GTID IO Pos   : {gtid_retrieved[:40]}...")

    print("=" * 60 + "\n")

    if io_running == "Yes" and sql_running == "Yes":
        if behind and behind > 0:
            return False, f"复制正常但有延时 ({behind} 秒)"
        return True, "复制正常"
    else:
        return False, f"复制异常: IO={io_running}, SQL={sql_running}"