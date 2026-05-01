"""MySQL 健康检查模块。"""

from __future__ import annotations

import subprocess
from datetime import datetime, timedelta
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
    check: bool = False,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    """执行 shell 命令。"""
    cp = subprocess.run(
        cmd,
        shell=True,
        timeout=timeout,
        capture_output=capture_output,
        text=True,
    )
    return cp


# ---------------------------------------------------------------------------
# 检查项
# ---------------------------------------------------------------------------

def check_connectivity(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查 MySQL 连接性。"""
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
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
        conn.close()
        return CheckResult(
            item="连接性",
            status="PASS",
            message=f"MySQL {version} 可连接 {host}:{port}",
            suggestion="",
        )
    except Exception as e:
        return CheckResult(
            item="连接性",
            status="FAIL",
            message=f"无法连接 {host}:{port}: {e}",
            suggestion="检查 MySQL 是否运行、端口是否可达、用户名密码是否正确",
        )


def check_version(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查 MySQL 版本。"""
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
            cur.execute("SELECT VERSION()")
            version = cur.fetchone()[0]
        conn.close()

        # 检查版本是否在支持列表内
        major = version.split(".")[0] if version else ""
        if major in ("5.7", "8.0", "8.4", "9.0"):
            return CheckResult(
                item="版本",
                status="PASS",
                message=f"MySQL {version}",
                suggestion="",
            )
        else:
            return CheckResult(
                item="版本",
                status="WARN",
                message=f"MySQL {version}（较老或较新版本）",
                suggestion="建议使用 5.7 / 8.0 / 8.4 / 9.0 LTS 版本",
            )
    except Exception as e:
        return CheckResult(
            item="版本",
            status="WARN",
            message=f"无法获取版本: {e}",
            suggestion="",
        )


def check_replication(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查主从复制状态。"""
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
            status = cur.fetchone()
        conn.close()

        if not status:
            return CheckResult(
                item="复制状态",
                status="INFO",
                message="非备库（无复制配置）",
                suggestion="",
            )

        io_running = status.get("Slave_IO_Running", "No")
        sql_running = status.get("Slave_SQL_Running", "No")
        behind = status.get("Seconds_Behind_Master")

        # 判断状态
        if io_running == "Yes" and sql_running == "Yes":
            if behind and behind > 0:
                if behind > 1800:  # 30分钟
                    return CheckResult(
                        item="复制状态",
                        status="FAIL",
                        message=f"复制延时严重: {behind} 秒（约 {behind // 60} 分钟）",
                        suggestion="建议使用 ops_db rebuild 重搭备库",
                    )
                elif behind > 300:  # 5分钟
                    return CheckResult(
                        item="复制状态",
                        status="WARN",
                        message=f"复制延时: {behind} 秒（约 {behind // 60} 分钟）",
                        suggestion="可观察一段时间，如持续增大建议重建",
                    )
                else:
                    return CheckResult(
                        item="复制状态",
                        status="PASS",
                        message=f"复制正常，延时 {behind} 秒",
                        suggestion="",
                    )
            else:
                return CheckResult(
                    item="复制状态",
                    status="PASS",
                    message="复制正常，无延时",
                    suggestion="",
                )
        else:
            last_error = status.get("Last_Error", "未知错误")
            return CheckResult(
                item="复制状态",
                status="FAIL",
                message=f"复制异常: IO={io_running}, SQL={sql_running}",
                suggestion=f"错误: {last_error[:100] if last_error else '未知'}",
            )

    except Exception as e:
        return CheckResult(
            item="复制状态",
            status="WARN",
            message=f"无法获取复制状态: {e}",
            suggestion="",
        )


def check_slow_queries(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
    hours: int = 24,
) -> CheckResult:
    """检查慢查询数量。"""
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
            cur.execute(
                "SELECT COUNT(*) FROM mysql.slow_log "
                f"WHERE start_time >= DATE_SUB(NOW(), INTERVAL {hours} HOUR)"
            )
            count = cur.fetchone()[0]
        conn.close()

        if count > 5000:
            return CheckResult(
                item=f"慢查询（{hours}h）",
                status="FAIL",
                message=f"慢查询过多: {count} 条",
                suggestion="优化慢查询，或调整 long_query_time",
            )
        elif count > 1000:
            return CheckResult(
                item=f"慢查询（{hours}h）",
                status="WARN",
                message=f"慢查询较多: {count} 条",
                suggestion="关注高频慢查询",
            )
        else:
            return CheckResult(
                item=f"慢查询（{hours}h）",
                status="PASS",
                message=f"慢查询正常: {count} 条",
                suggestion="",
            )
    except Exception as e:
        return CheckResult(
            item=f"慢查询（{hours}h）",
            status="WARN",
            message=f"无法获取慢查询: {e}",
            suggestion="",
        )


def check_connections(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查连接数使用率。"""
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
            # 获取当前连接数
            cur.execute("SHOW STATUS LIKE 'Threads_connected'")
            connected = int(cur.fetchone()["Value"])

            # 获取最大连接数
            cur.execute("SHOW VARIABLES LIKE 'max_connections'")
            max_conn = int(cur.fetchone()["Value"])

        conn.close()

        usage = (connected / max_conn) * 100 if max_conn > 0 else 0

        if usage > 90:
            return CheckResult(
                item="连接数",
                status="FAIL",
                message=f"连接数过高: {connected}/{max_conn} ({usage:.1f}%)",
                suggestion="检查连接泄漏或增加 max_connections",
            )
        elif usage > 80:
            return CheckResult(
                item="连接数",
                status="WARN",
                message=f"连接数较高: {connected}/{max_conn} ({usage:.1f}%)",
                suggestion="关注连接增长趋势",
            )
        else:
            return CheckResult(
                item="连接数",
                status="PASS",
                message=f"连接数正常: {connected}/{max_conn} ({usage:.1f}%)",
                suggestion="",
            )
    except Exception as e:
        return CheckResult(
            item="连接数",
            status="WARN",
            message=f"无法获取连接数: {e}",
            suggestion="",
        )


def check_lock_waits(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查锁等待情况。"""
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
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.INNODB_LOCK_WAITS"
            )
            count = cur.fetchone()[0]
        conn.close()

        if count > 50:
            return CheckResult(
                item="锁等待",
                status="FAIL",
                message=f"锁等待严重: {count} 个",
                suggestion="检查长时间锁事务，可能需要 kill",
            )
        elif count > 10:
            return CheckResult(
                item="锁等待",
                status="WARN",
                message=f"有锁等待: {count} 个",
                suggestion="关注是否有阻塞",
            )
        else:
            return CheckResult(
                item="锁等待",
                status="PASS",
                message=f"无锁等待",
                suggestion="",
            )
    except Exception:
        # 可能是权限不足或非 InnoDB 表
        return CheckResult(
            item="锁等待",
            status="INFO",
            message="无法检查（权限不足或非 InnoDB）",
            suggestion="",
        )


def check_disk_usage(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查数据目录磁盘使用率。"""
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
            cur.execute("SHOW VARIABLES LIKE 'datadir'")
            datadir = cur.fetchone()["Value"]
        conn.close()

        # 检查磁盘使用率
        cp = run_command(f"df -h {datadir}")
        lines = cp.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            usage_str = parts[4] if len(parts) > 4 else "0%"
            usage = int(usage_str.rstrip("%"))

            if usage > 95:
                return CheckResult(
                    item="磁盘使用率",
                    status="FAIL",
                    message=f"磁盘空间严重不足: {usage_str}",
                    suggestion="立即清理或扩容",
                )
            elif usage > 85:
                return CheckResult(
                    item="磁盘使用率",
                    status="WARN",
                    message=f"磁盘使用率较高: {usage_str}",
                    suggestion="关注并考虑清理",
                )
            else:
                return CheckResult(
                    item="磁盘使用率",
                    status="PASS",
                    message=f"磁盘空间充足: {usage_str}",
                    suggestion="",
                )

        return CheckResult(
            item="磁盘使用率",
            status="WARN",
            message="无法获取磁盘信息",
            suggestion="",
        )
    except Exception as e:
        return CheckResult(
            item="磁盘使用率",
            status="WARN",
            message=f"无法获取磁盘信息: {e}",
            suggestion="",
        )


def check_gtid_mode(
    host: str,
    port: int,
    user: str,
    password: Optional[str] = None,
) -> CheckResult:
    """检查 GTID 模式。"""
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
            cur.execute("SELECT @@GLOBAL.gtid_mode AS gtid_mode, "
                       "@@GLOBAL.enforce_gtid_consistency AS gtid_consistency")
            row = cur.fetchone()
        conn.close()

        gtid_mode = row["gtid_mode"] if row else "OFF"
        gtid_consistency = row["gtid_consistency"] if row else "OFF"

        if gtid_mode == "ON" and gtid_consistency == "ON":
            status = "PASS"
            message = "GTID 模式已开启"
        elif gtid_mode == "OFF":
            status = "WARN"
            message = "GTID 模式未开启"
        else:
            status = "INFO"
            message = f"GTID 部分开启: mode={gtid_mode}, consistency={gtid_consistency}"

        return CheckResult(
            item="GTID 模式",
            status=status,
            message=message,
            suggestion="",
        )
    except Exception as e:
        return CheckResult(
            item="GTID 模式",
            status="WARN",
            message=f"无法获取 GTID 状态: {e}",
            suggestion="",
        )


# ---------------------------------------------------------------------------
# 主检查函数
# ---------------------------------------------------------------------------

def check(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    check_replication: bool = True,
    check_performance: bool = True,
) -> tuple[bool, str]:
    """
    MySQL 健康检查。

    检查项：
    1. 连接性（PASS/WARN/FAIL）
    2. 版本（PASS/WARN）
    3. 复制状态（PASS/WARN/FAIL）— 需要 check_replication=True
    4. 慢查询（PASS/WARN/FAIL）— 需要 check_performance=True
    5. 连接数（PASS/WARN/FAIL）
    6. 锁等待（PASS/WARN/FAIL）
    7. 磁盘使用率（PASS/WARN/FAIL）
    8. GTID 模式（PASS/WARN/INFO）

    :param host: MySQL 主机
    :param port: MySQL 端口
    :param user: MySQL 用户
    :param password: 密码
    :param check_replication: 是否检查复制状态
    :param check_performance: 是否检查性能指标
    :return (success, summary)
    """
    pwd = password or __import__("os").getenv("MYSQL_PASSWORD")

    print("\n" + "=" * 60)
    print("🔍  MySQL 健康检查")
    print("=" * 60)
    print(f"  主机 : {host}:{port}")
    print(f"  用户 : {user}")
    print(f"  时间 : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")

    results: list[CheckResult] = []

    # 基础检查（必做）
    print("📋 执行检查项...")

    # 1. 连接性
    result = check_connectivity(host, port, user, pwd)
    results.append(result)
    print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

    # 如果连接失败，跳过后续检查
    if result.status == "FAIL":
        print("\n❌ 无法连接 MySQL，跳过其他检查项")
        print_summary(results)
        return False, "连接失败"

    # 2. 版本
    result = check_version(host, port, user, pwd)
    results.append(result)
    print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

    # 3. GTID 模式
    result = check_gtid_mode(host, port, user, pwd)
    results.append(result)
    print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

    # 4. 复制状态（可选）
    if check_replication:
        result = check_replication(host, port, user, pwd)
        results.append(result)
        print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

    # 5. 性能检查（可选）
    if check_performance:
        result = check_slow_queries(host, port, user, pwd)
        results.append(result)
        print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

        result = check_connections(host, port, user, pwd)
        results.append(result)
        print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

        result = check_lock_waits(host, port, user, pwd)
        results.append(result)
        print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

        result = check_disk_usage(host, port, user, pwd)
        results.append(result)
        print(f"  {'✅' if result.status == 'PASS' else '⚠️ ' if result.status == 'WARN' else '❌'} {result.item}: {result.message}")

    print("\n")
    return print_summary(results)


def print_summary(results: list[CheckResult]) -> tuple[bool, str]:
    """打印检查汇总。"""
    passed = sum(1 for r in results if r.status == "PASS")
    warned = sum(1 for r in results if r.status == "WARN")
    failed = sum(1 for r in results if r.status == "FAIL")
    total = len(results)

    print("=" * 60)
    print("📊  检查汇总")
    print("=" * 60)
    print(f"  ✅ 通过 : {passed}/{total}")
    print(f"  ⚠️  警告 : {warned}/{total}")
    print(f"  ❌ 失败 : {failed}/{total}")
    print("=" * 60)

    # 列出有问题的项
    issues = [r for r in results if r.status in ("WARN", "FAIL")]
    if issues:
        print("\n📝 问题详情:")
        for r in issues:
            icon = "⚠️ " if r.status == "WARN" else "❌"
            print(f"  {icon} [{r.item}] {r.message}")
            if r.suggestion:
                print(f"      建议: {r.suggestion}")

    print("=" * 60 + "\n")

    if failed > 0:
        return False, f"检查失败: {failed} 项"
    elif warned > 0:
        return False, f"检查警告: {warned} 项"
    else:
        return True, f"检查通过: {passed} 项"