"""前置检查模块 — 磁盘/依赖/端口/权限等检查。"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
from dataclasses import dataclass, field
from typing import Optional

from .logger import get_logger
from .system_detect import check_command_exists

logger = get_logger(__name__)


@dataclass
class CheckResult:
    item: str
    status: str          # PASS / WARN / FAIL
    message: str
    suggestion: str = ""


@dataclass
class PreflightReport:
    results: list[CheckResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.status in ("PASS", "WARN") for r in self.results)

    @property
    def has_fatal(self) -> bool:
        return any(r.status == "FAIL" for r in self.results)

    def summary(self) -> str:
        lines = ["=" * 40, "前置检查报告", "=" * 40]
        for r in self.results:
            icon = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗"}[r.status]
            lines.append(f"  [{icon}] {r.item}: {r.message}")
            if r.suggestion:
                lines.append(f"       → {r.suggestion}")
        lines.append("=" * 40)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# 通用检查项
# ---------------------------------------------------------------------------

def check_disk_space(path: str, required_gb: float) -> CheckResult:
    """检查路径所在分区的可用空间是否满足要求。"""
    try:
        stat = shutil.disk_usage(path)
        free_gb = stat.free / (1024 ** 3)
        total_gb = stat.total / (1024 ** 3)
        used_pct = (stat.used / stat.total) * 100

        if free_gb >= required_gb:
            return CheckResult(
                item="磁盘空间",
                status="PASS",
                message=f"{path} 可用 {free_gb:.1f}GB / 总计 {total_gb:.1f}GB (已用 {used_pct:.0f}%)",
            )
        else:
            return CheckResult(
                item="磁盘空间",
                status="FAIL",
                message=f"{path} 可用空间 {free_gb:.1f}GB 不足，需要 {required_gb:.1f}GB",
                suggestion=f"清理磁盘或扩大分区：df -h {path}",
            )
    except Exception as e:
        return CheckResult(
            item="磁盘空间",
            status="FAIL",
            message=f"检查磁盘空间失败: {e}",
        )


def check_port_available(port: int, host: str = "127.0.0.1") -> CheckResult:
    """检查端口是否已被占用。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    try:
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            return CheckResult(
                item="端口检测",
                status="FAIL",
                message=f"{host}:{port} 已被占用",
                suggestion=f"使用另一个端口，或停止已有服务：lsof -i :{port}",
            )
        return CheckResult(
            item="端口检测",
            status="PASS",
            message=f"{host}:{port} 可用",
        )
    except Exception as e:
        return CheckResult(
            item="端口检测",
            status="FAIL",
            message=f"端口检测失败: {e}",
        )


def check_root() -> CheckResult:
    """检查是否以 root 身份运行。"""
    if os.geteuid() == 0:
        return CheckResult(item="权限", status="PASS", message="当前以 root 身份运行")
    return CheckResult(
        item="权限",
        status="FAIL",
        message="当前非 root 身份，部分操作（如安装服务）需要 sudo",
        suggestion="使用 sudo 运行，或切换到 root 用户",
    )


def check_command(cmd: str, package_hint: str = "") -> CheckResult:
    """检查命令是否存在。"""
    if check_command_exists(cmd):
        return CheckResult(item=f"命令 {cmd}", status="PASS", message=f"{cmd} 已安装")
    suggestion = f"安装 {package_hint or cmd}" if package_hint else ""
    return CheckResult(
        item=f"命令 {cmd}",
        status="FAIL",
        message=f"{cmd} 未找到",
        suggestion=suggestion,
    )


def check_xtrabackup() -> CheckResult:
    """检查 xtrabackup 是否已安装，返回可用版本。"""
    result = check_command("xtrabackup", "xtrabackup")
    if result.status == "PASS":
        # 获取版本
        try:
            cp = subprocess.run(
                ["xtrabackup", "--version"],
                capture_output=True, text=True,
            )
            version = cp.stdout.strip().split()[-1]
            result.message = f"xtrabackup {version} 已安装"
        except Exception:
            result.message = "xtrabackup 已安装（版本未知）"
    return result


def check_mysqldump() -> CheckResult:
    """检查 mysqldump 是否可用。"""
    return check_command("mysqldump", "mysql-client")


def check_mysql_client() -> CheckResult:
    """检查 mysql client 是否可用。"""
    result = check_command("mysql", "mysql-client")
    if result.status == "PASS":
        try:
            cp = subprocess.run(
                ["mysql", "--version"],
                capture_output=True, text=True,
            )
            result.message = f"mysql client {cp.stdout.strip().split()[-1]} 已安装"
        except Exception:
            result.message = "mysql client 已安装"
    return result


def check_data_dir_writable(datadir: str) -> CheckResult:
    """检查 data 目录是否可写。"""
    # 先确保目录存在
    os.makedirs(datadir, exist_ok=True)
    if os.access(datadir, os.W_OK):
        return CheckResult(
            item="目录权限",
            status="PASS",
            message=f"{datadir} 可写",
        )
    return CheckResult(
        item="目录权限",
        status="FAIL",
        message=f"{datadir} 不可写",
        suggestion=f"修改权限：chown -R mysql:mysql {datadir}",
    )


def check_selinux() -> CheckResult:
    """检查 SELinux 状态。"""
    if not os.path.exists("/usr/sbin/getenforce"):
        return CheckResult(item="SELinux", status="PASS", message="SELinux 未安装")

    try:
        cp = subprocess.run(
            ["getenforce"],
            capture_output=True, text=True,
        )
        status = cp.stdout.strip()
        if status == "Enforcing":
            return CheckResult(
                item="SELinux",
                status="WARN",
                message="SELinux 当前为 Enforcing 模式",
                suggestion="MySQL 可能在 SELinux 下遇到权限问题，建议设为 Permissive 或配置布尔值",
            )
        return CheckResult(item="SELinux", status="PASS", message=f"SELinux {status}")
    except Exception:
        return CheckResult(item="SELinux", status="WARN", message="SELinux 状态未知")


def check_firewalld() -> CheckResult:
    """检查防火墙状态。"""
    if not check_command_exists("firewall-cmd"):
        return CheckResult(item="防火墙", status="PASS", message="firewalld 未安装")

    try:
        cp = subprocess.run(
            ["firewall-cmd", "--state"],
            capture_output=True, text=True,
        )
        if cp.stdout.strip() == "running":
            return CheckResult(
                item="防火墙",
                status="WARN",
                message="firewalld 正在运行",
                suggestion="MySQL 端口（如 3306）可能需要手动放行：firewall-cmd --add-port=3306/tcp --permanent",
            )
        return CheckResult(item="防火墙", status="PASS", message="firewalld 未运行")
    except Exception:
        return CheckResult(item="防火墙", status="WARN", message="防火墙状态未知")


def check_mysql_running(port: int = 3306) -> CheckResult:
    """检查 MySQL 是否已在运行。"""
    result = check_port_available(port)
    if result.status == "PASS":
        return CheckResult(item="MySQL 进程", status="PASS", message=f"端口 {port} 未被占用，无 MySQL 运行")
    return CheckResult(
        item="MySQL 进程",
        status="WARN",
        message=f"端口 {port} 已被占用，MySQL 可能在运行",
        suggestion="如需安装，请先停止已有 MySQL 或使用不同端口",
    )


# ---------------------------------------------------------------------------
# 组合检查
# ---------------------------------------------------------------------------

def run_preflight_checks(
    actions: list[str],
    *,
    port: int = 3306,
    datadir: str = "/var/lib/mysql",
    backup_dest: Optional[str] = None,
) -> PreflightReport:
    """
    执行前置检查。

    :param actions: 需要的动作列表，如 ["install", "backup", "restore"]
    :param port: MySQL 端口
    :param datadir: MySQL 数据目录
    :param backup_dest: 备份目标路径（backup 时检查）
    """
    report = PreflightReport()

    # install 需要检查的内容
    if "install" in actions:
        report.results.append(check_root())
        report.results.append(check_mysql_running(port))
        report.results.append(check_port_available(port))
        report.results.append(check_data_dir_writable(datadir))
        report.results.append(check_selinux())
        report.results.append(check_firewalld())
        report.results.append(check_disk_space(datadir, required_gb=5.0))
        report.results.append(check_xtrabackup())
        report.results.append(check_mysqldump())

    # backup 需要检查的内容
    if "backup" in actions:
        report.results.append(check_root())
        report.results.append(check_mysql_client())
        report.results.append(check_xtrabackup())
        if backup_dest:
            report.results.append(check_disk_space(backup_dest, required_gb=1.0))
        report.results.append(check_port_available(port))  # 检测连接时用

    # restore 需要检查的内容
    if "restore" in actions:
        report.results.append(check_root())
        report.results.append(check_mysql_running(port))  # restore 需要先停止 MySQL
        report.results.append(check_xtrabackup())
        if backup_dest:
            report.results.append(check_disk_space(datadir, required_gb=1.0))

    return report
