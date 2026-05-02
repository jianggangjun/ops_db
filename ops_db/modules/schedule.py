"""MySQL 备份定时调度模块 — 支持本地和远程 crontab 管理。"""

from __future__ import annotations

import os
import re
import subprocess
from typing import Optional

from ..lib.logger import get_logger

logger = get_logger(__name__)

# crontab 中 ops_db job 的标记前缀
CRON_MARKER = "ops_db_backup"


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
    return cp


def _validate_cron(cron_expr: str) -> bool:
    """简单验证 cron 表达式格式（5段）。"""
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return False
    return True


def _marker_name(name: str) -> str:
    """生成 crontab job 的标识名。"""
    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    return f"{CRON_MARKER}_{safe}"


def _is_ops_db_cron_line(line: str) -> bool:
    """判断是否 ops_db 的调度行。"""
    return CRON_MARKER in line and "python3 -m ops_db" in line


def _parse_cron_line(line: str) -> Optional[dict]:
    """解析 crontab 行，提取 job 信息。"""
    # 标准格式：cron_expr user python3 -m ops_db ... # marker_name
    parts = line.strip().split()
    if len(parts) < 6:
        return None
    # 前5段是 cron 时间表达式
    cron_expr = " ".join(parts[:5])
    # 找到 # marker
    marker_idx = -1
    for i in range(len(parts) - 1, -1, -1):
        if parts[i].startswith(f"#{CRON_MARKER}"):
            marker_idx = i
            break
    if marker_idx < 0:
        return None
    marker = parts[marker_idx].lstrip("#")
    cmd_parts = parts[5:marker_idx]
    cmd = " ".join(cmd_parts)
    return {"cron": cron_expr, "cmd": cmd, "marker": marker, "raw": line.strip()}


def _list_crons_local() -> list[dict]:
    """获取本地机器的 ops_db 调度列表。"""
    result = run_command("crontab -l", check=False)
    if result.returncode != 0:
        return []
    lines = result.stdout.split("\n")
    jobs = []
    for line in lines:
        if _is_ops_db_cron_line(line):
            parsed = _parse_cron_line(line)
            if parsed:
                jobs.append(parsed)
    return jobs


def _list_crons_remote(
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: Optional[str],
    ssh_key: Optional[str],
) -> list[dict]:
    """获取远程机器的 ops_db 调度列表。"""
    from ..lib.ssh_client import SSHClient, PARAMIKO_AVAILABLE

    if not PARAMIKO_AVAILABLE:
        logger.error("Paramiko 未安装，无法使用 SSH 远程功能")
        return []

    try:
        client = SSHClient()
        client.connect(
            host=ssh_host,
            port=ssh_port,
            user=ssh_user,
            password=ssh_password,
            key_file=ssh_key,
        )
        result = client.exec_command("crontab -l", timeout=30)
        client.disconnect()

        if not result.success:
            return []

        lines = result.stdout.split("\n")
        jobs = []
        for line in lines:
            if _is_ops_db_cron_line(line):
                parsed = _parse_cron_line(line)
                if parsed:
                    jobs.append(parsed)
        return jobs
    except Exception as e:
        logger.error(f"SSH 获取调度列表失败: {e}")
        return []


def _add_cron_local(
    cron_expr: str,
    name: str,
    full_cmd: str,
) -> tuple[bool, str]:
    """在本地机器添加 cron job。"""
    if not _validate_cron(cron_expr):
        return False, f"无效的 cron 表达式: {cron_expr}"

    marker = _marker_name(name)
    cron_line = f"{cron_expr} {full_cmd}  # {marker}"

    # 获取现有 crontab，过滤掉同名 job
    existing = run_command("crontab -l 2>/dev/null || true", check=False)
    lines = existing.stdout.split("\n") if existing.stdout else []
    new_lines = [l for l in lines if marker not in l]
    new_lines.append(cron_line)

    new_crontab = "\n".join(new_lines) + "\n"
    result = run_command(
        f"echo {repr(new_crontab)} | crontab -",
        check=False,
    )
    if result.returncode == 0:
        return True, f"调度已添加: {name} ({cron_expr})"
    return False, f"添加失败: {result.stderr}"


def _add_cron_remote(
    cron_expr: str,
    name: str,
    full_cmd: str,
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: Optional[str],
    ssh_key: Optional[str],
) -> tuple[bool, str]:
    """在远程机器添加 cron job。"""
    from ..lib.ssh_client import SSHClient, PARAMIKO_AVAILABLE

    if not PARAMIKO_AVAILABLE:
        return False, "Paramiko 未安装"

    if not _validate_cron(cron_expr):
        return False, f"无效的 cron 表达式: {cron_expr}"

    marker = _marker_name(name)
    cron_line = f"{cron_expr} {full_cmd}  # {marker}"

    try:
        client = SSHClient()
        client.connect(
            host=ssh_host,
            port=ssh_port,
            user=ssh_user,
            password=ssh_password,
            key_file=ssh_key,
        )

        # 获取现有 crontab，过滤同名 job
        result = client.exec_command("crontab -l 2>/dev/null || true", timeout=30)
        lines = result.stdout.split("\n") if result.stdout else []
        new_lines = [l for l in lines if marker not in l]
        new_lines.append(cron_line)
        new_crontab = "\n".join(new_lines) + "\n"

        # 写入新 crontab
        write_result = client.exec_command(
            f"printf '%s' {repr(new_crontab)} | crontab -",
            timeout=30,
        )
        client.disconnect()

        if write_result.success:
            return True, f"调度已添加: {name} on {ssh_host} ({cron_expr})"
        return False, f"添加失败: {write_result.stderr}"
    except Exception as e:
        return False, f"SSH 添加调度失败: {e}"


def _remove_cron_local(name: str) -> tuple[bool, str]:
    """删除本地机器的 cron job。"""
    marker = _marker_name(name)

    existing = run_command("crontab -l 2>/dev/null || true", check=False)
    lines = existing.stdout.split("\n") if existing.stdout else []
    new_lines = [l for l in lines if marker not in l]
    new_crontab = "\n".join(new_lines) + "\n"

    result = run_command(
        f"printf '%s' {repr(new_crontab)} | crontab -",
        check=False,
    )
    if result.returncode == 0:
        return True, f"调度已删除: {name}"
    return False, f"删除失败: {result.stderr}"


def _remove_cron_remote(
    name: str,
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: Optional[str],
    ssh_key: Optional[str],
) -> tuple[bool, str]:
    """删除远程机器的 cron job。"""
    from ..lib.ssh_client import SSHClient, PARAMIKO_AVAILABLE

    if not PARAMIKO_AVAILABLE:
        return False, "Paramiko 未安装"

    marker = _marker_name(name)

    try:
        client = SSHClient()
        client.connect(
            host=ssh_host,
            port=ssh_port,
            user=ssh_user,
            password=ssh_password,
            key_file=ssh_key,
        )

        result = client.exec_command("crontab -l 2>/dev/null || true", timeout=30)
        lines = result.stdout.split("\n") if result.stdout else []
        new_lines = [l for l in lines if marker not in l]
        new_crontab = "\n".join(new_lines) + "\n"

        write_result = client.exec_command(
            f"printf '%s' {repr(new_crontab)} | crontab -",
            timeout=30,
        )
        client.disconnect()

        if write_result.success:
            return True, f"调度已删除: {name} on {ssh_host}"
        return False, f"删除失败: {write_result.stderr}"
    except Exception as e:
        return False, f"SSH 删除调度失败: {e}"


# ---------------------------------------------------------------------------
# CLI 入口函数
# ---------------------------------------------------------------------------

def schedule_add(
    name: str,
    cron: str,
    backup_cmd: str,
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> tuple[bool, str]:
    """
    添加定时备份调度。

    :param name: 调度名称（唯一标识）
    :param cron: cron 表达式，如 "0 2 * * *"
    :param backup_cmd: 完整的备份命令（不含 cron 前缀），如 "python3 -m ops_db backup --type full"
    :param ssh_host: SSH 目标主机（None 表示本地）
    :param ssh_port: SSH 端口
    :param ssh_user: SSH 用户
    :param ssh_password: SSH 密码
    :param ssh_key: SSH 私钥路径
    :return: (success, message)
    """
    # 补全完整命令（绝对路径）
    python_cmd = f"cd {os.path.dirname(os.path.dirname(__file__))} && {backup_cmd}"

    if ssh_host:
        return _add_cron_remote(
            cron, name, python_cmd,
            ssh_host, ssh_port, ssh_user, ssh_password, ssh_key,
        )
    else:
        return _add_cron_local(cron, name, python_cmd)


def schedule_list(
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> tuple[bool, str]:
    """
    列出所有 ops_db 定时调度。

    :return: (success, message)
    """
    if ssh_host:
        jobs = _list_crons_remote(ssh_host, ssh_port, ssh_user, ssh_password, ssh_key)
    else:
        jobs = _list_crons_local()

    if not jobs:
        print("暂无定时调度")
        return True, "无调度"

    print(f"\n{'='*60}")
    print(f"{'定时备份调度' if not ssh_host else f'远程调度 ({ssh_host})'}")
    print(f"{'='*60}")
    print(f"{'Cron 表达式':<20} {'名称':<20} 命令")
    print(f"{'-'*60}")
    for job in jobs:
        marker = job.get("marker", "")
        name = marker.replace(f"{CRON_MARKER}_", "") if marker else "-"
        print(f"{job['cron']:<20} {name:<20} {job['cmd'][:40]}")
    print(f"{'='*60}\n")

    return True, f"共 {len(jobs)} 个调度"


def schedule_remove(
    name: str,
    ssh_host: Optional[str] = None,
    ssh_port: int = 22,
    ssh_user: str = "root",
    ssh_password: Optional[str] = None,
    ssh_key: Optional[str] = None,
) -> tuple[bool, str]:
    """
    删除指定的定时备份调度。

    :param name: 调度名称
    :return: (success, message)
    """
    if ssh_host:
        return _remove_cron_remote(
            name, ssh_host, ssh_port, ssh_user, ssh_password, ssh_key,
        )
    else:
        return _remove_cron_local(name)