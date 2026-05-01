"""系统探测模块 — 检测 OS 版本、发行版、架构。"""

from __future__ import annotations

import platform
import subprocess
from dataclasses import dataclass
from typing import Optional


@dataclass
class OSInfo:
    os: str          # 简称：centos7, ubuntu22, debian12, rocky9, almalinux9 ...
    family: str      # rhel / debian
    version: str     # 完整版本号：7.9, 22.04, 12.2 ...
    arch: str        # x86_64 / aarch64
    raw_name: str    # 原始发行版名称


# MySQL 版本推荐矩阵
MYSQL_VERSION_MAP = {
    "centos7":   {"default": "5.7",   "xtrabackup": "2.4"},
    "centos8":   {"default": "8.0",   "xtrabackup": "8.0"},
    "rhel7":     {"default": "5.7",   "xtrabackup": "2.4"},
    "rhel8":     {"default": "8.0",   "xtrabackup": "8.0"},
    "rhel9":     {"default": "8.0",   "xtrabackup": "8.0"},
    "rocky8":    {"default": "8.0",   "xtrabackup": "8.0"},
    "rocky9":    {"default": "8.0",   "xtrabackup": "8.0"},
    "almalinux8": {"default": "8.0",  "xtrabackup": "8.0"},
    "almalinux9": {"default": "8.0",  "xtrabackup": "8.0"},
    "ubuntu20":  {"default": "8.0",   "xtrabackup": "8.0"},
    "ubuntu22":  {"default": "8.0",   "xtrabackup": "8.0"},
    "ubuntu24":  {"default": "8.4",   "xtrabackup": "8.2"},
    "debian11":  {"default": "8.0",   "xtrabackup": "8.0"},
    "debian12":  {"default": "8.0",   "xtrabackup": "8.0"},
}


def detect_os() -> OSInfo:
    """检测当前操作系统信息。"""
    uname = platform.uname()
    system = uname.system.lower()
    release = uname.release
    arch = uname.machine

    raw_name = ""
    version = ""

    if system == "linux":
        # 优先用 /etc/os-release（最准确）
        raw_name, version = _detect_from_os_release()

        if not raw_name:
            # 降级：解析 uname -r
            version = release.split("-")[0]

    elif system == "darwin":
        raw_name = "macos"
        version = platform.mac_ver()[0]

    elif system == "windows":
        raw_name = "windows"
        version = platform.win32_ver()[0]

    # 归一化简称
    os_key = _normalize_os_key(raw_name, version)

    return OSInfo(
        os=os_key,
        family="rhel" if os_key in (
            "centos7", "centos8", "rhel7", "rhel8", "rhel9",
            "rocky8", "rocky9", "almalinux8", "almalinux9",
        ) else "debian" if os_key in (
            "ubuntu20", "ubuntu22", "ubuntu24", "debian11", "debian12",
        ) else "other",
        version=version,
        arch=arch,
        raw_name=raw_name,
    )


def _detect_from_os_release() -> tuple[str, str]:
    """解析 /etc/os-release 获取发行版信息。"""
    try:
        with open("/etc/os-release", "r") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return "", ""

    name = ""
    version_id = ""

    for line in lines:
        if line.startswith("ID="):
            name = line.split("=")[1].strip().strip('"')
        elif line.startswith("VERSION_ID="):
            version_id = line.split("=")[1].strip().strip('"')

    return name, version_id


def _normalize_os_key(raw_name: str, version: str) -> str:
    """将原始发行版名归一化为短键。"""
    name = raw_name.lower()

    if name == "centos":
        return "centos7" if version.startswith("7") else "centos8"

    if name == "rocky":
        return f"rocky{version.split('.')[0]}"

    if name == "almalinux":
        return f"almalinux{version.split('.')[0]}"

    if name == "rhel":
        return f"rhel{version.split('.')[0]}"

    if name == "ubuntu":
        return f"ubuntu{version.split('.')[0]}"

    if name == "debian":
        return f"debian{version.split('.')[0]}"

    return name


def get_recommended_mysql_version(os_key: str) -> Optional[tuple[str, str]]:
    """根据 OS 推荐 MySQL 和 XtraBackup 版本。"""
    info = MYSQL_VERSION_MAP.get(os_key)
    if info:
        return info["default"], info["xtrabackup"]
    return None


def check_command_exists(cmd: str) -> bool:
    """检查命令是否存在。"""
    try:
        subprocess.run(
            ["which", cmd],
            capture_output=True,
            check=False,
        )
        return True
    except Exception:
        return False
