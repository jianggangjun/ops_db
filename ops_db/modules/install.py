"""MySQL 安装模块 — 支持多镜像源。"""

from __future__ import annotations

import os
import random
import string
import subprocess
import sys
from typing import Optional

from ..lib.checker import (
    PreflightReport,
    check_disk_space,
    check_firewalld,
    check_mysql_running,
    check_port_available,
    check_root,
    check_selinux,
)
from ..lib.config_gen import write_my_cnf
from ..lib.logger import get_logger
from ..lib.mysql_conn import get_version
from ..lib.system_detect import (
    OSInfo,
    detect_os,
    get_recommended_mysql_version,
)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# 国内镜像源配置
# ---------------------------------------------------------------------------

class Mirror:
    """镜像源配置。"""
    def __init__(
        self,
        name: str,
        mysql_yum_repo: str,       # MySQL yum repo rpm URL
        mysql_yum_repo_el8: str,   # EL8 的 yum repo
        percona_repo: str,         # Percona XtraBackup 源
        mysql_apt_repo: str = "",   # MySQL apt 源（Debian/Ubuntu）
        percona_apt: str = "",     # Percona XtraBackup apt 源
    ):
        self.name = name
        self.mysql_yum_repo = mysql_yum_repo          # 5.7 / 8.0 repo URL
        self.mysql_yum_repo_el8 = mysql_yum_repo_el8  # EL8+ 用这个
        self.percona_repo = percona_repo
        self.mysql_apt_repo = mysql_apt_repo
        self.percona_apt = percona_apt


MIRRORS: dict[str, Mirror] = {
    "tencent": Mirror(
        name="腾讯云",
        mysql_yum_repo="https://mirrors.cloud.tencent.com/mysql/yum/mysql-5.7-community-release-el7-11.noarch.rpm",
        mysql_yum_repo_el8="https://mirrors.cloud.tencent.com/mysql/yum/mysql-80-community-release-el8-4.noarch.rpm",
        percona_repo="https://mirrors.cloud.tencent.com/percona/yum/release/latest/percona-release-latest.noarch.rpm",
        mysql_apt_repo="https://mirrors.cloud.tencent.com/mysql/apt/",
        percona_apt="https://mirrors.cloud.tencent.com/percona/apt/",
    ),
    "aliyun": Mirror(
        name="阿里云",
        mysql_yum_repo="https://mirrors.aliyun.com/mysql/yum/mysql57-community-release-el7-11.noarch.rpm",
        mysql_yum_repo_el8="https://mirrors.aliyun.com/mysql/yum/mysql80-community-release-el8-4.noarch.rpm",
        percona_repo="https://mirrors.aliyun.com/percona/yum/release/latest/percona-release-latest.noarch.rpm",
        mysql_apt_repo="",
        percona_apt="",
    ),
    "tsinghua": Mirror(
        name="清华镜像",
        mysql_yum_repo="https://mirrors.tuna.tsinghua.edu.cn/mysql/yum/mysql-5.7-community-release-el7-11.noarch.rpm",
        mysql_yum_repo_el8="https://mirrors.tuna.tsinghua.edu.cn/mysql/yum/mysql-80-community-release-el8-4.noarch.rpm",
        percona_repo="https://mirrors.tuna.tsinghua.edu.cn/percona/yum/release/latest/percona-release-latest.noarch.rpm",
        mysql_apt_repo="https://mirrors.tuna.tsinghua.edu.cn/mysql/apt/",
        percona_apt="https://mirrors.tuna.tsinghua.edu.cn/percona/apt/",
    ),
    "official": Mirror(
        name="官方源（海外，速度慢）",
        mysql_yum_repo="https://dev.mysql.com/get/mysql57-community-release-el7-11.noarch.rpm",
        mysql_yum_repo_el8="https://dev.mysql.com/get/mysql80-community-release-el8-4.noarch.rpm",
        percona_repo="https://repo.percona.com/yum/percona-release-latest.noarch.rpm",
        mysql_apt_repo="https://repo.mysql.com/apt/",
        percona_apt="https://repo.percona.com/apt/",
    ),
}


def _load_intranet_mirror() -> Mirror:
    """
    从环境变量加载内网镜像源配置。

    环境变量：
      INTRANET_MYSQL_YUM_REPO      — MySQL yum repo URL（EL7）
      INTRANET_MYSQL_YUM_REPO_EL8  — MySQL yum repo URL（EL8+）
      INTRANET_MYSQL_APT_REPO      — MySQL apt repo URL
      INTRANET_PERCONA_YUM_REPO    — Percona yum repo URL
      INTRANET_PERCONA_APT_REPO    — Percona apt repo URL
    """
    import os
    return Mirror(
        name="内网源",
        mysql_yum_repo=os.environ.get("INTRANET_MYSQL_YUM_REPO", ""),
        mysql_yum_repo_el8=os.environ.get("INTRANET_MYSQL_YUM_REPO_EL8", ""),
        percona_repo=os.environ.get("INTRANET_PERCONA_YUM_REPO", ""),
        mysql_apt_repo=os.environ.get("INTRANET_MYSQL_APT_REPO", ""),
        percona_apt=os.environ.get("INTRANET_PERCONA_APT_REPO", ""),
    )


def get_mirror(name: str) -> Mirror:
    """获取镜像源配置，intranet 从环境变量加载。"""
    if name == "intranet":
        return _load_intranet_mirror()
    return MIRRORS.get(name, MIRRORS["tencent"])

# ---------------------------------------------------------------------------
# XtraBackup 版本与 MySQL 版本对应关系
# ---------------------------------------------------------------------------
# XtraBackup 2.4 → MySQL 5.6/5.7, MariaDB 10.x
# XtraBackup 8.0 → MySQL 8.0.x
# XtraBackup 8.2 → MySQL 8.0.x / 8.4.x ✅
# XtraBackup 9.0 → MySQL 8.0.x / 8.4.x / 9.0.x
#
# 安装时：版本越高越通用，但需要 repo 中有对应版本
# 设计原则：保守策略，优先用 repo 中稳定可获取的版本

XTRABACKUP_VERSION_MATRIX = {
    ("5.7", "8.0"): "80",   # MySQL 5.7/8.0 → XtraBackup 8.0
    ("5.7", "8.4"): "82",   # MySQL 5.7/8.4 → XtraBackup 8.2
    ("8.0", "8.0"): "80",   # MySQL 8.0 → XtraBackup 8.0
    ("8.0", "8.4"): "82",   # MySQL 8.0 → XtraBackup 8.2
    ("8.4", "8.4"): "82",   # MySQL 8.4 → XtraBackup 8.2（推荐）
    ("8.4", "9.0"): "90",   # MySQL 8.4/9.0 → XtraBackup 9.0
    ("9.0", "9.0"): "90",   # MySQL 9.0 → XtraBackup 9.0
}

# CentOS 7 只支持 XtraBackup 2.4
XTRABACKUP_CENTOS7 = "24"

# XtraBackup 包名（Percona 仓库中的名字）
XTRABACKUP_PACKAGE = {
    "24": "percona-xtrabackup-24",
    "80": "percona-xtrabackup-80",
    "82": "percona-xtrabackup-82",
    "90": "percona-xtrabackup-90",
}


def get_xtrabackup_version(mysql_version: str, os_key: str) -> str:
    """
    根据 MySQL 版本和 OS 返回推荐的 XtraBackup 版本。

    CentOS 7 → 只能是 2.4
    其他系统：
      MySQL 5.7  → 8.0（保守）
      MySQL 8.0  → 8.0
      MySQL 8.4  → 8.2
      MySQL 9.0  → 9.0
    """
    if os_key == "centos7":
        return XTRABACKUP_CENTOS7

    major = mysql_version.split(".")[0] if mysql_version else "8"
    minor = mysql_version.split(".")[1] if mysql_version.count(".") >= 1 else "0"
    mysql_major = f"{major}.{minor}"

    key = (mysql_major, mysql_version)
    return XTRABACKUP_VERSION_MATRIX.get(key, "80")


# ---------------------------------------------------------------------------
# 密码生成 & 命令执行
# ---------------------------------------------------------------------------

def generate_password(length: int = 16) -> str:
    """生成随机 MySQL root 密码。"""
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def run_command(
    cmd: str,
    env: Optional[dict] = None,
    timeout: int = 300,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """统一执行 shell 命令。"""
    logger.info(f"执行命令: {cmd[:80]}...")
    merged_env = dict(os.environ)
    if env:
        merged_env.update(env)

    cp = subprocess.run(
        cmd,
        shell=True,
        env=merged_env,
        timeout=timeout,
        capture_output=True,
        text=True,
    )
    if check and cp.returncode != 0:
        logger.error(f"命令失败 [{cp.returncode}]: {cp.stderr[:300]}")
        raise RuntimeError(f"命令执行失败:\n{cp.stderr[:500]}")
    return cp


# ---------------------------------------------------------------------------
# 主安装函数
# ---------------------------------------------------------------------------

def install_mysql(
    version: Optional[str] = None,
    os_info: Optional[OSInfo] = None,
    port: int = 3306,
    datadir: str = "/var/lib/mysql",
    logdir: Optional[str] = None,
    server_id: Optional[int] = None,
    role: str = "single",
    root_password: Optional[str] = None,
    yes: bool = False,
    mirror: str = "tencent",
) -> tuple[bool, str]:
    """
    安装 MySQL。

    :param version: MySQL 版本，如 "5.7"、"8.0"、"8.4"，None 表示自动推荐
    :param os_info: OS 信息，不传则自动探测
    :param port: 端口
    :param datadir: 数据目录
    :param logdir: 日志目录
    :param server_id: server-id，不传则自动生成
    :param role: single / master / slave
    :param root_password: root 密码，不传则自动生成
    :param yes: 跳过确认
    :param mirror: 镜像源，tencent / aliyun / tsinghua / official
    """
    # 1. 前置检查
    report = PreflightReport()
    report.results.extend([
        check_root(),
        check_mysql_running(port),
        check_port_available(port),
        check_selinux(),
        check_firewalld(),
        check_disk_space("/var/lib", required_gb=5.0),
    ])

    if report.has_fatal:
        print(report.summary())
        return False, "前置检查失败"

    # 2. 系统探测
    if os_info is None:
        os_info = detect_os()
    logger.info(
        f"检测到系统: {os_info.raw_name} {os_info.version} ({os_info.arch}), "
        f"family: {os_info.family}"
    )

    # 3. 镜像源
    mirror_cfg = get_mirror(mirror)
    logger.info(f"使用镜像源: {mirror_cfg.name} ({mirror})")

    # 4. 确定版本
    if version is None:
        recommended = get_recommended_mysql_version(os_info.os)
        if recommended:
            version = recommended[0]
            logger.info(f"自动推荐 MySQL {version}")
        else:
            version = "8.0"

    # 5. XtraBackup 版本
    xtrabackup_ver = get_xtrabackup_version(version, os_info.os)
    logger.info(f"推荐 XtraBackup 版本: {xtrabackup_ver}")

    # 6. server-id
    if server_id is None:
        import hashlib
        seed = f"127.0.0.1:{port}".encode()
        server_id = (int(hashlib.md5(seed).hexdigest(), 16) % 254) + 1

    # 7. 密码
    if root_password is None:
        root_password = generate_password()

    # 8. 打印安装计划
    print_plan(version, xtrabackup_ver, os_info, mirror_cfg, port, datadir, server_id, role, root_password)

    if not yes:
        confirm = input("\n确认开始安装？[y/N]: ").strip().lower()
        if confirm != "y":
            return False, "用户取消"

    # 9. 执行安装
    try:
        success, msg = _do_install(
            version, xtrabackup_ver, os_info, mirror_cfg,
            port, datadir, logdir, server_id, role, root_password,
        )
        return success, msg
    except Exception as e:
        logger.exception(f"安装异常: {e}")
        return False, str(e)


def _do_install(
    version: str,
    xtrabackup_ver: str,
    os_info: OSInfo,
    mirror: Mirror,
    port: int,
    datadir: str,
    logdir: Optional[str],
    server_id: int,
    role: str,
    root_password: str,
) -> tuple[bool, str]:
    """执行实际安装步骤。"""
    os_key = os_info.os

    # 创建目录
    logger.info("创建目录结构...")
    os.makedirs(datadir, exist_ok=True)
    if logdir is None:
        logdir = os.path.join(datadir, "log")
    os.makedirs(logdir, exist_ok=True)

    # 获取安装命令
    if os_info.family == "rhel":
        cmds = _build_rhel_install_cmds(version, os_key, mirror)
        xtrabackup_cmd = _build_xtrabackup_rhel_cmd(xtrabackup_ver, mirror)
    elif os_info.family == "debian":
        cmds = _build_debian_install_cmds(version, os_key, mirror)
        xtrabackup_cmd = _build_xtrabackup_debian_cmd(xtrabackup_ver, mirror)
    else:
        return False, f"不支持的操作系统: {os_info.raw_name}"

    # 执行 MySQL 安装
    for i, cmd in enumerate(cmds):
        logger.info(f"MySQL 安装步骤 {i+1}/{len(cmds)}: {cmd[:70]}...")
        run_command(cmd, timeout=300)

    # 执行 XtraBackup 安装
    if xtrabackup_cmd:
        logger.info(f"安装 XtraBackup {xtrabackup_ver}: {xtrabackup_cmd[:70]}...")
        run_command(xtrabackup_cmd, timeout=180)

    # 生成 my.cnf
    logger.info("生成 my.cnf...")
    my_cnf_path = "/etc/my.cnf"
    if os_info.family == "debian":
        my_cnf_path = "/etc/mysql/mysql.conf.d/mysqld.cnf"

    write_my_cnf(
        path=my_cnf_path,
        port=port,
        datadir=datadir,
        logdir=logdir,
        server_id=server_id,
        role=role,
        mysql_version=version,
    )
    logger.info(f"配置文件已写入: {my_cnf_path}")

    # 初始化 data 目录
    logger.info("初始化 MySQL data 目录...")
    if os_key == "centos7" and version.startswith("5.7"):
        run_command("mysqld --initialize-insecure --datadir=/var/lib/mysql", timeout=120)
    else:
        run_command(
            f"mysqld --initialize --datadir={datadir} "
            f"--user=mysql --password={root_password}",
            timeout=120,
        )

    # 权限
    run_command(f"chown -R mysql:mysql {datadir}")
    run_command(f"chown -R mysql:mysql {logdir}")

    # 启动服务（适配 OS）
    if os_info.family == "debian":
        run_command("systemctl start mysql || true", timeout=30)
    else:
        run_command("systemctl start mysqld || service mysqld start", timeout=30)
        run_command("systemctl enable mysqld || true", timeout=10)

    # 等待启动
    import time
    logger.info("等待 MySQL 服务启动（约 10s）...")
    time.sleep(10)

    # 检测服务是否真正启动（端口检测）
    if not _check_mysql_port_ready(port, timeout=30):
        logger.warning(f"端口 {port} 未就绪，服务可能启动失败")

    # 验证
    try:
        ver = get_version("127.0.0.1", port, "root", root_password)
        logger.info(f"MySQL 安装成功，版本: {ver}")
    except Exception as e:
        logger.warning(f"无法立即验证连接: {e}")

    print_success(version, xtrabackup_ver, port, root_password, my_cnf_path)
    return True, f"MySQL {version} 安装成功，XtraBackup {xtrabackup_ver}，端口 {port}"


def _build_rhel_install_cmds(version: str, os_key: str, mirror: Mirror) -> list[str]:
    """构建 RHEL 系 MySQL 安装命令。"""
    is_el7 = os_key in ("centos7", "rhel7")
    is_el8plus = not is_el7

    cmds = []

    if is_el7:
        # CentOS 7 / RHEL 7
        repo_url = mirror.mysql_yum_repo
        cmds.append(f"yum install -y {repo_url}")
        cmds.append("yum install -y mysql-community-server mysql-community-client")
        if version.startswith("5.7"):
            cmds.append("yum install -y perl-Digest-Maker")
    else:
        # CentOS 8 / RHEL 8+ / Rocky / AlmaLinux
        repo_url = mirror.mysql_yum_repo_el8
        cmds.append(f"dnf install -y {repo_url}")
        cmds.append("dnf install -y mysql-community-server mysql-community-client")

    return cmds


def _build_debian_install_cmds(version: str, os_key: str, mirror: Mirror) -> list[str]:
    """构建 Debian/Ubuntu MySQL 安装命令。"""
    cmds = [
        "apt-get update",
        f"DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server mysql-client",
    ]
    return cmds


def _build_xtrabackup_rhel_cmd(xtrabackup_ver: str, mirror: Mirror) -> str:
    """构建 RHEL 系 XtraBackup 安装命令。"""
    pkg = XTRABACKUP_PACKAGE.get(xtrabackup_ver, f"percona-xtrabackup-{xtrabackup_ver}")
    percona_repo = mirror.percona_repo

    # CentOS 7 用 yum，CentOS 8+ 用 dnf
    # percona-release 安装后，yum/dnf install 均可
    return (
        f"yum install -y {percona_repo} 2>/dev/null || "
        f"dnf install -y {percona_repo} 2>/dev/null; "
        f"yum install -y {pkg} || dnf install -y {pkg}"
    )


def _build_xtrabackup_debian_cmd(xtrabackup_ver: str, mirror: Mirror) -> str:
    """构建 Debian/Ubuntu XtraBackup 安装命令。"""
    percona_release_pkg = f"percona-release_{_get_percona_debian_pkg_suffix()}"
    return (
        f"wget -q -O /tmp/{percona_release_pkg} {mirror.percona_apt}pool/main/p/percona-release/{percona_release_pkg} "
        f"&& dpkg -i /tmp/{percona_release_pkg} "
        f"&& percona-release setup pdps{xtrabackup_ver} "
        f"&& apt-get update && apt-get install -y percona-xtrabackup-{xtrabackup_ver}"
    )


def _get_percona_debian_pkg_suffix() -> str:
    """根据当前系统发行版返回 percona-release 包后缀。"""
    try:
        # 简单读取 lsb_release 或 os-release
        with open("/etc/os-release", "r") as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("VERSION_CODENAME="):
                codename = line.split("=")[1].strip()
                return f"percona-release_latest.{codename}_all.deb"
    except Exception:
        pass
    return "percona-release_latest.bullseye_all.deb"  # 默认 debian 11


# ------------------------------------------------------------------
# 辅助函数
# ------------------------------------------------------------------

def _check_mysql_port_ready(port: int, timeout: int = 30) -> bool:
    """检测 MySQL 端口是否可连接。"""
    import socket, time
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect(("127.0.0.1", port))
            sock.close()
            return True
        except (socket.error, OSError):
            time.sleep(1)
    return False


# ------------------------------------------------------------------
# 输出
# ------------------------------------------------------------------

def print_plan(
    version: str,
    xtrabackup_ver: str,
    os_info: OSInfo,
    mirror: Mirror,
    port: int,
    datadir: str,
    server_id: int,
    role: str,
    root_password: str,
) -> None:
    """打印安装计划。"""
    print("\n" + "=" * 50)
    print("📋  安装计划")
    print("=" * 50)
    print(f"  OS         : {os_info.raw_name} {os_info.version} ({os_info.arch})")
    print(f"  镜像源     : {mirror.name}")
    print(f"  MySQL版本  : {version}")
    print(f"  XtraBackup : {xtrabackup_ver}")
    print(f"  角色       : {role}")
    print(f"  端口       : {port}")
    print(f"  数据目录   : {datadir}")
    print(f"  server-id  : {server_id}")
    print(f"  root密码   : {root_password[:4]}****（安装后请尽快修改）")
    print("=" * 50)


def print_success(
    version: str,
    xtrabackup_ver: str,
    port: int,
    root_password: str,
    my_cnf_path: str,
) -> None:
    """打印安装成功信息。"""
    print("\n" + "=" * 50)
    print("✅  MySQL 安装完成！")
    print("=" * 50)
    print(f"  配置文件   : {my_cnf_path}")
    print(f"  MySQL版本  : {version}")
    print(f"  XtraBackup : {xtrabackup_ver}")
    print(f"  端口       : {port}")
    print(f"  root密码   : {root_password}")
    print()
    print("  登录命令:")
    print(f"  mysql -uroot -p'{root_password}' -h127.0.0.1 -P{port}")
    print()
    print("  修改密码:")
    print(f"  ALTER USER 'root'@'localhost' IDENTIFIED BY '新密码';")
    print("=" * 50 + "\n")
