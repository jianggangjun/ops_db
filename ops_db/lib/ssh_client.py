"""SSH 远程执行客户端 — 基于 Paramiko。

支持：
- 密码 / 密钥 两种认证方式
- 远程命令执行
- 文件上传（scp style）
- sudo 提升权限
- 多主机并行执行
"""

from __future__ import annotations

import io
import os
import re
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Paramiko 默认未安装，延迟导入
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False

from .logger import get_logger

logger = get_logger(__name__)


@dataclass
class SSHResult:
    """远程命令执行结果。"""
    host: str
    success: bool
    stdout: str
    stderr: str
    returncode: int
    duration_ms: int


@dataclass
class SSHHost:
    """SSH 目标主机配置。"""
    host: str              # IP 或 hostname
    port: int = 22
    user: str = "root"
    password: Optional[str] = None
    key_file: Optional[str] = None   # 私钥路径
    key_passphrase: Optional[str] = None  # 私钥密码


@dataclass
class SSHTarget:
    """SSH 操作目标描述。"""
    host_config: SSHHost
    python_path: str = "python3"   # 远程 Python 解释器路径


def _check_paramiko() -> None:
    """检查 Paramiko 是否可用。"""
    if not PARAMIKO_AVAILABLE:
        raise RuntimeError(
            "Paramiko 未安装，请运行: pip install paramiko\n"
            "或者: pip install ops_db[remote]"
        )


# ---------------------------------------------------------------------------
# SSH 连接管理
# ---------------------------------------------------------------------------

class SSHClient:
    """
    SSH 客户端封装。

    用法::

        client = SSHClient()
        client.connect("192.168.1.10", user="root", password="xxx")
        result = client.exec_command("df -h")
        print(result.stdout)
        client.disconnect()
    """

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self._client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        self._connected = False

    def connect(
        self,
        host: str,
        port: int = 22,
        user: str = "root",
        password: Optional[str] = None,
        key_file: Optional[str] = None,
        key_passphrase: Optional[str] = None,
    ) -> None:
        """
        建立 SSH 连接，支持密码或密钥认证。

        :param host: 目标主机
        :param port: SSH 端口
        :param user: 用户名
        :param password: 密码（密钥认证时为 None）
        :param key_file: 私钥路径
        :param key_passphrase: 私钥密码（可选）
        """
        _check_paramiko()

        logger.info(f"SSH 连接 {user}@{host}:{port}...")
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: dict = {
            "hostname": host,
            "port": port,
            "username": user,
            "timeout": self.timeout,
            "look_for_keys": False,  # 禁用默认密钥搜索
            "allow_agent": False,
        }

        if key_file:
            connect_kwargs["key_filename"] = key_file
            if key_passphrase:
                connect_kwargs["passphrase"] = key_passphrase
        elif password:
            connect_kwargs["password"] = password
        else:
            # 尝试默认密钥
            connect_kwargs["look_for_keys"] = True
            connect_kwargs["allow_agent"] = True

        try:
            self._client.connect(**connect_kwargs)
            self._connected = True
            logger.info(f"SSH 连接成功: {host}")
        except paramiko.AuthenticationException:
            raise SSHAuthError(f"SSH 认证失败: {user}@{host}:{port}")
        except paramiko.SSHException as e:
            raise SSHConnectionError(f"SSH 连接失败: {e}")

    def disconnect(self) -> None:
        """关闭 SSH 连接。"""
        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None

        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            self._connected = False
            logger.info("SSH 连接已关闭")

    def is_connected(self) -> bool:
        """检查是否已连接。"""
        if not self._client or not self._connected:
            return False
        transport = self._client.get_transport()
        return transport is not None and transport.is_active()

    def exec_command(
        self,
        command: str,
        timeout: int = 300,
        sudo: bool = False,
    ) -> SSHResult:
        """
        在远程主机执行命令。

        :param command: 要执行的命令
        :param timeout: 超时时间（秒）
        :param sudo: 是否使用 sudo 执行
        :return: SSHResult
        """
        if not self._connected or not self._client:
            raise SSHConnectionError("未建立 SSH 连接")

        full_command = command
        if sudo and self._is_root() is False:
            # 检查是否需要 sudo
            full_command = f"sudo {command}"

        import time
        start_ms = int(time.time() * 1000)

        logger.debug(f"执行命令: {command[:80]}...")
        _, stdout, stderr = self._client.exec_command(
            full_command,
            timeout=timeout,
        )

        # 等待命令完成
        exit_status = stdout.channel.recv_exit_status()

        stdout_text = stdout.read().decode("utf-8", errors="replace")
        stderr_text = stderr.read().decode("utf-8", errors="replace")
        duration_ms = int(time.time() * 1000) - start_ms

        success = exit_status == 0
        if not success:
            logger.warning(f"命令失败 [{exit_status}]: {stderr_text[:200]}")
        else:
            logger.debug(f"命令执行成功 ({duration_ms}ms)")

        return SSHResult(
            host=self._client.getpeername()[0] if self._client else "?",
            success=success,
            stdout=stdout_text,
            stderr=stderr_text,
            returncode=exit_status,
            duration_ms=duration_ms,
        )

    def exec_command_with_input(
        self,
        command: str,
        input_text: str,
        timeout: int = 300,
    ) -> SSHResult:
        """
        执行需要交互式输入的命令（如 apt install y）。

        :param input_text: 要输入的文本（通常含换行）
        """
        if not self._connected or not self._client:
            raise SSHConnectionError("未建立 SSH 连接")

        import time
        start_ms = int(time.time() * 1000)

        logger.debug(f"交互式命令: {command[:60]}...")
        channel = self._client.get_transport().open_session()
        channel.set_combine_stderr(False)
        channel.settimeout(timeout)
        channel.exec_command(command)

        # 发送输入
        if input_text:
            channel.send(input_text.encode())
            channel.shutdown_write()

        # 读取输出
        stdout_buf = io.BytesIO()
        stderr_buf = io.BytesIO()

        while True:
            if channel.exit_status_ready():
                break
            # 读取 stdout
            if channel.recv_ready():
                stdout_buf.write(channel.recv(4096))
            # 读取 stderr
            if channel.recv_stderr_ready():
                stderr_buf.write(channel.recv_stderr(4096))

        # 最后再读一次退出后的输出
        while True:
            try:
                chunk = channel.recv(4096)
                if not chunk:
                    break
                stdout_buf.write(chunk)
            except Exception:
                break

        exit_status = channel.recv_exit_status()
        duration_ms = int(time.time() * 1000) - start_ms

        stdout_text = stdout_buf.getvalue().decode("utf-8", errors="replace")
        stderr_text = stderr_buf.getvalue().decode("utf-8", errors="replace")

        return SSHResult(
            host=self._client.getpeername()[0] if self._client else "?",
            success=exit_status == 0,
            stdout=stdout_text,
            stderr=stderr_text,
            returncode=exit_status,
            duration_ms=duration_ms,
        )

    def put_file(
        self,
        local_path: str,
        remote_path: str,
        mode: Optional[int] = None,
    ) -> None:
        """
        上传本地文件到远程主机。

        :param local_path: 本地文件路径
        :param remote_path: 远程目标路径
        :param mode: 文件权限（如 0o644）
        """
        if not self._connected or not self._client:
            raise SSHConnectionError("未建立 SSH 连接")

        logger.info(f"上传文件: {local_path} → {remote_path}")
        if not self._sftp:
            self._sftp = self._client.open_sftp()

        self._sftp.put(local_path, remote_path)
        if mode is not None:
            self._sftp.chmod(remote_path, mode)

    def put_directory(
        self,
        local_dir: str,
        remote_dir: str,
        exclude: Optional[list[str]] = None,
    ) -> None:
        """
        上传本地目录到远程主机（tar + untar 方式，高效）。

        :param local_dir: 本地目录
        :param remote_dir: 远程目标目录
        :param exclude: 排除的文件/目录模式
        """
        if not self._connected or not self._client:
            raise SSHConnectionError("未建立 SSH 连接")

        logger.info(f"上传目录: {local_dir} → {remote_dir}")

        # 1. 创建 tar 包（内存中）
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.GZIP) as tar:
            tar.add(local_dir, arcname=os.path.basename(local_dir))
        tar_buffer.seek(0)

        # 2. 通过 stdin 发送 tar 包
        channel = self._client.get_transport().open_session()
        channel.exec_command(f"mkdir -p {remote_dir} && cd {remote_dir} && tar -xzf -")

        # 发送 tar 数据
        channel.set_combine_stderr(True)
        channel.send(tar_buffer.getvalue())
        channel.shutdown_write()

        # 等待完成
        exit_status = channel.recv_exit_status()
        if exit_status != 0:
            output = channel.recv(4096).decode("utf-8", errors="replace")
            raise RuntimeError(f"上传目录失败: {output}")

        logger.info(f"目录上传成功: {remote_dir}")

    def get_file(self, remote_path: str, local_path: str) -> None:
        """从远程主机下载文件到本地。"""
        if not self._connected or not self._client:
            raise SSHConnectionError("未建立 SSH 连接")

        logger.info(f"下载文件: {remote_path} → {local_path}")
        if not self._sftp:
            self._sftp = self._client.open_sftp()

        self._sftp.get(remote_path, local_path)

    def _is_root(self) -> Optional[bool]:
        """检查当前用户是否是 root。"""
        if not self._connected or not self._client:
            return None
        # 检查 USER 环境变量
        _, stdout, _ = self._client.exec_command("echo $USER")
        user = stdout.read().decode().strip()
        return user == "root"


class SSHPool:
    """
    SSH 连接池 — 管理多台主机的并发连接。

    用法::

        pool = SSHPool([
            {"host": "192.168.1.10", "password": "xxx"},
            {"host": "192.168.1.11", "password": "xxx"},
        ])
        results = pool.exec_command_parallel("df -h")
        pool.disconnect_all()
    """

    def __init__(self, hosts: list[SSHHost]):
        self.hosts = hosts
        self._clients: dict[str, SSHClient] = {}

    def connect_all(self) -> None:
        """建立所有主机的 SSH 连接。"""
        for host_config in self.hosts:
            client = SSHClient()
            try:
                client.connect(
                    host=host_config.host,
                    port=host_config.port,
                    user=host_config.user,
                    password=host_config.password,
                    key_file=host_config.key_file,
                    key_passphrase=host_config.key_passphrase,
                )
                self._clients[f"{host_config.host}:{host_config.port}"] = client
            except Exception as e:
                logger.error(f"连接到 {host_config.host} 失败: {e}")
                raise

    def disconnect_all(self) -> None:
        """关闭所有 SSH 连接。"""
        for key, client in self._clients.items():
            try:
                client.disconnect()
            except Exception as e:
                logger.warning(f"关闭连接 {key} 时出错: {e}")
        self._clients.clear()

    def get_client(self, host: str, port: int = 22) -> SSHClient:
        """获取指定主机的 SSH 客户端。"""
        key = f"{host}:{port}"
        if key not in self._clients:
            raise SSHConnectionError(f"主机 {key} 未连接")
        return self._clients[key]

    def exec_command_parallel(
        self,
        command: str,
        sudo: bool = False,
        timeout: int = 300,
    ) -> dict[str, SSHResult]:
        """
        并行在所有主机执行命令。

        :return: {host: SSHResult}
        """
        import concurrent.futures

        results: dict[str, SSHResult] = {}

        def run_on_host(host_config: SSHHost) -> tuple[str, SSHResult]:
            key = f"{host_config.host}:{host_config.port}"
            client = self._clients.get(key)
            if not client:
                return key, SSHResult(
                    host=host_config.host,
                    success=False,
                    stdout="",
                    stderr="Not connected",
                    returncode=-1,
                    duration_ms=0,
                )
            result = client.exec_command(command, sudo=sudo, timeout=timeout)
            return key, result

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.hosts)) as executor:
            futures = {
                executor.submit(run_on_host, h): h for h in self.hosts
            }
            for future in concurrent.futures.as_completed(futures):
                key, result = future.result()
                results[key] = result

        return results

    def __enter__(self) -> "SSHPool":
        self.connect_all()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect_all()


# ---------------------------------------------------------------------------
# 远程文件打包工具（ops_db 分发）
# ---------------------------------------------------------------------------

def package_ops_db_for_remote() -> bytes:
    """
    将当前 ops_db 目录打包为 tar.gz，返回字节数据。

    用于通过 stdin 上传到远程主机：
    1. 将 tar.gz 通过 stdin 传到远程
    2. 远程解压到 /tmp/ops_db_remote/
    3. 在远程执行 Python 脚本
    """
    buffer = io.BytesIO()
    script_dir = Path(__file__).parent.parent

    with tarfile.open(fileobj=buffer, mode="w:gz", format=tarfile.GZIP) as tar:
        # 排除 __pycache__、*.pyc、.git 等
        excludes = {
            "__pycache__", ".git", ".hermes", "logs", "*.log",
            ".DS_Store", "*.pyc", ".pytest_cache",
        }

        for item in script_dir.rglob("*"):
            if item.is_file():
                # 检查是否在排除列表
                rel = item.relative_to(script_dir)
                rel_str = str(rel)
                if any(ex in rel_str for ex in excludes):
                    continue
                tar.add(item, arcname=rel)

    buffer.seek(0)
    return buffer.getvalue()


def deploy_and_run_on_remote(
    ssh_client: SSHClient,
    remote_work_dir: str = "/tmp/ops_db_remote",
    command: str = "",
    module: Optional[str] = None,
    module_args: Optional[dict] = None,
) -> SSHResult:
    """
    将 ops_db 打包上传到远程主机并执行。

    :param ssh_client: 已连接的 SSHClient
    :param remote_work_dir: 远程工作目录
    :param command: 直接执行的 shell 命令
    :param module: 要执行的模块名（如 "install", "backup"）
    :param module_args: 模块参数字典
    :return: SSHResult
    """
    # 1. 打包
    tar_data = package_ops_db_for_remote()
    logger.info(f"ops_db 打包完成，大小: {len(tar_data) / 1024:.1f} KB")

    # 2. 远程创建目录
    ssh_client.exec_command(f"mkdir -p {remote_work_dir}", sudo=True)

    # 3. 通过 stdin 发送 tar.gz
    channel = ssh_client._client.get_transport().open_session()
    channel.exec_command(f"cd {remote_work_dir} && tar -xzf -")

    channel.set_combine_stderr(True)
    channel.send(tar_data)
    channel.shutdown_write()

    exit_status = channel.recv_exit_status()
    if exit_status != 0:
        output = channel.recv(4096).decode("utf-8", errors="replace")
        raise RuntimeError(f"上传 ops_db 失败: {output}")

    logger.info(f"ops_db 已部署到 {remote_work_dir}")

    # 4. 构造并执行命令
    if command:
        full_cmd = f"cd {remote_work_dir} && {command}"
    elif module:
        args_str = ""
        if module_args:
            arg_parts = []
            for k, v in module_args.items():
                if isinstance(v, bool):
                    if v:
                        arg_parts.append(f"--{k}")
                elif isinstance(v, list):
                    for item in v:
                        arg_parts.append(f"--{k}={item}")
                else:
                    arg_parts.append(f"--{k}={v}")
            args_str = " ".join(arg_parts)

        full_cmd = f"cd {remote_work_dir} && python3 ops_db.py {module} {args_str}"
    else:
        raise ValueError("必须指定 command 或 module")

    logger.info(f"远程执行: {full_cmd[:100]}...")
    return ssh_client.exec_command(full_cmd, sudo=True)


# ---------------------------------------------------------------------------
# 异常类
# ---------------------------------------------------------------------------

class SSHConnectionError(RuntimeError):
    """SSH 连接错误。"""
    pass


class SSHAuthError(RuntimeError):
    """SSH 认证错误。"""
    pass
