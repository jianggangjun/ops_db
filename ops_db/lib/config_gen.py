"""my.cnf 配置生成 — Jinja2 模板渲染。"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATE_DIR = Path(__file__).parent.parent / "config"


def render_my_cnf(
    port: int = 3306,
    datadir: str = "/var/lib/mysql",
    logdir: Optional[str] = None,
    server_id: int = 1,
    role: str = "master",
    binlog: bool = True,
    gtid_mode: bool = False,
    slow_query_log: bool = True,
    max_connections: int = 500,
    innodb_buffer_pool_size: Optional[str] = None,
    mysql_version: str = "8.0",
    extra_options: Optional[dict] = None,
) -> str:
    """
    渲染 my.cnf 配置文件。

    :param port: MySQL 端口
    :param datadir: 数据目录
    :param logdir: 日志目录（默认 datadir/log）
    :param server_id: server-id（主从必须唯一）
    :param role: 角色 master / slave
    :param binlog: 是否开启 binlog
    :param gtid_mode: 是否开启 GTID
    :param slow_query_log: 是否开启慢查询日志
    :param max_connections: 最大连接数
    :param innodb_buffer_pool_size: 如 "4G"，留空则自动计算
    :param mysql_version: MySQL 版本（影响部分参数语法）
    :param extra_options: 额外配置项字典
    """
    if logdir is None:
        logdir = os.path.join(datadir, "log")

    # 自动计算 InnoDB buffer pool（保守策略：机器内存的 50%）
    if innodb_buffer_pool_size is None:
        try:
            # 读取 /proc/meminfo 获取内存大小
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        total_kb = int(line.split()[1])
                        total_gb = total_kb / (1024 * 1024)
                        pool_gb = int(total_gb * 0.5)
                        innodb_buffer_pool_size = f"{pool_gb}G"
                        break
        except Exception:
            innodb_buffer_pool_size = "1G"

    major = mysql_version.split(".")[0] if mysql_version else "8"

    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    template = env.get_template("my.cnf.j2")
    return template.render(
        port=port,
        datadir=datadir,
        logdir=logdir,
        server_id=server_id,
        role=role,
        binlog=binlog,
        gtid_mode=gtid_mode,
        slow_query_log=slow_query_log,
        max_connections=max_connections,
        innodb_buffer_pool_size=innodb_buffer_pool_size,
        mysql_major_version=major,
        extra_options=extra_options or {},
    )


def compute_server_id(host: str, port: int, default: int = 1) -> int:
    """
    根据 host + port 生成稳定的 server-id。

    用于单机多实例场景，生成一个确定性的数字。
    """
    seed = f"{host}:{port}".encode()
    hash_val = int(hashlib.md5(seed).hexdigest(), 16)
    return (hash_val % 254) + 1  # 1-255 范围


def write_my_cnf(
    path: str,
    port: int = 3306,
    datadir: str = "/var/lib/mysql",
    logdir: Optional[str] = None,
    server_id: Optional[int] = None,
    role: str = "master",
    **kwargs,
) -> str:
    """
    渲染并写入 my.cnf 文件。

    :param path: 写入路径
    :param server_id: 不传则自动生成
    :return: 写入的内容
    """
    if server_id is None:
        server_id = compute_server_id("127.0.0.1", port)

    content = render_my_cnf(
        port=port,
        datadir=datadir,
        logdir=logdir,
        server_id=server_id,
        role=role,
        **kwargs,
    )

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write(content)

    return content
