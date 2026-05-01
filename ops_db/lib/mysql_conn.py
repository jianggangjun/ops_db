"""MySQL 连接封装 — PyMySQL 上下文管理器。"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Generator, Optional

import pymysql
from pymysql.cursors import DictCursor

# 延迟导入，避免主模块加载时即要求 pymysql 可用


def _get_password_from_env(password: Optional[str] = None) -> Optional[str]:
    """优先从环境变量读取密码。"""
    return password or os.getenv("MYSQL_PASSWORD")


@contextmanager
def get_conn(
    host: str = "127.0.0.1",
    port: int = 3306,
    user: str = "root",
    password: Optional[str] = None,
    charset: str = "utf8mb4",
    connect_timeout: int = 10,
) -> Generator[pymysql.Connection, None, None]:
    """
    MySQL 连接上下文管理器，自动 commit/rollback。

    用法：
        with get_conn(host, port, user, password) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print(cur.fetchone())
    """
    pwd = _get_password_from_env(password)
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=pwd,
            charset=charset,
            connect_timeout=connect_timeout,
            autocommit=False,
        )
    except pymysql.err.OperationalError as e:
        raise ConnectionError(f"MySQL 连接失败 [{host}:{port}]: {e}") from e

    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


def is_mysql_running(host: str = "127.0.0.1", port: int = 3306) -> bool:
    """快速检测 MySQL 是否可连接。"""
    try:
        with get_conn(host, port, "root", connect_timeout=3) as conn:
            return True
    except Exception:
        return False


def get_version(host: str, port: int, user: str, password: Optional[str] = None) -> str:
    """获取 MySQL 版本字符串。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            return cur.fetchone()[0]


def get_server_id(host: str, port: int, user: str, password: Optional[str] = None) -> int:
    """获取 server_id。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT @@server_id")
            row = cur.fetchone()
            return int(row[0]) if row else 0


def get_datadir(host: str, port: int, user: str, password: Optional[str] = None) -> str:
    """获取 datadir 路径。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT @@datadir")
            return cur.fetchone()[0]


def get_data_size(host: str, port: int, user: str, password: Optional[str] = None) -> int:
    """获取数据目录大小（字节）。"""
    datadir = get_datadir(host, port, user, password)
    import os
    total = 0
    for dirpath, _, filenames in os.walk(datadir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total


def get_master_status(
    host: str, port: int, user: str, password: Optional[str] = None
) -> dict[str, Any]:
    """
    获取主库复制坐标。

    返回 {'file': 'mysql-bin.000001', 'position': 1234, 'gtid': 'xxx'} 或 {}
    """
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
            if not row:
                return {}
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))

            # 提取 GTID
            gtid = result.get("Executed_Gtid_Set", "") or result.get("Gtid_Set", "")
            return {
                "file": result.get("File", ""),
                "position": result.get("Position", 0),
                "gtid": gtid,
            }


def get_slave_status(
    host: str, port: int, user: str, password: Optional[str] = None
) -> dict[str, Any]:
    """
    获取备库复制状态。

    返回 {
        'io_running': 'Yes'/'No'/'Connecting',
        'sql_running': 'Yes'/'No',
        'lag': 0,
        'last_error': '',
        'master_host': 'xxx',
    } 或空字典（不是备库）
    """
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW SLAVE STATUS")
            row = cur.fetchone()
            if not row:
                return {}
            cols = [d[0] for d in cur.description]
            result = dict(zip(cols, row))
            return {
                "io_running": result.get("Slave_IO_Running", "No"),
                "sql_running": result.get("Slave_SQL_Running", "No"),
                "lag": result.get("Seconds_Behind_Master", 0) or 0,
                "last_error": result.get("Last_Error", ""),
                "master_host": result.get("Master_Host", ""),
                "master_log_file": result.get("Master_Log_File", ""),
                "read_master_log_pos": result.get("Read_Master_Log_Pos", 0),
                "relay_master_log_file": result.get("Relay_Master_Log_File", ""),
                "exec_master_log_pos": result.get("Exec_Master_Log_Pos", 0),
                "gtid_mode": result.get("Gtid_Mode", ""),
                "auto_position": result.get("Auto_Position", 0),
            }


def create_repl_user(
    host: str,
    port: int,
    user: str,
    password: str,
    repl_user: str,
    repl_password: str,
    repl_hosts: str = "%",
) -> None:
    """在主库创建复制账户。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            # 检查是否已存在
            cur.execute(f"SELECT 1 FROM mysql.user WHERE User='{repl_user}' AND Host='{repl_hosts}'")
            if cur.fetchone():
                return  # 已存在，跳过

            cur.execute(
                f"CREATE USER '{repl_user}'@'{repl_hosts}' "
                f"IDENTIFIED BY '{repl_password}'"
            )
            cur.execute(
                f"GRANT REPLICATION SLAVE ON *.* TO '{repl_user}'@'{repl_hosts}'"
            )
            conn.commit()


def table_count(
    host: str, port: int, user: str, password: Optional[str] = None, database: Optional[str] = None
) -> int:
    """统计某个库的表数量或总行数。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            db = database or "information_schema"
            cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{db}'")
            return int(cur.fetchone()[0])


def get_databases(
    host: str, port: int, user: str, password: Optional[str] = None
) -> list[str]:
    """获取所有数据库名。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return [row[0] for row in cur.fetchall()]


def get_connection_id(host: str, port: int, user: str, password: Optional[str] = None) -> int:
    """获取当前连接的 connection_id。"""
    with get_conn(host, port, user, password) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT connection_id()")
            return int(cur.fetchone()[0])
