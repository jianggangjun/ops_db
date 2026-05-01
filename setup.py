from setuptools import setup, find_packages

setup(
    name="ops_db",
    version="0.1.0",
    description="MySQL 运维工具 — 安装、备份、恢复、主从、重搭、健康检查",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pymysql>=1.1.0",
        "paramiko>=3.0.0",
        "jinja2>=3.0.0",
    ],
    entry_points={
        "console_scripts": [
            "ops_db=ops_db.__main__:main",
        ],
    },
)
