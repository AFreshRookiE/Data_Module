"""
打包脚本：使用 PyInstaller 将 run_once.py 打包为 Windows exe。
运行方式：python build_exe.py
"""

import subprocess
import sys


def build() -> None:
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",                          # 打包为单个 exe
        "--console",                          # 保留控制台窗口（显示进度）
        "--name", "ETF数据拉取",              # exe 文件名
        "--add-data", "config.yaml;.",        # 将 config.yaml 打包进去
        "--hidden-import", "akshare",
        "--hidden-import", "dolphindb",
        "--hidden-import", "chinese_calendar",
        "--hidden-import", "apscheduler",
        "--hidden-import", "pydantic_settings",
        "--hidden-import", "tenacity",
        "--hidden-import", "pandas",
        "--hidden-import", "pyarrow",
        "--hidden-import", "yaml",
        "run_once.py",
    ]

    print("开始打包，请稍候...\n")
    result = subprocess.run(cmd, check=False)

    if result.returncode == 0:
        print("\n打包成功！exe 文件位于 dist/ETF数据拉取.exe")
        print("\n注意：首次运行前请将 config.yaml 放在 exe 同目录下，")
        print("      并填写正确的 DolphinDB 连接信息。")
    else:
        print("\n打包失败，请确认已安装 PyInstaller：pip install pyinstaller")
        sys.exit(1)


if __name__ == "__main__":
    build()
