@echo off
chcp 65001 >nul
title ETF 数据拉取工具
echo ================================================
echo   ETF 数据管道 - 手动触发模式
echo ================================================
echo.
cd /d "%~dp0"
python run_once.py
