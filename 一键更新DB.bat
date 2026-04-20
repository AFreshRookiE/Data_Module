@echo off
chcp 65001 >nul
title 一键更新DB
cd /d D:\AKShare_Module
D:\Anaconda\python.exe update_db.py
pause
