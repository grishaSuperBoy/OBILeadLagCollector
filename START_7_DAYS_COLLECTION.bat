@echo off
title OBI Lead-Lag Collector (7-Day Autonomous Station)
color 0A
chcp 65001 > nul

echo =====================================================================
echo  🚀 ЗАПУСК 7-ДНЕВНОГО АВТОНОМНОГО СБОРЩИКА ДАННЫХ
echo  💾 Хранение: data/collector_local.db (SQLite) + MongoDB Atlas
echo  🌐 Живой дашборд: http://localhost:8080
echo  🛡️ Защита от сна Windows: ВКЛЮЧЕНА
echo =====================================================================

cd /d "%~dp0"

"C:\Users\Vitali\AppData\Local\pypoetry\Cache\virtualenvs\obi-lead-lag-collector-Vl9LR9Vq-py3.11\Scripts\python.exe" run_collector_windows.py

pause
