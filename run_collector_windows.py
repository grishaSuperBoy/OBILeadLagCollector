"""
Unkillable 7-Day Windows Watchdog Runner for OBI Lead-Lag Collector.
Features:
1. Prevents Windows Sleep & Network Suspension (SetThreadExecutionState).
2. Auto-restarts on any socket disconnect, unhandled crash or network drop within 3s.
3. Dual-storage logging (Local SQLite + MongoDB Atlas).
4. Serves real-time Dark Dashboard on http://localhost:8080.
"""
import sys
import os
import time
import subprocess
import ctypes
from datetime import datetime
from pathlib import Path

# Force UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
WATCHDOG_LOG = LOG_DIR / "watchdog.log"


def log_msg(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{ts}] {msg}"
    print(formatted)
    with open(WATCHDOG_LOG, "a", encoding="utf-8") as f:
        f.write(formatted + "\n")


def prevent_windows_sleep():
    """Блокирует переход Windows в спящий режим и отключение сетевых карт."""
    if sys.platform == "win32":
        try:
            ES_CONTINUOUS = 0x80000000
            ES_SYSTEM_REQUIRED = 0x00000001
            ES_AWAYMODE_REQUIRED = 0x00000040
            res = ctypes.windll.kernel32.SetThreadExecutionState(
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
            )
            if res != 0:
                log_msg("🛡️ Режим защиты от сна Windows успешно активирован (ПК не уснет 7 дней).")
            else:
                log_msg("⚠️ Предупреждение: Не удалось установить флаг сна Windows.")
        except Exception as e:
            log_msg(f"⚠️ Ошибка при вызове SetThreadExecutionState: {e}")


def run_watchdog():
    prevent_windows_sleep()

    python_exe = sys.executable
    main_script = Path("main.py").resolve()

    log_msg("=================================================================")
    log_msg("🚀 СТАРТ 7-ДНЕВНОГО НЕУБИВАЕМОГО СБОРЩИКА OBI LEAD-LAG")
    log_msg(f"📡 Python: {python_exe}")
    log_msg(f"🎯 Скрипт: {main_script}")
    log_msg("💾 Хранилище: Локальный SQLite (data/collector_local.db) + MongoDB Atlas")
    log_msg("🌐 Дашборд в браузере: http://localhost:8080")
    log_msg("=================================================================")

    restart_count = 0

    while True:
        try:
            restart_count += 1
            log_msg(f"▶️ Запуск сессии сборщика (Запуск #{restart_count})...")
            
            # Запускаем основной процесс
            process = subprocess.Popen(
                [python_exe, str(main_script)],
                env=os.environ.copy()
            )

            # Ждем завершения
            exit_code = process.wait()

            log_msg(f"⚠️ Процесс сборщика завершился с кодом: {exit_code}")
            log_msg("🔄 Авто-перезапуск через 3 секунды...")
            time.sleep(3.0)

        except KeyboardInterrupt:
            log_msg("🛑 Остановка сборщика пользователем (Ctrl+C). Корректный выход.")
            if 'process' in locals() and process.poll() is None:
                process.terminate()
            break
        except Exception as e:
            log_msg(f"❌ Критическая ошибка вотчдога: {e}")
            log_msg("🔄 Перезапуск через 5 секунд...")
            time.sleep(5.0)


if __name__ == "__main__":
    run_watchdog()
