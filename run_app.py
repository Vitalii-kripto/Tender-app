import subprocess
import sys
import os
import time
import signal

def run_services():
    """
    Запускает React Frontend и FastAPI Backend в параллельных процессах.
    """
    print("==================================================")
    print("   TenderSmart Gidroizol AI - ЗАПУСК СИСТЕМЫ")
    print("==================================================")

    # Определяем команду для npm в зависимости от ОС
    # Windows требует npm.cmd, Linux/Mac просто npm
    npm_cmd = "npm.cmd" if os.name == 'nt' else "npm"
    
    # Пути
    project_root = os.getcwd()
    
    processes = []

    try:
        # 1. Запуск Frontend (Vite)
        print(f"🚀 Запуск Frontend ({npm_cmd} run dev)...")
        frontend_process = subprocess.Popen(
            [npm_cmd, "run", "dev"],
            cwd=project_root,
            shell=False, # Shell=False безопаснее и позволяет лучше управлять процессом
            env=os.environ.copy()
        )
        processes.append(frontend_process)

        # Небольшая пауза, чтобы фронт успел инициализироваться (опционально)
        time.sleep(1)

        # 2. Запуск Backend (Python)
        # Используем текущий интерпретатор python (из venv)
        python_executable = sys.executable 
        print(f"🐍 Запуск Backend ({python_executable} backend.py)...")
        
        backend_process = subprocess.Popen(
            [python_executable, "backend.py"],
            cwd=project_root,
            env=os.environ.copy()
        )
        processes.append(backend_process)

        print("\n✅ Оба сервиса запущены. Нажмите Ctrl+C для остановки.\n")
        
        # Ожидаем завершения процессов (фактически висит, пока не нажмем Ctrl+C)
        backend_process.wait()
        frontend_process.wait()

    except KeyboardInterrupt:
        print("\n\n🛑 Остановка сервисов...")
    finally:
        # Принудительное завершение при выходе
        for p in processes:
            if p.poll() is None: # Если процесс еще жив
                p.terminate()
                # p.kill() # Если terminate не сработает
        print("Система остановлена.")

if __name__ == "__main__":
    run_services()
