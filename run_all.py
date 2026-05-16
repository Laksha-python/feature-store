import subprocess
import sys
import time
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent
DOCKER_COMPOSE = PROJECT_ROOT / "docker" / "docker-compose.yml"
processes = []
def start_process(cmd, cwd=None):
    print(f"\nLaunching: {' '.join(cmd)}\n")
    p = subprocess.Popen(cmd, cwd=cwd)
    processes.append(p)
    return p

def start_docker():
    print("\n[1/2] Starting Docker infrastructure...\n")
    subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(DOCKER_COMPOSE),
            "up",
            "-d"
        ],
        check=True
    )
    print("Docker services started.")
    time.sleep(8)

def start_backend():
    print("\n[2/3] Starting FastAPI backend...\n")
    start_process(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "api.feature_api:app",
            "--reload",
            "--app-dir",
            str(PROJECT_ROOT),
            "--port",
            "8000"
        ],
        cwd=PROJECT_ROOT
    )

def start_ui():
    print("\n[3/3] Starting Streamlit UI...\n")
    start_process(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "ui/app.py",
            "--logger.level=info"
        ],
        cwd=PROJECT_ROOT
    )

def shutdown():
    print("\nStopping services...\n")
    for p in processes:
        try:
            p.terminate()
        except:
            pass

def main():
    try:
        start_docker()
        start_backend()
        start_ui()
        print("\nSystem started successfully\n")
        print("API:")
        print("http://localhost:8000")
        print("\nDocs (Swagger UI):")
        print("http://localhost:8000/docs")
        print("\nUI (Streamlit):")
        print("http://localhost:8501")
        print("\nPress CTRL+C to shutdown\n")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown()

if __name__ == "__main__":
    main()