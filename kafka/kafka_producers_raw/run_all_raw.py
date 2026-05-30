# run_all_raw_topics.py

import subprocess
import signal
import time

processes = []

scripts = [
    "visa_raw.py",
    "mastercard_raw.py",
    "mpesa_raw.py",
    "pesapal_raw.py",
    "flutter_raw.py"
]


def shutdown(signum=None, frame=None):
    print("\nStopping all producers...")

    for process in processes:
        process.terminate()

    for process in processes:
        process.wait()

    print("All producers stopped.")
    exit(0)


if __name__ == "__main__":

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Starting all raw topic producers...\n")

    for script in scripts:
        process = subprocess.Popen(["python", script])
        processes.append(process)
        print(f"Started {script}")

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        shutdown()