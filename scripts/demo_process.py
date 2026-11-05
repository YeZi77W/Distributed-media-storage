"""Linux foreground ACL service launcher. PID + executable + starttime ownership."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

children = {}


def identity(pid):
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return [os.readlink(f"/proc/{pid}/exe"), stat[19]] if stat[0] != "Z" else None
    except (OSError, IndexError):
        return None


def reachable(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def stop(records):
    for record in reversed(records):
        pid = record["pid"]
        if identity(pid) == record["identity"]:
            os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 45
    while any(identity(r["pid"]) == r["identity"] for r in records):
        if time.monotonic() >= deadline:
            raise RuntimeError("owned processes still alive after SIGTERM; state retained")
        time.sleep(0.2)


def launch(a, label, exe, port):
    service_env = os.environ.copy()
    service_env["MASTER_SERVICE"] = "ALONE"
    binary = (a.build/"bin"/exe).resolve()
    with (a.run_dir/(label+".log")).open("ab") as log:
        child = subprocess.Popen([str(binary), "--config", str((a.config_dir/(label+".cfg")).resolve()),
                                  "--listen", f"127.0.0.1:{port}"],
                                 env=service_env, cwd=a.run_dir, stdin=subprocess.DEVNULL,
                                 stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    children[child.pid] = child
    token = identity(child.pid)
    for _ in range(50):
        if token and token[0] == str(binary):
            break
        if child.poll() is not None:
            break
        time.sleep(.02)
        token = identity(child.pid)
    if not token or token[0] != str(binary):
        raise RuntimeError(f"cannot establish PID ownership: {label}; exit={child.poll()}; log={a.run_dir}/{label}.log")
    return {"pid": child.pid, "identity": token, "label": label}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["start", "stop", "stop-a", "start-a"])
    p.add_argument("--build", type=Path)
    p.add_argument("--config-dir", type=Path, default=Path(__file__).resolve().parents[1]/"etc/demo")
    p.add_argument("--run-dir", type=Path, default=Path.home()/"tnv-demo/run")
    a = p.parse_args()
    a.run_dir.mkdir(parents=True, exist_ok=True)
    with (a.run_dir/"lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = a.run_dir/"processes.json"
        if a.action in ("stop-a", "start-a"):
            records = json.loads(state.read_text())
            selected = [r for r in records if r["label"] == "storage-a"]
            if len(selected) != 1:
                raise RuntimeError("storage-a state missing/ambiguous")
            if a.action == "stop-a":
                stop(selected)
            else:
                if not a.build:
                    p.error("start-a requires --build")
                if identity(selected[0]["pid"]) == selected[0]["identity"] or reachable(23000):
                    raise RuntimeError("storage-a still alive or port occupied")
                record = launch(a, "storage-a", "storage", 23000)
                records = [record if r["label"] == "storage-a" else r for r in records]
                state.write_text(json.dumps(records, indent=2))
            return
        if a.action == "stop":
            if state.exists():
                stop(json.loads(state.read_text()))
                state.unlink()
            return
        if not a.build:
            p.error("start requires --build")
        if state.exists():
            raise RuntimeError("state already exists; run stop with the same --run-dir first")
        if subprocess.run(["mountpoint", "-q", "/mnt/tnv/group001"]).returncode:
            raise RuntimeError("/mnt/tnv/group001 must already be mounted")
        services = [("id", "id", 22000), ("tracker", "tracker", 21010),
                    ("storage-a", "storage", 23000), ("storage-b", "storage", 23001)]
        for label, exe, port in services:
            if reachable(port):
                raise RuntimeError(f"port {port} already in use")
            if not (a.build/"bin"/exe).is_file() or not (a.config_dir/(label+".cfg")).is_file():
                raise RuntimeError(f"missing binary/config for {label}")
        records = []
        try:
            for label, exe, port in services:
                record = launch(a, label, exe, port)
                child_pid = record["pid"]
                records.append(record)
                state.write_text(json.dumps(records, indent=2))
                deadline = time.monotonic()+20
                while not reachable(port):
                    if identity(child_pid) != record["identity"] or time.monotonic() >= deadline:
                        raise RuntimeError(f"{label} failed readiness; exit={children[child_pid].poll()}; log={a.run_dir}/{label}.log")
                    time.sleep(.2)
            # TCP readiness is not registration readiness: bound the real Client check too.
            deadline = time.monotonic()+20
            while True:
                result = subprocess.run([str((a.build/"bin/client").resolve()), "127.0.0.1:21010", "groups"],
                                        capture_output=True, text=True, timeout=10)
                if result.returncode == 0 and "COUNT OF ACTIVE STORAGES: 2" in result.stdout:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("two active Storages not visible via Client groups")
                time.sleep(1)
            print(f"Started four foreground services; state/logs: {a.run_dir}")
        except BaseException:
            stop(records)
            state.unlink(missing_ok=True)
            raise

if __name__ == "__main__":
    main()
