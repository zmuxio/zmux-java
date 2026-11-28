#!/usr/bin/env python3
"""Run H3 and WebTransport over every pair of supplied wire peers."""

import argparse
import itertools
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path


def stop(process):
    if process is None or process.poll() is not None:
        return
    if os.name == "nt":
        process.kill()
        process.wait(timeout=5)
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        process.kill()
    process.wait(timeout=5)


def wait_for_line(server, stdout, deadline, prefix, offset=0):
    with stdout.open("rb") as output:
        output.seek(offset)
        while time.monotonic() < deadline:
            offset = output.tell()
            line = output.readline(8193)
            if len(line) > 8192:
                raise ValueError("server output line exceeded 8192 bytes")
            if line.endswith(b"\n"):
                message = line.decode(errors="replace").strip()
                print(f"  server: {message}", flush=True)
                if message.startswith(prefix):
                    return message, output.tell()
            elif server.poll() is not None:
                raise ValueError(
                    f"server exited {server.returncode} before {prefix.strip()}"
                )
            else:
                output.seek(offset)
                time.sleep(min(0.025, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"deadline waiting for {prefix.strip()}")


def run_case(server_path, client_path, protocol, cert, key, timeout, *, dedicated_server=False):
    deadline = time.monotonic() + timeout
    server = client = None
    offset = 0
    client_env = dict(os.environ, ZMUX_WT_DEDICATED_SERVER="1" if dedicated_server else "0")
    with (
        tempfile.TemporaryDirectory(prefix="cell-") as directory,
        tempfile.TemporaryFile() as server_log,
        tempfile.TemporaryFile() as client_log,
    ):
        stdout = Path(directory) / "server.stdout"
        try:
            with stdout.open("wb") as output:
                server = subprocess.Popen(
                    [server_path, "server", protocol, "0", cert, key],
                    stdout=output,
                    stderr=server_log,
                    start_new_session=os.name != "nt",
                )
            ready, offset = wait_for_line(server, stdout, deadline, "READY ")
            port = int(ready.removeprefix("READY "))
            if not 0 < port <= 65535:
                raise ValueError(f"invalid readiness port {port}")
            client = subprocess.Popen(
                [client_path, "client", protocol, str(port), cert, key],
                stdout=client_log,
                stderr=subprocess.STDOUT,
                env=client_env,
                start_new_session=os.name != "nt",
            )
            result = client.wait(timeout=max(0, deadline - time.monotonic()))
            if result:
                raise ValueError(f"client exit {result}")
            if protocol == "wt":
                _, offset = wait_for_line(
                    server, stdout, deadline, "PASS wt server", offset
                )
            if server.poll() not in (0, None):
                raise ValueError(f"server exit {server.returncode}")
            return True
        except (OSError, subprocess.TimeoutExpired, ValueError) as error:
            print(f"  {error}", flush=True)
            return False
        finally:
            stop(client)
            stop(server)
            if stdout.exists():
                with stdout.open("rb") as output:
                    output.seek(offset)
                    remainder = output.read().decode(errors="replace").strip()
                if remainder:
                    print(f"  server stdout: {remainder}", flush=True)
            for label, log in (("server", server_log), ("client", client_log)):
                log.seek(0)
                output = log.read().decode(errors="replace").strip()
                if output:
                    print(f"  {label}: {output}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for peer in ("java", "go", "rust"):
        parser.add_argument(
            f"--{peer}",
            type=Path,
            required=peer == "java",
            help=f"executable {peer} peer",
        )
    parser.add_argument("--protocol", action="append", choices=("h3", "wt"))
    parser.add_argument(
        "--timeout", type=int, default=30, help="hard deadline per cell in seconds"
    )
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    peers = {}
    for name in ("java", "go", "rust"):
        path = getattr(args, name)
        if path is not None:
            if not path.is_file() or not os.access(path, os.X_OK):
                parser.error(f"{name} peer {path} is not executable")
            peers[name] = path.resolve()

    directions = [("java", "java")]
    directions.extend(itertools.permutations(peers, 2))

    failures = []
    with tempfile.TemporaryDirectory(prefix="zmux-h3-wire-") as directory:
        cert, key = (Path(directory) / filename for filename in ("cert.pem", "key.pem"))
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-out",
                cert,
                "-keyout",
                key,
            ],
            check=True,
            timeout=15,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        for protocol, (client, server) in itertools.product(
            args.protocol or ("h3", "wt"), directions
        ):
            cell = f"{protocol} {client} client -> {server} server"
            print(cell, flush=True)
            passed = run_case(
                peers[server], peers[client], protocol, cert, key, args.timeout,
                dedicated_server=server == "rust",
            )
            print(f"  {'PASS' if passed else 'FAIL'}", flush=True)
            if not passed:
                failures.append(cell)
    if failures:
        print("Failed cells:", *failures, sep="\n  ")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
