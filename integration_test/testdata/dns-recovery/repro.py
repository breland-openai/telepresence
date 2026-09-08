#!/usr/bin/env python3
"""Run a real rootd against injected stale legacy DNS rules in isolated namespaces."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


NAMES = {
    "github.com": "192.0.2.10",
    "cache.build.internal.example": "192.0.2.11",
    "bootstrap.dns-recovery.test": "192.0.2.12",
}
UPSTREAM = "198.18.0.53"
CLIENT = "198.18.0.1"


def run(command, **kwargs):
    return subprocess.run(command, text=True, capture_output=True, check=True, **kwargs)


def dns_server(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host, port = address.rsplit(":", 1)
    sock.bind((host, int(port)))
    print("DNS fixture ready", flush=True)
    while True:
        packet, peer = sock.recvfrom(65535)
        if len(packet) < 17 or struct.unpack("!H", packet[4:6])[0] != 1:
            continue
        try:
            labels, pos = [], 12
            while packet[pos]:
                length = packet[pos]
                labels.append(packet[pos + 1 : pos + 1 + length].decode("ascii"))
                pos += length + 1
            pos += 1
            qtype, qclass = struct.unpack("!HH", packet[pos : pos + 4])
            question = packet[12 : pos + 4]
            name = ".".join(labels).lower()
            address = NAMES.get(name)
            answer = b""
            if address and qtype == 1 and qclass == 1:
                answer = b"\xc0\x0c" + struct.pack("!HHIH", 1, 1, 0, 4)
                answer += socket.inet_aton(address)
            flags = 0x8180 if address else 0x8183
            response = packet[:2] + struct.pack("!HHHHH", flags, 1, bool(answer), 0, 0)
            sock.sendto(response + question + answer, peer)
            print(json.dumps({"query": name, "type": qtype, "answer": address}), flush=True)
        except (IndexError, UnicodeError, struct.error):
            continue


def probe():
    results = {}
    for name, expected in NAMES.items():
        began = time.monotonic()
        try:
            actual = sorted({r[4][0] for r in socket.getaddrinfo(name, 443, socket.AF_INET)})
            results[name] = {"ok": actual == [expected], "addresses": actual}
        except socket.gaierror as error:
            results[name] = {"ok": False, "error": str(error)}
        results[name]["seconds"] = round(time.monotonic() - began, 3)
    return {"ok": all(r["ok"] for r in results.values()), "names": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary")
    parser.add_argument("--expect", choices=["broken", "recovered"])
    parser.add_argument("--user", default=os.environ.get("SUDO_USER", "dev-user"))
    parser.add_argument("--output")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--mode", choices=["run", "dns", "probe", "auth"], default="run")
    parser.add_argument("--listen", default=UPSTREAM + ":53", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.mode == "dns":
        dns_server(args.listen)
        return
    if args.mode in ("probe", "auth"):
        result = probe()
        if args.mode == "auth":
            Path(args.output).write_text(json.dumps(result, indent=2) + "\n")
            print("DNS_RECOVERY_AUTH_PROBE_" + ("PASSED" if result["ok"] else "FAILED"), file=sys.stderr)
            # This fixture deliberately refuses credentials. No Kubernetes request is authorized.
            sys.exit(41 if result["ok"] else 42)
        print(json.dumps(result))
        return
    if not args.binary or not args.expect:
        parser.error("--binary and --expect are required")
    if os.geteuid() != 0:
        parser.error("run as root on a disposable Linux VM")
    for command in ("ip", "iptables", "unshare", "mount", "runuser"):
        if shutil.which(command) is None:
            parser.error("missing dependency: " + command)
    account = pwd.getpwnam(args.user)
    if account.pw_uid == 0:
        parser.error("--user must name a non-root account for the native CLI")
    binary = str(Path(args.binary).resolve(strict=True))
    script = str(Path(__file__).resolve())
    output = Path(args.output or tempfile.mkdtemp(prefix="tp-dns-recovery-"))
    output.mkdir(parents=True, exist_ok=True)
    output = output.resolve()
    if not args.inside:
        command = ["unshare", "--mount", "--propagation", "private", sys.executable, script,
                   "--inside", "--binary", binary, "--expect", args.expect,
                   "--user", args.user, "--output", str(output)]
        result = subprocess.run(command)
        print("Evidence directory: " + str(output))
        sys.exit(result.returncode)

    # These bind mounts only exist inside this process's private mount namespace.
    for target in ("/var/cache/telepresence", "/etc/telepresence"):
        if Path(target).is_dir():
            empty = output / ("masked-" + target.replace("/", "-"))
            empty.mkdir(exist_ok=True)
            run(["mount", "--bind", str(empty), target])

    output.chmod(0o755)
    os.chown(output, account.pw_uid, account.pw_gid)
    client_ns = "tp-dnsc-" + str(os.getpid())
    dns_ns = "tp-dnsu-" + str(os.getpid())
    netns_config = Path("/etc/netns") / client_ns
    processes, namespaces, handles = [], [], []
    events = []

    def event(phase, **fields):
        record = {"phase": phase, **fields}
        events.append(record)
        print(json.dumps(record), flush=True)
        (output / "results.json").write_text(json.dumps(events, indent=2) + "\n")

    def ns(command, namespace=client_ns, **kwargs):
        return run(["ip", "netns", "exec", namespace, *command], **kwargs)

    def check_dns(phase, wanted):
        result = json.loads(ns([sys.executable, script, "--mode", "probe"]).stdout)
        event(phase, **result)
        if result["ok"] != wanted:
            raise AssertionError(f"{phase}: expected DNS ok={wanted}, got {result}")

    def snapshot(label):
        for name, command in (("iptables", ["iptables", "-t", "nat", "-S"]),
                              ("routes", ["ip", "route", "show", "table", "all"])):
            (output / f"{label}-{name}.txt").write_text(ns(command).stdout)

    def assert_routing(stale_present):
        rules = ns(["iptables", "-t", "nat", "-S", "TELEPRESENCE_DNS"]).stdout
        present = "--to-destination 127.0.0.1:19553" in rules
        if present != stale_present:
            raise AssertionError(f"expected fixture DNAT present={stale_present}: {rules}")
        unrelated = ns(["iptables", "-t", "nat", "-S", "DNS_RECOVERY_UNRELATED"]).stdout
        if unrelated != unrelated_rules:
            raise AssertionError("native startup changed an unrelated NAT chain")

    def seed_stale_rules():
        # This is intentional failure injection, not evidence that rootd installed these rules.
        for command in (("-D", "OUTPUT", "-j", "TELEPRESENCE_DNS"),
                        ("-F", "TELEPRESENCE_DNS"), ("-X", "TELEPRESENCE_DNS")):
            subprocess.run(["ip", "netns", "exec", client_ns, "iptables", "-t", "nat", *command],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ns(["iptables", "-t", "nat", "-N", "TELEPRESENCE_DNS"])
        ns(["iptables", "-t", "nat", "-A", "TELEPRESENCE_DNS", "-p", "udp",
            "--source", CLIENT, "--sport", "19554", "-j", "RETURN"])
        ns(["iptables", "-t", "nat", "-A", "TELEPRESENCE_DNS", "-p", "udp",
            "--dest", UPSTREAM + "/32", "--dport", "53", "-j", "DNAT",
            "--to-destination", "127.0.0.1:19553"])
        ns(["iptables", "-t", "nat", "-I", "OUTPUT", "1", "-j", "TELEPRESENCE_DNS"])

    def native_cli(cache, config, command, kubeconfig=None):
        environment = ["XDG_CACHE_HOME=" + str(cache.parent), "XDG_CONFIG_HOME=" + str(config.parent),
                       "TELEPRESENCE_USER_DAEMON_ADDRESS=", "DEV_TELEPRESENCE_CONFIG_DIR=",
                       "DEV_TELEPRESENCE_LOG_DIR="]
        if kubeconfig is not None:
            environment.append("KUBECONFIG=" + str(kubeconfig))
        return ["ip", "netns", "exec", client_ns, "runuser", "-u", args.user, "--", "env",
                *environment, binary, *command]

    def start_rootd(case):
        cache = output / case / "cache" / "telepresence"
        config = output / case / "config" / "telepresence"
        (cache / "logs").mkdir(parents=True)
        config.mkdir(parents=True)
        (config / "config.yml").write_text("logLevels:\n  rootDaemon: debug\n  userDaemon: debug\ntimeouts:\n  clusterConnect: 10s\n  trafficManagerConnect: 10s\n")
        for path in [output / case, *(output / case).rglob("*")]:
            os.chown(path, account.pw_uid, account.pw_gid)
        # The first native CLI invocation initializes version.json and clears an
        # incompatible cache by quitting discovered daemons. Do it before this
        # fixture registers rootd, using the native metadata format.
        initialized = subprocess.run(native_cli(cache, config, ["version", "--format", "json"]),
                                     capture_output=True, text=True, timeout=15)
        (output / case / "cache-initialization.log").write_text(initialized.stdout + initialized.stderr)
        if initialized.returncode != 0 or not (cache / "version.json").is_file():
            raise RuntimeError("native CLI did not initialize its cache; inspect cache-initialization.log")
        initial_versions = json.loads(initialized.stdout)
        if initial_versions.get("root_daemon") != "not running":
            raise RuntimeError("cache initialization unexpectedly discovered a root daemon: " + initialized.stdout)
        (cache / "rootd").mkdir()
        (cache / "rootd" / "daemon.json").write_text('{"daemon_port":19557}\n')
        for path in (cache / "rootd", cache / "rootd" / "daemon.json"):
            os.chown(path, account.pw_uid, account.pw_gid)
        logfile = output / case / "rootd.log"
        handle = logfile.open("w")
        handles.append(handle)
        command = ["ip", "netns", "exec", client_ns, binary, "rootd", "--cache", str(cache),
                   "--config", str(config / "config.yml"), "--logfile", "stdout", "--address", "127.0.0.1:19557"]
        process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT)
        processes.append(process)
        for _ in range(100):
            if process.poll() is not None:
                raise RuntimeError("rootd exited: " + logfile.read_text())
            if "Listener opened" in logfile.read_text():
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("rootd did not open its native listener")
        # A TCP listener alone can precede initialization. The native version command
        # obtains root_daemon through its Version RPC, without creating a session.
        readiness_log = output / case / "readiness.log"
        deadline = time.monotonic() + 30
        attempts = []
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("rootd exited during RPC readiness: " + logfile.read_text())
            try:
                ready = subprocess.run(native_cli(cache, config, ["version", "--format", "json"]),
                                       capture_output=True, text=True, timeout=3)
                attempts.append(ready.stdout + ready.stderr)
                readiness_log.write_text("\n".join(attempts))
                versions = json.loads(ready.stdout)
                root_version = versions.get("root_daemon", "")
                if ready.returncode == 0 and root_version.startswith("v") and root_version == versions.get("client"):
                    event(case + "-native-rootd-ready", pid=process.pid, rpc="Version", version=root_version)
                    break
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as error:
                attempts.append(str(error))
                readiness_log.write_text("\n".join(attempts))
            time.sleep(0.1)
        else:
            raise RuntimeError("native Version RPC did not confirm rootd readiness; inspect readiness.log")
        return process, cache, config, logfile

    try:
        event("identity", binary=binary, sha256=hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
              expected=args.expect, fixture="injected orphan legacy NAT; no cluster connection")
        for namespace in (client_ns, dns_ns):
            run(["ip", "netns", "add", namespace])
            namespaces.append(namespace)
            ns(["ip", "link", "set", "lo", "up"], namespace=namespace)
        run(["ip", "link", "add", "tpc0", "netns", client_ns, "type", "veth", "peer", "name", "tpu0", "netns", dns_ns])
        for namespace, interface, address in ((client_ns, "tpc0", CLIENT), (dns_ns, "tpu0", UPSTREAM)):
            ns(["ip", "address", "add", address + "/24", "dev", interface], namespace=namespace)
            ns(["ip", "link", "set", interface, "up"], namespace=namespace)
        netns_config.mkdir(parents=True)
        (netns_config / "resolv.conf").write_text("nameserver " + UPSTREAM + "\noptions timeout:1 attempts:1\n")
        lines = Path("/etc/nsswitch.conf").read_text().splitlines()
        (netns_config / "nsswitch.conf").write_text("\n".join("hosts: files dns" if line.startswith("hosts:") else line for line in lines) + "\n")
        handle = (output / "dns-fixture.log").open("w")
        handles.append(handle)
        dns_process = subprocess.Popen(["ip", "netns", "exec", dns_ns, sys.executable, script, "--mode", "dns"], stdout=handle, stderr=subprocess.STDOUT)
        processes.append(dns_process)
        for _ in range(50):
            if "DNS fixture ready" in (output / "dns-fixture.log").read_text():
                break
            time.sleep(0.1)
        check_dns("control-direct-physical-resolver", True)
        ns(["iptables", "-t", "nat", "-N", "DNS_RECOVERY_UNRELATED"])
        ns(["iptables", "-t", "nat", "-A", "DNS_RECOVERY_UNRELATED", "-p", "udp",
            "--dest", "198.19.0.53/32", "--dport", "53", "-j", "DNAT",
            "--to-destination", "127.0.0.1:19555"])
        unrelated_rules = ns(["iptables", "-t", "nat", "-S", "DNS_RECOVERY_UNRELATED"]).stdout

        # An occupied Telepresence listener is preserved, even in a separate native rootd startup.
        live_log = output / "live-listener.log"
        live_handle = live_log.open("w")
        handles.append(live_handle)
        live = subprocess.Popen(["ip", "netns", "exec", client_ns, sys.executable, script,
                                 "--mode", "dns", "--listen", "127.0.0.1:19553"],
                                stdout=live_handle, stderr=subprocess.STDOUT)
        processes.append(live)
        for _ in range(50):
            if "DNS fixture ready" in live_log.read_text():
                break
            time.sleep(0.1)
        seed_stale_rules()
        check_dns("control-live-listener-before-startup", True)
        live_rootd, _, _, _ = start_rootd("live-control")
        check_dns("control-live-listener-after-startup", True)
        assert_routing(True)
        snapshot("live-control-preserved")
        live_rootd.terminate()
        live_rootd.wait(timeout=10)
        live.terminate()
        live.wait(timeout=10)
        event("control-live-listener-and-unrelated-chain-preserved")

        recovered = args.expect == "recovered"
        for case, stop_signal in (("graceful", signal.SIGTERM), ("killed", signal.SIGKILL)):
            seed_stale_rules()
            snapshot(case + "-injected")
            check_dns(case + "-injected-stale-nat", False)
            process, cache, config, logfile = start_rootd(case)
            check_dns(case + "-after-native-startup", recovered)
            assert_routing(not recovered)
            snapshot(case + "-started")

            auth_file = output / case / "auth-result.json"
            auth_file.touch()
            os.chown(auth_file, account.pw_uid, account.pw_gid)
            kubeconfig = output / case / "kubeconfig.json"
            kubeconfig.write_text(json.dumps({
                "apiVersion": "v1", "kind": "Config", "current-context": "dns-recovery",
                "clusters": [{"name": "fixture", "cluster": {"server": "https://198.18.0.53:16443"}}],
                "contexts": [{"name": "dns-recovery", "context": {"cluster": "fixture", "user": "fixture"}}],
                "users": [{"name": "fixture", "user": {"exec": {
                    "apiVersion": "client.authentication.k8s.io/v1", "interactiveMode": "Never",
                    "command": sys.executable, "args": [script, "--mode", "auth", "--output", str(auth_file)]}}}],
            }))
            os.chown(kubeconfig, account.pw_uid, account.pw_gid)
            cli_command = native_cli(cache, config, ["connect", "--context", "dns-recovery",
                                     "--manager-namespace", "ambassador", "--progress", "plain"], kubeconfig)
            try:
                cli = subprocess.run(cli_command, capture_output=True, text=True, timeout=25)
                (output / case / "cli.log").write_text(cli.stdout + cli.stderr)
            except subprocess.TimeoutExpired as error:
                (output / case / "cli.log").write_text(str(error))
                raise RuntimeError("native CLI bootstrap did not finish within 25 seconds") from error
            if not auth_file.read_text():
                raise RuntimeError("native CLI never invoked the exec-auth DNS probe; inspect cli.log")
            auth_result = json.loads(auth_file.read_text())
            event(case + "-native-cli-exec-auth", cli_exit=cli.returncode, **auth_result)
            if auth_result["ok"] != recovered:
                raise AssertionError("exec-auth result disagrees with expected host DNS state")
            if "-- Starting new session" in logfile.read_text():
                raise AssertionError("rootd Connect ran; fixture must stop before Kubernetes bootstrap")
            process.send_signal(stop_signal)
            process.wait(timeout=10)
            event(case + "-rootd-stopped", signal=stop_signal.name, exit=process.returncode)
            check_dns(case + "-after-stop", recovered)
        event("PASS", expectation=args.expect)
    finally:
        for namespace in namespaces:
            listed = subprocess.run(["ip", "netns", "pids", namespace], capture_output=True, text=True)
            for pid in listed.stdout.split():
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        for handle in handles:
            handle.close()
        for namespace in reversed(namespaces):
            subprocess.run(["ip", "netns", "delete", namespace], check=False)
        shutil.rmtree(netns_config, ignore_errors=True)


if __name__ == "__main__":
    main()
