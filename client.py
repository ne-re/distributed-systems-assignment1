#!/usr/bin/env python3
"""
Distributed Systems Assignment 1: Mini Banking Transaction Client & Quorum Witness
Supports Interactive CLI, Normal Operations, 6 Error Cases, Concurrency, Atomicity,
Max-Clients Limit, Primary-Backup Failover (Section 5), and Network Partition / Split-Brain Demo (Section 6).
"""

import argparse
import json
import socket
import threading
import time
import uuid


class ClusterEndpoint:
    """Tracks the currently active server (Primary vs Backup) for automatic client failover."""

    def __init__(self, primary_host: str, primary_port: int, backup_host: str | None = None, backup_port: int = 5000):
        self.primary = (primary_host, primary_port)
        self.backup = (backup_host, backup_port) if backup_host else None
        self.current = self.primary
        self.lock = threading.Lock()

    def get_target(self) -> tuple[str, int]:
        with self.lock:
            return self.current

    def switch_to_backup(self) -> tuple[str, int] | None:
        with self.lock:
            if self.backup and self.current != self.backup:
                print(
                    f"\n[CLIENT FAILOVER] Primary ({self.primary[0]}:{self.primary[1]}) is unreachable! "
                    f"Automatically switching to Backup ({self.backup[0]}:{self.backup[1]})...",
                    flush=True,
                )
                self.current = self.backup
            return self.current


def start_witness_service(listen_port: int = 5001) -> None:
    """
    Starts a lightweight Quorum Witness / Arbiter TCP service in the background on the Client VM.
    Helps Server B distinguish between 'Server A crashed' vs 'Network link between A and B is cut'.
    """
    def handle_witness_conn(conn: socket.socket):
        try:
            conn.settimeout(2.0)
            data = conn.recv(1024).decode("utf-8", errors="replace").strip()
            if not data:
                return
            msg = json.loads(data)
            cmd = msg.get("internal_cmd")

            if cmd == "witness_ping":
                conn.sendall((json.dumps({"status": "alive"}) + "\n").encode("utf-8"))

            elif cmd == "can_promote":
                p_host = msg.get("primary_host")
                p_port = int(msg.get("primary_port", 5000))
                # Witness checks if Primary is still alive from Witness's perspective
                primary_alive = False
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as psock:
                        psock.settimeout(1.0)
                        psock.connect((p_host, p_port))
                        psock.sendall((json.dumps({"internal_cmd": "heartbeat"}) + "\n").encode("utf-8"))
                        presp = json.loads(psock.recv(1024).decode("utf-8").strip())
                        if presp.get("status") == "alive" and presp.get("role") == "primary":
                            primary_alive = True
                except Exception:
                    primary_alive = False

                # Allow Backup to promote ONLY if Primary is truly unreachable/dead
                conn.sendall((json.dumps({"allow_promote": not primary_alive}) + "\n").encode("utf-8"))
        except Exception:
            pass
        finally:
            conn.close()

    def server_loop():
        wsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        wsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        wsock.bind(("0.0.0.0", listen_port))
        wsock.listen(32)
        while True:
            try:
                conn, _ = wsock.accept()
                threading.Thread(target=handle_witness_conn, args=(conn,), daemon=True).start()
            except Exception:
                break

    t = threading.Thread(target=server_loop, daemon=True)
    t.start()


def send_raw_to_node(host: str, port: int, raw_text: str, timeout: float = 3.0) -> dict:
    """Sends a raw newline-terminated string to a specific server node."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.sendall((raw_text + "\n").encode("utf-8"))
        response_data = b""
        while not response_data.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_data += chunk
        if not response_data:
            raise ConnectionError("Server closed connection without sending a reply")
        return json.loads(response_data.decode("utf-8").strip())


def send_raw_payload(endpoint: ClusterEndpoint, raw_text: str) -> dict:
    """Sends raw payload with automatic failover to Backup if Primary crashes."""
    host, port = endpoint.get_target()
    try:
        return send_raw_to_node(host, port, raw_text)
    except (socket.error, ConnectionError, TimeoutError) as exc:
        if endpoint.backup:
            b_host, b_port = endpoint.switch_to_backup()
            return send_raw_to_node(b_host, b_port, raw_text)
        raise exc


def send_request(endpoint: ClusterEndpoint, payload: dict) -> dict:
    """Serializes a dictionary to JSON with a persistent req_id for idempotency."""
    if "req_id" not in payload:
        payload["req_id"] = uuid.uuid4().hex[:6]
    return send_raw_payload(endpoint, json.dumps(payload))


def run_interactive_mode(endpoint: ClusterEndpoint) -> None:
    host, port = endpoint.get_target()
    print(f"=== Interactive Client Console Connected to {host}:{port} ===")
    if endpoint.backup:
        print(f"    Backup Server Configured: {endpoint.backup[0]}:{endpoint.backup[1]}")
    print("You can send requests in two ways:")
    print('  1. Raw JSON: {"action": "deposit", "account": "A1001", "amount": 200}')
    print("  2. Short syntax: balance <account> | deposit <account> <amount> | withdraw <account> <amount>")
    print("Type 'exit' or 'quit' to close the console.\n")

    while True:
        try:
            user_input = input("bank-client> ").strip()
            if not user_input:
                continue
            if user_input.lower() in {"exit", "quit"}:
                print("Exiting interactive console.")
                break

            if user_input.startswith("{"):
                response = send_raw_payload(endpoint, user_input)
            else:
                parts = user_input.split()
                cmd = parts[0].lower()
                payload = {"req_id": uuid.uuid4().hex[:6]}

                if cmd == "balance" and len(parts) >= 2:
                    payload.update({"action": "balance", "account": parts[1]})
                elif cmd in {"deposit", "withdraw"} and len(parts) >= 3:
                    try:
                        amt = float(parts[2])
                    except ValueError:
                        amt = parts[2]
                    payload.update({"action": cmd, "account": parts[1], "amount": amt})
                else:
                    response = send_raw_payload(endpoint, user_input)
                    print(f"Server Response:\n{json.dumps(response, indent=2)}\n")
                    continue

                response = send_request(endpoint, payload)

            print(f"Server Response:\n{json.dumps(response, indent=2)}\n")

        except KeyboardInterrupt:
            print("\nExiting interactive console.")
            break
        except Exception as exc:
            print(f"[ERROR] Communication failed: {exc}\n")


def run_normal_demo(endpoint: ClusterEndpoint) -> None:
    print("=== 1. Normal Operations Demo ===")
    steps = [
        ("Initial Balance Inquiry (A1001)", {"action": "balance", "account": "A1001"}),
        ("Deposit 300 into A1001", {"action": "deposit", "account": "A1001", "amount": 300}),
        ("Withdraw 200 from A1001", {"action": "withdraw", "account": "A1001", "amount": 200}),
        ("Final Balance Inquiry (A1001)", {"action": "balance", "account": "A1001"}),
    ]
    for title, req in steps:
        resp = send_request(endpoint, req)
        print(f"[*] {title}:\n    Request : {req}\n    Response: {resp}\n")


def run_error_demo(endpoint: ClusterEndpoint) -> None:
    print("=== 2. Error Handling Demo (All 6 Required Cases) ===")
    error_cases = [
        ("Case 1: Unknown Account", {"action": "balance", "account": "Z9999"}),
        ("Case 2: Insufficient Funds", {"action": "withdraw", "account": "A1002", "amount": 50000}),
        ("Case 3a: Negative Amount (-50)", {"action": "deposit", "account": "A1001", "amount": -50}),
        ("Case 3b: Zero Amount (0)", {"action": "withdraw", "account": "A1001", "amount": 0}),
        ("Case 3c: Invalid String Amount ('abc')", {"action": "deposit", "account": "A1001", "amount": "abc"}),
        ("Case 4: Unsupported Action ('transfer')", {"action": "transfer", "account": "A1001", "amount": 100}),
        ("Case 5: Missing Fields (withdraw without amount)", {"action": "withdraw", "account": "A1001"}),
    ]
    for title, req in error_cases:
        resp = send_request(endpoint, req)
        print(f"[*] {title}:\n    Request : {req}\n    Response: {resp}\n")

    malformed_str = "{{{broken_json_random_bytes_!@#$"
    resp = send_raw_payload(endpoint, malformed_str)
    print(f"[*] Case 6: Malformed Request (Broken JSON):\n    Raw Sent: {malformed_str}\n    Response: {resp}\n")


def run_concurrency_demo(endpoint: ClusterEndpoint, num_threads: int = 15, withdraw_amount: float = 10.0) -> None:
    account = "A1003"
    print(f"=== 3. Concurrency & Isolation Demo ({num_threads} Simultaneous Withdrawals on {account}) ===")

    initial_resp = send_request(endpoint, {"action": "balance", "account": account})
    initial_balance = initial_resp.get("balance", 0.0)
    print(f"[*] Starting balance for {account}: {initial_balance}")

    results = []
    results_lock = threading.Lock()
    barrier = threading.Barrier(num_threads)

    def worker(tid: int):
        barrier.wait()
        resp = send_request(
            endpoint,
            {"action": "withdraw", "account": account, "amount": withdraw_amount, "req_id": f"th{tid:02d}-{uuid.uuid4().hex[:4]}"}
        )
        with results_lock:
            results.append((tid, resp))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    success_count = sum(1 for _, r in results if r.get("status") == "success")
    error_count = num_threads - success_count

    final_resp = send_request(endpoint, {"action": "balance", "account": account})
    final_balance = final_resp.get("balance", 0.0)
    expected_balance = initial_balance - (success_count * withdraw_amount)

    print(f"[*] Concurrent Withdrawals Completed: {success_count} succeeded, {error_count} rejected.")
    print(f"[*] Expected Final Balance: {expected_balance}")
    print(f"[*] Actual Final Balance  : {final_balance}")
    assert abs(final_balance - expected_balance) < 1e-6, "Race condition detected! Balance mismatch!"
    print("[SUCCESS] Concurrency test passed with zero lost updates!\n")


def run_atomicity_demo(endpoint: ClusterEndpoint) -> None:
    account = "A1001"
    print(f"=== 4. Atomicity (Mid-Transaction Failure Rollback) Demo on {account} ===")

    before = send_request(endpoint, {"action": "balance", "account": account})
    print(f"[*] Balance BEFORE failed transaction: {before.get('balance')}")

    fail_req = {
        "action": "withdraw",
        "account": account,
        "amount": 100,
        "simulate_fail": True,
    }
    fail_resp = send_request(endpoint, fail_req)
    print(f"[*] Mid-transaction failure response: {fail_resp}")

    after = send_request(endpoint, {"action": "balance", "account": account})
    print(f"[*] Balance AFTER failed transaction : {after.get('balance')}")
    assert before.get("balance") == after.get("balance"), "Atomicity violated!"
    print("[SUCCESS] Atomicity verified: account balance remained unchanged!\n")


def run_limit_demo(endpoint: ClusterEndpoint, burst_clients: int = 25) -> None:
    print(f"=== 5. Max Simultaneous Clients Limit Demo (Sending {burst_clients} concurrent connections) ===")
    barrier = threading.Barrier(burst_clients)
    results = []
    lock = threading.Lock()

    def worker(tid: int):
        barrier.wait()
        resp = send_request(
            endpoint,
            {"action": "balance", "account": "A1001", "hold_ms": 400, "req_id": f"lim{tid:02d}-{uuid.uuid4().hex[:4]}"}
        )
        with lock:
            results.append(resp)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(burst_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = sum(1 for r in results if r.get("status") == "success")
    rejected = sum(1 for r in results if r.get("status") == "error")
    print(f"[*] Out of {burst_clients} simultaneous clients: {accepted} served, {rejected} rejected due to max-clients limit.\n")


def run_failover_demo(endpoint: ClusterEndpoint) -> None:
    """Demonstrates Section 5 Bonus: Primary crash and automatic failover to Backup."""
    if not endpoint.backup:
        print("[ERROR] You must provide --backup-host <SERVER_B_IP> to run the failover demo!")
        return

    account = "A1001"
    total_tx = 15
    deposit_amount = 10.0

    print("=== 6. Advanced Bonus (Section 5): Primary-Backup Failover & Idempotency Demo ===")
    init_resp = send_request(endpoint, {"action": "balance", "account": account})
    initial_balance = init_resp.get("balance", 0.0)
    print(f"[*] Initial Balance of {account}: {initial_balance}")
    print("    >>> ACTION REQUIRED: Kill Server A (Ctrl+C) around transaction #5! <<<\n")

    for i in range(1, total_tx + 1):
        tx_id = f"failover-tx-{i:02d}-{uuid.uuid4().hex[:4]}"
        req = {"action": "deposit", "account": account, "amount": deposit_amount, "req_id": tx_id}
        resp = send_request(endpoint, req)
        active_host, _ = endpoint.get_target()
        print(f"    [Tx {i:02d}/{total_tx}] req_id={tx_id} Routed to {active_host} -> Response: {resp}")
        time.sleep(1.0)

    print("\n[*] Testing Idempotency (Duplicate Prevention): Resending last transaction ID...")
    dup_resp = send_request(endpoint, req)
    print(f"    Duplicate Request Response: {dup_resp}")

    final_resp = send_request(endpoint, {"action": "balance", "account": account})
    final_balance = final_resp.get("balance", 0.0)
    expected_balance = initial_balance + (total_tx * deposit_amount)

    print(f"\n[*] Initial Balance : {initial_balance}")
    print(f"[*] Expected Balance: {expected_balance}")
    print(f"[*] Actual Balance  : {final_balance}")
    assert abs(final_balance - expected_balance) < 1e-6, "Failover balance mismatch!"
    print("[SUCCESS] Fault-tolerant failover verified! Zero transactions lost or applied twice.\n")


def run_partition_demo(endpoint: ClusterEndpoint) -> None:
    """
    Demonstrates Section 6 Extreme Bonus: Network Partition and Split-Brain Prevention.
    While the link between Server A and Server B is cut:
    - Client sends deposits to Server A (which holds Quorum with Witness -> Succeeds)
    - Client simultaneously attempts conflicting withdrawals on Server B (which lacks Quorum -> Rejected!)
    When the network link is restored, Server B automatically synchronizes missed transactions from Server A.
    """
    if not endpoint.backup:
        print("[ERROR] You must provide --backup-host <SERVER_B_IP> to run the partition demo!")
        return

    p_host, p_port = endpoint.primary
    b_host, b_port = endpoint.backup
    account = "A1001"

    print("=== 7. Extreme Bonus (Section 6): Network Partition & Split-Brain Prevention Demo ===")
    init_a = send_raw_to_node(p_host, p_port, json.dumps({"action": "balance", "account": account, "req_id": "init-a"}))
    init_b = send_raw_to_node(b_host, b_port, json.dumps({"action": "balance", "account": account, "req_id": "init-b"}))
    start_bal = init_a.get("balance", 0.0)
    print(f"[*] Starting Balance on Server A ({p_host}): {start_bal}")
    print(f"[*] Starting Balance on Server B ({b_host}): {init_b.get('balance')}")
    print("\n>>> STEP 1: Cut the network link between Server A and Server B now! <<<")
    print(f"    Run on Server A: sudo iptables -A INPUT -s {b_host} -j DROP && sudo iptables -A OUTPUT -d {b_host} -j DROP\n")

    accepted_on_a = 0
    for i in range(1, 16):
        tx_a = f"part-A-{i:02d}-{uuid.uuid4().hex[:4]}"
        req_a = {"action": "deposit", "account": account, "amount": 10.0, "req_id": tx_a}
        resp_a = send_raw_to_node(p_host, p_port, json.dumps(req_a))
        if resp_a.get("status") == "success":
            accepted_on_a += 1

        # Simultaneously try to send a conflicting withdrawal directly to Server B to test Split-Brain!
        tx_b = f"part-B-{i:02d}-{uuid.uuid4().hex[:4]}"
        req_b = {"action": "withdraw", "account": account, "amount": 50.0, "req_id": tx_b}
        resp_b = send_raw_to_node(b_host, b_port, json.dumps(req_b))

        print(f"[Step {i:02d}/15]")
        print(f"   -> Server A (Primary + Witness Quorum) Deposit +10 : {resp_a}")
        print(f"   -> Server B (Isolated Backup) Conflicting Withdraw : {resp_b}")
        time.sleep(1.5)

    print("\n>>> STEP 2: Restore the network link between Server A and Server B now! <<<")
    print("    Run on Server A: sudo iptables -F")
    print("    Waiting 5 seconds for automatic state reconciliation...\n")
    time.sleep(5.0)

    final_a = send_raw_to_node(p_host, p_port, json.dumps({"action": "balance", "account": account, "req_id": "fin-a"}))
    final_b = send_raw_to_node(b_host, b_port, json.dumps({"action": "balance", "account": account, "req_id": "fin-b"}))
    expected = start_bal + (accepted_on_a * 10.0)

    print(f"[*] Expected Final Balance      : {expected}")
    print(f"[*] Final Balance on Server A   : {final_a.get('balance')}")
    print(f"[*] Final Balance on Server B   : {final_b.get('balance')}")
    assert abs(final_a.get("balance", -1) - expected) < 1e-6, "Server A balance mismatch!"
    assert abs(final_b.get("balance", -2) - expected) < 1e-6, "Server B did not converge to Server A!"
    print("[SUCCESS] Split-Brain prevented during partition & both servers converged to identical state after recovery!\n")


def main():
    parser = argparse.ArgumentParser(description="Mini Banking Transaction Client")
    parser.add_argument("--host", type=str, required=True, help="Real IP address of Primary Server A")
    parser.add_argument("--port", type=int, default=5000, help="Primary Server TCP port")
    parser.add_argument("--backup-host", type=str, default=None, help="Real IP address of Backup Server B")
    parser.add_argument("--backup-port", type=int, default=5000, help="Backup Server TCP port")
    parser.add_argument("--witness-port", type=int, default=5001, help="Port for local Quorum Witness service")
    parser.add_argument(
        "--mode",
        type=str,
        choices=[
            "interactive", "normal", "errors", "concurrency",
            "atomicity", "limit", "failover", "partition", "all", "single"
        ],
        default="interactive",
        help="Execution mode (default: interactive)",
    )
    parser.add_argument("--action", type=str, help="Action for single mode (balance, deposit, withdraw)")
    parser.add_argument("--account", type=str, default="A1001", help="Account ID for single mode")
    parser.add_argument("--amount", type=float, default=100.0, help="Amount for single mode")
    args = parser.parse_args()

    # Start the lightweight Quorum Witness listener in the background on Client VM
    start_witness_service(args.witness_port)

    endpoint = ClusterEndpoint(args.host, args.port, args.backup_host, args.backup_port)

    if args.mode == "interactive":
        run_interactive_mode(endpoint)
    elif args.mode == "normal":
        run_normal_demo(endpoint)
    elif args.mode == "errors":
        run_error_demo(endpoint)
    elif args.mode == "concurrency":
        run_concurrency_demo(endpoint)
    elif args.mode == "atomicity":
        run_atomicity_demo(endpoint)
    elif args.mode == "limit":
        run_limit_demo(endpoint)
    elif args.mode == "failover":
        run_failover_demo(endpoint)
    elif args.mode == "partition":
        run_partition_demo(endpoint)
    elif args.mode == "single":
        payload = {"action": args.action, "account": args.account}
        if args.action in {"deposit", "withdraw"}:
            payload["amount"] = args.amount
        print(send_request(endpoint, payload))
    elif args.mode == "all":
        run_normal_demo(endpoint)
        run_error_demo(endpoint)
        run_concurrency_demo(endpoint)
        run_atomicity_demo(endpoint)
        run_limit_demo(endpoint)


if __name__ == "__main__":
    main()
