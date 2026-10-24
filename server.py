#!/usr/bin/env python3
"""
Distributed Systems Assignment 1: Mini Banking Transaction Server
Complete Implementation covering:
- Core Requirements (Sections 1-4): TCP Sockets, Configurable Concurrency, ACID SQLite, 6 Error Cases, Logging
- Advanced Bonus (Section 5): Primary-Backup Replication, Heartbeat Failover, Idempotency
- Extreme Bonus (Section 6): Network Partition & Split-Brain Prevention (Quorum/Witness) + Auto-Recovery Sync
"""

import argparse
import datetime
import json
import math
import socket
import sqlite3
import threading
import time
import uuid

db_lock = threading.Lock()


class BankDatabase:
    """Manages persistent account state, ACID transactions, and ordered transaction logs for recovery."""

    def __init__(self, db_path: str = "bank.db"):
        self.db_path = db_path
        self._initialize_database()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.execute("PRAGMA journal_mode = TRUNCATE;")
        conn.execute("PRAGMA synchronous = FULL;")
        return conn

    def _initialize_database(self) -> None:
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id TEXT PRIMARY KEY,
                        balance REAL NOT NULL CHECK(balance >= 0)
                    );
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_transactions (
                        seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        req_id TEXT UNIQUE NOT NULL,
                        action TEXT NOT NULL,
                        account_id TEXT NOT NULL,
                        amount REAL NOT NULL,
                        new_balance REAL NOT NULL,
                        response_json TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );
                    """
                )
                initial_accounts = [
                    ("A1001", 1000.0),
                    ("A1002", 500.0),
                    ("A1003", 2500.0),
                ]
                conn.executemany(
                    "INSERT OR IGNORE INTO accounts (account_id, balance) VALUES (?, ?);",
                    initial_accounts,
                )
                conn.execute("COMMIT;")
            except Exception:
                conn.execute("ROLLBACK;")
                raise
            finally:
                conn.close()

    def get_all_transactions(self) -> list[dict]:
        """Returns ordered list of all committed transactions for partition recovery synchronization."""
        with db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT seq_id, req_id, action, account_id, amount, new_balance, response_json
                    FROM processed_transactions ORDER BY seq_id ASC;
                    """
                )
                rows = cursor.fetchall()
                return [
                    {
                        "seq_id": r[0],
                        "req_id": r[1],
                        "action": r[2],
                        "account": r[3],
                        "amount": r[4],
                        "new_balance": r[5],
                        "response": json.loads(r[6]),
                    }
                    for r in rows
                ]
            finally:
                conn.close()

    def get_all_balances(self) -> dict[str, float]:
        """Returns a dictionary of all account balances."""
        with db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT account_id, balance FROM accounts ORDER BY account_id;")
                return {r[0]: float(r[1]) for r in cursor.fetchall()}
            finally:
                conn.close()

    def apply_replicated_state(
        self, req_id: str, action: str, account_id: str, amount: float, new_balance: float, response: dict
    ) -> bool:
        """Applies a replicated or recovered transaction onto the local SQLite database."""
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                cursor = conn.execute(
                    "SELECT 1 FROM processed_transactions WHERE req_id = ?;", (req_id,)
                )
                if cursor.fetchone() is not None:
                    conn.execute("COMMIT;")
                    return False  # Already applied

                conn.execute(
                    "UPDATE accounts SET balance = ? WHERE account_id = ?;",
                    (new_balance, account_id),
                )
                now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO processed_transactions
                    (req_id, action, account_id, amount, new_balance, response_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (req_id, action, account_id, amount, new_balance, json.dumps(response), now_str),
                )
                conn.execute("COMMIT;")
                return True
            except Exception:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                return False
            finally:
                conn.close()

    def execute_transaction(
        self,
        req_id: str,
        action: str,
        account_id: str,
        amount: float = 0.0,
        simulate_fail: bool = False,
        replicate_callback=None,
    ) -> dict:
        """Executes an ACID transaction with idempotency and optional synchronous replication."""
        with db_lock:
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")

                if action in {"deposit", "withdraw"}:
                    tx_cursor = conn.execute(
                        "SELECT response_json FROM processed_transactions WHERE req_id = ?;",
                        (req_id,),
                    )
                    tx_row = tx_cursor.fetchone()
                    if tx_row is not None:
                        conn.execute("COMMIT;")
                        cached_resp = json.loads(tx_row[0])
                        cached_resp["idempotent_replay"] = True
                        return cached_resp

                cursor = conn.execute(
                    "SELECT balance FROM accounts WHERE account_id = ?;", (account_id,)
                )
                row = cursor.fetchone()

                if row is None:
                    conn.execute("ROLLBACK;")
                    return {
                        "status": "error",
                        "error": f"Unknown account: '{account_id}' does not exist",
                    }

                current_balance = float(row[0])

                if action == "balance":
                    conn.execute("COMMIT;")
                    return {
                        "status": "success",
                        "account": account_id,
                        "balance": current_balance,
                    }

                elif action in {"deposit", "withdraw"}:
                    if action == "withdraw" and current_balance < amount:
                        conn.execute("ROLLBACK;")
                        return {
                            "status": "error",
                            "error": f"Insufficient funds: cannot withdraw {amount} from balance {current_balance}",
                        }

                    new_balance = (
                        round(current_balance + amount, 4)
                        if action == "deposit"
                        else round(current_balance - amount, 4)
                    )

                    conn.execute(
                        "UPDATE accounts SET balance = ? WHERE account_id = ?;",
                        (new_balance, account_id),
                    )

                    if simulate_fail:
                        raise RuntimeError("Simulated mid-transaction failure before commit")

                    response = {
                        "status": "success",
                        "account": account_id,
                        "balance": new_balance,
                    }

                    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    conn.execute(
                        """
                        INSERT INTO processed_transactions
                        (req_id, action, account_id, amount, new_balance, response_json, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?);
                        """,
                        (req_id, action, account_id, amount, new_balance, json.dumps(response), now_str),
                    )

                    if replicate_callback is not None:
                        replicate_callback(req_id, action, account_id, amount, new_balance, response)

                    conn.execute("COMMIT;")
                    return response

                else:
                    conn.execute("ROLLBACK;")
                    return {
                        "status": "error",
                        "error": f"Unsupported action: '{action}'",
                    }

            except Exception as exc:
                try:
                    conn.execute("ROLLBACK;")
                except Exception:
                    pass
                return {
                    "status": "error",
                    "error": f"Transaction aborted and rolled back: {str(exc)}",
                }
            finally:
                conn.close()


class BankingServer:
    """Fault-Tolerant & Split-Brain Resistant TCP Banking Server."""

    SUPPORTED_ACTIONS = {"balance", "deposit", "withdraw"}

    def __init__(
        self,
        host: str,
        port: int,
        max_clients: int,
        db_path: str,
        log_file: str,
        role: str = "primary",
        peer_host: str | None = None,
        peer_port: int = 5000,
        witness_host: str | None = None,
        witness_port: int = 5001,
    ):
        self.host = host
        self.port = port
        self.max_clients = max_clients
        self.db = BankDatabase(db_path)
        self.log_file = log_file
        self.log_lock = threading.Lock()
        self.client_semaphore = threading.Semaphore(max_clients)

        self.role = role.lower()
        self.role_lock = threading.Lock()
        self.peer_host = peer_host
        self.peer_port = peer_port
        self.witness_host = witness_host
        self.witness_port = witness_port
        self.peer_reachable = True

    def get_role(self) -> str:
        with self.role_lock:
            return self.role

    def set_role(self, new_role: str, reason: str = "") -> None:
        with self.role_lock:
            if self.role != new_role:
                old_role = self.role
                self.role = new_role
                print(
                    f"\n[CLUSTER ROLE CHANGE] {old_role.upper()} -> {new_role.upper()} ({reason})",
                    flush=True,
                )

    def write_log(
        self, req_id: str, client_ip: str, action: str, account: str, result: str, latency_ms: float, extra_info: str = ""
    ) -> None:
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        log_line = (
            f"{timestamp} req={req_id} ip={client_ip} role={self.get_role()} action={action} "
            f"account={account} result={result} {extra_info}latency={latency_ms:.2f}ms"
        )
        with self.log_lock:
            print(log_line, flush=True)
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(log_line + "\n")

    def _rpc_call(self, host: str, port: int, payload: dict, timeout: float = 1.2) -> dict | None:
        """Helper to send an internal JSON message to Peer or Witness."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((host, port))
                sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))
                data = b""
                while not data.endswith(b"\n"):
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                if data:
                    return json.loads(data.decode("utf-8").strip())
        except Exception:
            return None
        return None

    def check_quorum_for_primary(self) -> bool:
        """
        Split-Brain Prevention (Section 6):
        In a cluster mode, Primary can only accept writes if it can reach at least one other node
        (either the Peer server OR the Witness node) to guarantee majority quorum (2 of 3).
        """
        if not self.witness_host:
            return True  # Standalone mode

        if self.peer_host:
            resp = self._rpc_call(self.peer_host, self.peer_port, {"internal_cmd": "heartbeat"}, timeout=1.0)
            if resp and resp.get("status") == "alive":
                return True

        if self.witness_host:
            resp = self._rpc_call(self.witness_host, self.witness_port, {"internal_cmd": "witness_ping"}, timeout=1.0)
            if resp and resp.get("status") == "alive":
                return True

        return False

    def can_backup_promote(self) -> bool:
        """
        Checks if Backup is allowed to promote to Primary when Peer is unreachable.
        If Witness is configured, asks Witness whether the Primary is truly dead (Crash)
        or still alive (Network Partition between Server A and Server B).
        """
        if not self.witness_host:
            return True  # No witness configured; standard Section 5 failover

        resp = self._rpc_call(
            self.witness_host,
            self.witness_port,
            {"internal_cmd": "can_promote", "primary_host": self.peer_host, "primary_port": self.peer_port},
            timeout=1.5,
        )
        if resp is None:
            return False  # Isolated backup cannot reach witness either -> no quorum
        return bool(resp.get("allow_promote", False))

    def replicate_to_backup(
        self, req_id: str, action: str, account_id: str, amount: float, new_balance: float, response: dict
    ) -> None:
        if not self.peer_host or self.get_role() != "primary":
            return

        rep_msg = {
            "internal_cmd": "replicate",
            "req_id": req_id,
            "action": action,
            "account": account_id,
            "amount": amount,
            "new_balance": new_balance,
            "response": response,
        }
        resp = self._rpc_call(self.peer_host, self.peer_port, rep_msg, timeout=1.2)
        if not resp or resp.get("status") != "replicated":
            if self.peer_reachable:
                print(
                    f"[PARTITION / LINK DOWN] Cannot reach peer {self.peer_host}:{self.peer_port} for req={req_id}. "
                    f"Operating via Quorum Witness until link recovers.",
                    flush=True,
                )
                self.peer_reachable = False

    def sync_from_primary(self) -> None:
        """Pulls missing transactions from Primary after network partition heals."""
        if not self.peer_host:
            return
        resp = self._rpc_call(self.peer_host, self.peer_port, {"internal_cmd": "fetch_log"}, timeout=3.0)
        if not resp or resp.get("status") != "ok":
            return

        tx_list = resp.get("transactions", [])
        applied_count = 0
        for tx in tx_list:
            if self.db.apply_replicated_state(
                tx["req_id"], tx["action"], tx["account"], tx["amount"], tx["new_balance"], tx["response"]
            ):
                applied_count += 1

        if applied_count > 0:
            print(
                f"\n[PARTITION RECOVERY] Network link restored! "
                f"Synchronized {applied_count} missed transaction(s) from Primary. "
                f"Current Balances: {self.db.get_all_balances()}\n",
                flush=True,
            )

    def cluster_monitor_loop(self) -> None:
        """
        Continuous background thread handling:
        1. Backup monitoring Primary health (Failover vs Split-Brain Partition detection)
        2. Automatic state reconciliation (Catch-up Sync) when a partitioned link heals
        """
        if not self.peer_host:
            return

        missed_heartbeats = 0
        was_partitioned = False

        while True:
            time.sleep(1.0)
            resp = self._rpc_call(self.peer_host, self.peer_port, {"internal_cmd": "heartbeat"}, timeout=1.2)

            if resp and resp.get("status") == "alive":
                peer_role = resp.get("role", "unknown")
                if not self.peer_reachable or was_partitioned:
                    print(f"\n[NETWORK RESTORED] Link to peer {self.peer_host}:{self.peer_port} is back UP!", flush=True)
                    self.peer_reachable = True
                    was_partitioned = False
                    # If peer is Primary and we are Backup, pull all missed transactions
                    if self.get_role() == "backup" and peer_role == "primary":
                        self.sync_from_primary()

                missed_heartbeats = 0
                if self.get_role() == "backup" and peer_role == "primary":
                    # Periodically ensure backup is in sync
                    self.sync_from_primary()
            else:
                missed_heartbeats += 1
                self.peer_reachable = False
                if missed_heartbeats >= 2 and self.get_role() == "backup":
                    if self.can_backup_promote():
                        self.set_role(
                            "primary",
                            "Primary confirmed unreachable by Witness! Promoting Backup to PRIMARY",
                        )
                    else:
                        if not was_partitioned:
                            print(
                                "\n[SPLIT-BRAIN PREVENTION] Peer link is DOWN, but Witness reports Primary is STILL ALIVE! "
                                "Refusing to promote to Primary to prevent Split-Brain data divergence.",
                                flush=True,
                            )
                        was_partitioned = True

    def validate_and_process(self, raw_payload: str, client_ip: str) -> dict:
        start_time = time.perf_counter()
        req_id = uuid.uuid4().hex[:6]
        action = "unknown"
        account = "unknown"

        try:
            request = json.loads(raw_payload)
            if not isinstance(request, dict):
                raise ValueError("JSON payload must be an object")
        except Exception:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            response = {"status": "error", "error": "Malformed request: invalid JSON payload"}
            self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="malformed_request" ')
            return response

        # Internal Cluster Commands
        internal_cmd = request.get("internal_cmd")
        if internal_cmd == "heartbeat":
            return {"status": "alive", "role": self.get_role(), "balances": self.db.get_all_balances()}

        if internal_cmd == "fetch_log":
            return {"status": "ok", "transactions": self.db.get_all_transactions()}

        if internal_cmd == "replicate":
            rep_req_id = str(request.get("req_id", req_id))
            rep_action = str(request.get("action", "unknown"))
            rep_account = str(request.get("account", "unknown"))
            rep_amount = float(request.get("amount", 0.0))
            rep_balance = float(request.get("new_balance", 0.0))
            rep_resp = request.get("response", {})

            self.db.apply_replicated_state(rep_req_id, rep_action, rep_account, rep_amount, rep_balance, rep_resp)
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            self.write_log(
                rep_req_id, client_ip, f"replicate_{rep_action}", rep_account, "success", latency_ms,
                f"balance={rep_balance} "
            )
            return {"status": "replicated"}

        if "req_id" in request and isinstance(request["req_id"], str) and request["req_id"].strip():
            req_id = request["req_id"].strip()

        action = str(request.get("action", "missing"))
        account = str(request.get("account", "missing"))

        # Split-Brain Protection on Backup Node:
        # If this node is still BACKUP, check if it can promote; if not, reject write requests!
        if self.get_role() == "backup":
            if not self.peer_reachable and self.can_backup_promote():
                self.set_role("primary", "Client request triggered failover check -> Promoted to PRIMARY")
            elif action in {"deposit", "withdraw"}:
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                response = {
                    "status": "error",
                    "error": "Split-brain prevention: This node is BACKUP and Primary is still active in another partition",
                }
                self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="split_brain_prevention_backup_reject" ')
                return response

        # Split-Brain Protection on Primary Node (Quorum check):
        if self.get_role() == "primary" and action in {"deposit", "withdraw"}:
            if not self.check_quorum_for_primary():
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                response = {
                    "status": "error",
                    "error": "Split-brain prevention: Primary is isolated from quorum and cannot safely commit writes",
                }
                self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="quorum_lost" ')
                return response

        # Standard Validation (Section 2.3)
        if "action" not in request or "account" not in request:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            response = {"status": "error", "error": "Missing fields: 'action' and 'account' are required"}
            self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="missing_fields" ')
            return response

        if not isinstance(request["action"], str) or not isinstance(request["account"], str) or not request["account"].strip():
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            response = {"status": "error", "error": "Invalid field types for 'action' or 'account'"}
            self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="invalid_fields" ')
            return response

        if action not in self.SUPPORTED_ACTIONS:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            response = {"status": "error", "error": f"Unsupported action: '{action}' is not implemented"}
            self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="unsupported_action" ')
            return response

        amount = 0.0
        if action in {"deposit", "withdraw"}:
            if "amount" not in request:
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                response = {"status": "error", "error": f"Missing fields: 'amount' is required for {action}"}
                self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="missing_amount" ')
                return response

            raw_amount = request["amount"]
            if isinstance(raw_amount, bool) or not isinstance(raw_amount, (int, float)) or not math.isfinite(float(raw_amount)):
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                response = {"status": "error", "error": f"Negative or invalid amount: '{raw_amount}' must be a valid positive number"}
                self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="invalid_amount_type" ')
                return response

            amount = float(raw_amount)
            if amount <= 0:
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                response = {"status": "error", "error": f"Negative or invalid amount: {amount} must be greater than 0"}
                self.write_log(req_id, client_ip, action, account, "error", latency_ms, 'reason="non_positive_amount" ')
                return response

        hold_ms = request.get("hold_ms", 0)
        if isinstance(hold_ms, (int, float)) and 0 < hold_ms <= 5000:
            time.sleep(hold_ms / 1000.0)

        simulate_fail = bool(request.get("simulate_fail", False))
        response = self.db.execute_transaction(
            req_id=req_id,
            action=action,
            account_id=account,
            amount=amount,
            simulate_fail=simulate_fail,
            replicate_callback=self.replicate_to_backup,
        )
        latency_ms = (time.perf_counter() - start_time) * 1000.0

        if response["status"] == "success":
            replay_tag = "idempotent_replay=true " if response.get("idempotent_replay") else ""
            extra = f"balance={response['balance']} {replay_tag}"
            self.write_log(req_id, client_ip, action, account, "success", latency_ms, extra)
        else:
            self.write_log(req_id, client_ip, action, account, "error", latency_ms, f'reason="{response["error"]}" ')

        return response

    def handle_client(self, conn: socket.socket, addr: tuple) -> None:
        client_ip = addr[0]
        try:
            conn.settimeout(30.0)
            buffer = ""
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8", errors="replace")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    response = self.validate_and_process(line, client_ip)
                    conn.sendall((json.dumps(response) + "\n").encode("utf-8"))
        except socket.timeout:
            pass
        except Exception as exc:
            print(f"[ERROR] Connection error with {client_ip}: {exc}", flush=True)
        finally:
            conn.close()
            self.client_semaphore.release()

    def start(self) -> None:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(128)

        print(
            f"[*] Mini Banking Server [{self.get_role().upper()}] listening on {self.host}:{self.port} "
            f"(Max clients: {self.max_clients}, Peer: {self.peer_host or 'None'}, Witness: {self.witness_host or 'None'})",
            flush=True,
        )

        if self.peer_host:
            mon_thread = threading.Thread(target=self.cluster_monitor_loop, daemon=True)
            mon_thread.start()

        try:
            while True:
                conn, addr = server_sock.accept()
                if not self.client_semaphore.acquire(blocking=False):
                    req_id = uuid.uuid4().hex[:6]
                    self.write_log(
                        req_id, addr[0], "connect", "N/A", "error", 0.0,
                        'reason="max_simultaneous_clients_reached" '
                    )
                    busy_resp = {
                        "status": "error",
                        "error": f"Server busy: maximum simultaneous clients ({self.max_clients}) reached",
                    }
                    try:
                        conn.sendall((json.dumps(busy_resp) + "\n").encode("utf-8"))
                    except Exception:
                        pass
                    conn.close()
                    continue

                client_thread = threading.Thread(
                    target=self.handle_client, args=(conn, addr), daemon=True
                )
                client_thread.start()
        except KeyboardInterrupt:
            print("\n[*] Shutting down server gracefully...", flush=True)
        finally:
            server_sock.close()


def main():
    parser = argparse.ArgumentParser(description="Mini Banking Transaction Server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind to")
    parser.add_argument("--port", type=int, default=5000, help="TCP port to listen on")
    parser.add_argument("--max-clients", type=int, default=20, help="Maximum simultaneous clients")
    parser.add_argument("--db", type=str, default="bank.db", help="Path to SQLite database file")
    parser.add_argument("--log", type=str, default="server.log", help="Path to log file")
    parser.add_argument("--role", type=str, choices=["primary", "backup"], default="primary", help="Cluster role")
    parser.add_argument("--peer-host", type=str, default=None, help="IP address of the peer server")
    parser.add_argument("--peer-port", type=int, default=5000, help="TCP port of the peer server")
    parser.add_argument("--witness-host", type=str, default=None, help="IP address of the Quorum Witness node")
    parser.add_argument("--witness-port", type=int, default=5001, help="TCP port of the Quorum Witness node")
    args = parser.parse_args()

    server = BankingServer(
        host=args.host,
        port=args.port,
        max_clients=args.max_clients,
        db_path=args.db,
        log_file=args.log,
        role=args.role,
        peer_host=args.peer_host,
        peer_port=args.peer_port,
        witness_host=args.witness_host,
        witness_port=args.witness_port,
    )
    server.start()


if __name__ == "__main__":
    main()
