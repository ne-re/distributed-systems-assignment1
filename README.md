# Distributed Banking Transaction Server
**ACID-Compliant, Fault-Tolerant & Network-Partition Resilient Banking System**

This project implements a multi-threaded distributed banking server and client test suite in Python using only the Python 3 Standard Library (`socket`, `threading`, `sqlite3`, `json`, `argparse`). It fulfills the Base Requirements, the Advanced Bonus (Primary-Backup Failover), and the Extreme Bonus (Network Partition & Split-Brain Prevention).

---

## Demo Videos & Technical Report
- **Demo Videos (Google Drive):** https://drive.google.com/drive/folders/1SjGmSgCz2QG5Z2hTkijVriClE1VL4BPc?usp=drive_link
  - `Demo 1`: Base Requirements (Normal Operations, Error Handling, Concurrency, Atomicity, Max Clients Limit, Durability)
  - `Demo 2`: Advanced Bonus (Primary-Backup Synchronous Replication, Automatic Failover, Idempotency)
  - `Demo 3`: Extreme Bonus (Network Partition Simulation, 3-Node Quorum Witness, Split-Brain Prevention, State Reconciliation)
- **Technical Report:** See `report.pdf` in this repository.

---

## Network Topology (3 VirtualBox VMs)
The system is deployed and tested across three isolated Ubuntu Linux virtual machines connected via a VirtualBox Host-Only network (`192.168.56.0/24`):

| Node Role | Hostname | IP Address | Port | Description |
| :--- | :--- | :--- | :--- | :--- |
| **Client & Quorum Witness** | `client-vm` | `192.168.56.101` | `5001` | Runs automated test suites, interactive CLI, and the lightweight Quorum Witness service. |
| **Server A (Primary)** | `server-vm` | `192.168.56.102` | `5000` | Primary banking server handling client transactions and replicating state to Backup. |
| **Server B (Backup)** | `ubuntu-Server-B` | `192.168.56.103` | `5000` | Backup server receiving synchronous state updates and monitoring Primary health. |

---

## Key Architectural Features
1. **Application Protocol:** Raw TCP sockets (`AF_INET`, `SOCK_STREAM`) with Newline-Delimited JSON (NDJSON) message framing and unique request identifiers (`req_id`).
2. **Concurrency & Capacity Control:** Thread-per-connection model bounded by a configurable non-blocking `threading.Semaphore` (`--max-clients`).
3. **ACID Guarantees:**
   - **Atomicity:** Manual transaction boundaries (`isolation_level=None`) with explicit `BEGIN IMMEDIATE`, `COMMIT`, and `ROLLBACK` on mid-transaction errors.
   - **Consistency:** Strict input validation (`NaN`, `Infinity`, negative/zero amounts, unknown actions/accounts) and SQLite `CHECK(balance >= 0)` constraints.
   - **Isolation:** Two-layer locking combining Python's `threading.Lock()` and SQLite's `BEGIN IMMEDIATE` to achieve serializable execution and prevent lost updates.
   - **Durability:** Enforced via `PRAGMA synchronous = FULL;` and `PRAGMA journal_mode = TRUNCATE;` to flush committed writes directly to disk.
4. **Fault Tolerance (Advanced Bonus):** Synchronous state replication prior to commit, heartbeat health monitoring, automatic client/server failover, and `req_id` deduplication (`processed_transactions` table) for **Exactly-Once** execution semantics.
5. **Split-Brain Prevention (Extreme Bonus):** 3-Node Majority Quorum architecture using a Witness node to distinguish true Primary crashes from network partitions, coupled with ordered log catch-up (`seq_id`) upon network recovery.

---

## How to Run

### Prerequisites
- Python 3.8+ (No external packages required; uses Python Standard Library only).
- To reset database state and logs before a fresh test on the servers:
  ```bash
  rm -f bank.db* server.log
  ```

---

### 1. Base Requirements (Single Server & Automated Tests)

**Step 1 — Start Server A (`192.168.56.102`):**
```bash
python3 server.py --host 0.0.0.0 --port 5000 --max-clients 20
```

**Step 2 — Run Automated Tests on Client (`192.168.56.101`):**
```bash
# Run the complete base test suite (Normal, Errors, Concurrency, Atomicity, Limit)
python3 client.py --host 192.168.56.102 --port 5000 --mode all

# Or run individual test modes:
python3 client.py --host 192.168.56.102 --port 5000 --mode normal
python3 client.py --host 192.168.56.102 --port 5000 --mode errors
python3 client.py --host 192.168.56.102 --port 5000 --mode concurrency
python3 client.py --host 192.168.56.102 --port 5000 --mode atomicity
python3 client.py --host 192.168.56.102 --port 5000 --mode limit

# Interactive CLI mode (for manual transactions & Durability restart test)
python3 client.py --host 192.168.56.102 --port 5000 --mode interactive
```

---

### 2. Advanced Bonus: Fault-Tolerant Cluster (Primary-Backup Failover)

**Step 1 — Start Server B as Backup (`192.168.56.103`):**
```bash
rm -f bank.db* server.log
python3 server.py --host 0.0.0.0 --port 5000 --role backup --peer-host 192.168.56.102
```

**Step 2 — Start Server A as Primary (`192.168.56.102`):**
```bash
rm -f bank.db* server.log
python3 server.py --host 0.0.0.0 --port 5000 --role primary --peer-host 192.168.56.103
```

**Step 3 — Run Failover Test on Client (`192.168.56.101`):**
```bash
python3 client.py --host 192.168.56.102 --backup-host 192.168.56.103 --mode failover
```
*(Note: Terminate Server A using `Ctrl+C` around transaction #5 when prompted by the client to observe seamless failover to Server B and idempotent replay verification).*

---

### 3. Extreme Bonus: Network Partition & Split-Brain Prevention

**Step 1 — Start Server B with Quorum Witness (`192.168.56.103`):**
```bash
rm -f bank.db* server.log
python3 server.py --host 0.0.0.0 --port 5000 --role backup --peer-host 192.168.56.102 --witness-host 192.168.56.101
```

**Step 2 — Start Server A with Quorum Witness (`192.168.56.102`):**
```bash
rm -f bank.db* server.log
python3 server.py --host 0.0.0.0 --port 5000 --role primary --peer-host 192.168.56.103 --witness-host 192.168.56.101
```

**Step 3 — Start Partition Test on Client (`192.168.56.101`):**
```bash
python3 client.py --host 192.168.56.102 --backup-host 192.168.56.103 --mode partition
```

**Step 4 — Simulate Network Partition between Server A and Server B:**
While the test is running (around Step 2 or 3), execute the following firewall rules in a second terminal on **Server A (`192.168.56.102`)** to sever direct communication with Server B:
```bash
sudo iptables -A INPUT -s 192.168.56.103 -j REJECT && sudo iptables -A OUTPUT -d 192.168.56.103 -j REJECT
```

**Step 5 — Heal Network Partition & Observe State Reconciliation:**
Around Step 12 or when prompted by the client, flush the firewall rules on **Server A (`192.168.56.102`)** to restore the link:
```bash
sudo iptables -F
```
*(Server B automatically fetches missed transactions from Server A and converges to the exact final balance).*

---

## Project Structure
```text
.
├── server.py          # Banking server, SQLite ACID engine, replication & quorum logic
├── client.py          # Client CLI, automated test suites & Quorum Witness service
├── requirements.txt   # Dependency specification (Python Standard Library)
├── report.pdf         # 5-page technical report
├── README.md          # Documentation and execution guide
└── .gitignore         # Excludes SQLite runtime databases, logs, and cache files
```
