"""
proxy.py — Proxy / User Shell
==============================
Acts as a psql-like shell. Coordinates between remote.py (port 5000)
and client.py (port 5001).

Architecture
------------
Two background tasks read from remote and client continuously and route
messages into asyncio queues:

  _remote_q  — normal responses from remote (QUERY_RESULT, LOCK_GRANT, …)
  _client_q  — normal responses from client (CACHE_HIT, WRITE_ACK, …)

Special unsolicited messages are handled out-of-band:
  RECALL_LOCK (from remote) → _handle_recall_lock() runs as a separate task
  CACHE_INVALIDATE (from remote) → forwarded to client silently
  INSERT_FLUSH_FAILED (from client) → printed to console immediately

A single asyncio.Lock (_op_lock) serialises the command-processing coroutine
and the recall handler so they never interleave their protocol exchanges.

Query Classification
--------------------
Type A  — non-cacheable SELECT → pre-flush dirty tables, then query remote.
Type B  — cacheable SELECT with equality WHERE → local cache or remote + cache.
INSERT  — lazy: apply locally, queue for background sync; return immediately.
Type C  — UPDATE/DELETE: lock rows on remote, apply locally, release lock when
          recalled or when pre-flush is triggered before a Type A query.
"""

import asyncio
import json
import os
import uuid
import getpass

import query as qmod


# ─── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE = "proxy_config.json"
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
else:
    config = {
        "remote_host": "192.168.1.100",
        "remote_port": 5000,
        "client_host": "localhost",
        "client_port": 5001,
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

REMOTE_HOST = config.get("remote_host", "192.168.1.100")
REMOTE_PORT = config.get("remote_port", 5000)
CLIENT_HOST = config.get("client_host", "localhost")
CLIENT_PORT = config.get("client_port", 5001)

CLIENT_ID = f"proxy-{uuid.uuid4().hex[:8]}"

# Session state (set after CONNECT)
_session: dict = {
    "database":     None,
    "user":         None,
    "schema":       {},
    # table → list[str]  — PKs for which this proxy holds a WRITE lock
    "active_locks": {},
}

# ─── Message queues (filled by background reader tasks) ───────────────────────

_remote_q: asyncio.Queue = None   # set in main()
_client_q: asyncio.Queue = None

# Serialises the command-processing coroutine and background insert flush
_op_lock: asyncio.Lock = None

# Dedicated queues used ONLY by _handle_recall_lock (decoupled from command loop)
# This prevents the deadlock where the recall handler and command loop both
# wait on the same queues while competing for _op_lock.
_recall_client_q: asyncio.Queue = None   # FLUSH_ACK / FLUSH_DONE_ACK for recall
_recall_remote_q: asyncio.Queue = None   # SYNC_ACK for recall

# Set to True while a recall is being handled; reader tasks route responses here
_recall_active: bool = False


# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
    data = (json.dumps(msg, default=str) + "\n").encode()
    writer.write(data)
    await writer.drain()


async def recv_msg(reader: asyncio.StreamReader) -> dict | None:
    try:
        line = await reader.readline()
        if not line:
            return None
        return json.loads(line.decode())
    except (json.JSONDecodeError, ConnectionResetError):
        return None


# ─── Display helpers ──────────────────────────────────────────────────────────

def _print_table(rows: list[dict], columns: list[str]):
    if not rows:
        print("(0 rows)")
        return
    cols   = columns or list(rows[0].keys())
    widths = {c: max(len(str(c)), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    sep    = "+" + "+".join("-" * (widths[c] + 2) for c in cols) + "+"
    header = "|" + "|".join(f" {c:<{widths[c]}} " for c in cols) + "|"
    print(sep); print(header); print(sep)
    for row in rows:
        line = "|" + "|".join(f" {str(row.get(c,'')):<{widths[c]}} " for c in cols) + "|"
        print(line)
    print(sep)
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ─── Background reader tasks ──────────────────────────────────────────────────

async def _remote_reader_task(remote_reader: asyncio.StreamReader,
                               remote_writer: asyncio.StreamWriter,
                               client_writer: asyncio.StreamWriter):
    """
    Continuously drain remote_reader.
    RECALL_LOCK  → spawn _handle_recall_lock as a task.
    CACHE_INVALIDATE → forward to client silently.
    SYNC_ACK while recall active → route to _recall_remote_q.
    Everything else → put in _remote_q for the command loop.
    """
    while True:
        msg = await recv_msg(remote_reader)
        if msg is None:
            break
        mtype = msg.get("type", "")
        if mtype == "RECALL_LOCK":
            asyncio.create_task(
                _handle_recall_lock(msg, remote_writer, client_writer)
            )
        elif mtype == "CACHE_INVALIDATE":
            asyncio.create_task(
                _forward_cache_invalidate(msg, client_writer)
            )
        elif mtype == "SYNC_ACK" and _recall_active:
            await _recall_remote_q.put(msg)
        else:
            await _remote_q.put(msg)


async def _client_reader_task(client_reader: asyncio.StreamReader):
    """
    Continuously drain client_reader.
    FLUSH_ACK / FLUSH_DONE_ACK while recall active → route to _recall_client_q.
    Everything else → put in _client_q for the command loop.
    """
    while True:
        msg = await recv_msg(client_reader)
        if msg is None:
            break
        mtype = msg.get("type", "")
        if _recall_active and mtype in ("FLUSH_ACK", "FLUSH_DONE_ACK"):
            await _recall_client_q.put(msg)
        else:
            await _client_q.put(msg)


async def _forward_cache_invalidate(msg: dict, client_writer: asyncio.StreamWriter):
    """Forward CACHE_INVALIDATE to client; consume the ack without blocking callers."""
    try:
        await send_msg(client_writer, msg)
        # Don't consume from any queue — the ack will arrive on _client_q and be
        # silently dropped as an unrecognised message type in the REPL loop.
    except Exception:
        pass


# ─── Background INSERT flush (proxy-driven, every 1 s) ───────────────────────

async def _background_insert_flush(remote_writer: asyncio.StreamWriter,
                                   client_writer: asyncio.StreamWriter):
    """
    Every 1 second, ask client for pending inserts, apply them on remote,
    then confirm to client so it clears them.
    All traffic goes through the existing proxy connections — no new sockets.
    """
    _fail_counts: dict = {}   # database → consecutive failure count

    while True:
        await asyncio.sleep(1.0)
        database = _session.get("database")
        if not database:
            continue

        async with _op_lock:
            await send_msg(client_writer, {
                "type":     "FLUSH_INSERTS",
                "database": database,
            })
            try:
                ack = await asyncio.wait_for(_client_q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                print("[proxy] BG flush: timeout waiting for client")
                continue

            if ack.get("type") == "ERROR":
                print(f"[proxy] BG flush: client error: {ack.get('message')}")
                continue

            insert_changes = ack.get("insert_changes", [])
            if not insert_changes:
                _fail_counts.pop(database, None)
                continue

            await send_msg(remote_writer, {
                "type":     "APPLY_CHANGES",
                "database": database,
                "changes":  insert_changes,
                "source":   "INSERT",
            })
            try:
                apply_ack = await asyncio.wait_for(_remote_q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                print("[proxy] BG flush: timeout waiting for remote")
                _fail_counts[database] = _fail_counts.get(database, 0) + 1
                continue

            if apply_ack.get("type") == "APPLY_ACK":
                await send_msg(client_writer, {
                    "type":     "FLUSH_INSERTS_DONE",
                    "database": database,
                })
                try:
                    await asyncio.wait_for(_client_q.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                _fail_counts.pop(database, None)
                print(f"[proxy] BG flush: {len(insert_changes)} insert(s) → remote '{database}'")
            else:
                _fail_counts[database] = _fail_counts.get(database, 0) + 1
                fails = _fail_counts[database]
                print(f"[proxy] BG flush failed ({fails}x): {apply_ack.get('message', apply_ack)}")


# ─── RECALL_LOCK handler (runs as a background task) ─────────────────────────
# Uses its own dedicated queues (_recall_client_q / _recall_remote_q) so it
# NEVER competes with the command loop for _op_lock or _client_q/_remote_q.
# The reader tasks route FLUSH_ACK, FLUSH_DONE_ACK, and SYNC_ACK here while
# _recall_active is True.

async def _handle_recall_lock(msg: dict, remote_writer: asyncio.StreamWriter,
                               client_writer: asyncio.StreamWriter):
    global _recall_active
    database = msg["database"]
    table    = msg["table"]
    pks      = msg["pks"]
    print(f"\n[proxy] ← RECALL_LOCK: {table} PKs={pks} — flushing & releasing …")

    _recall_active = True
    try:
        # Step 1: ask client for pending type-C changes for this table
        await send_msg(client_writer, {
            "type":     "FLUSH_PENDING",
            "database": database,
            "tables":   [table],
        })
        try:
            flush_ack = await asyncio.wait_for(_recall_client_q.get(), timeout=10.0)
        except asyncio.TimeoutError:
            print("[proxy] RECALL: timeout waiting for FLUSH_ACK from client")
            return
        type_c_changes = flush_ack.get("type_c_changes", [])

        # Step 2: release the lock on remote, sending the pending changes
        await send_msg(remote_writer, {
            "type":            "LOCK_RELEASE",
            "client_id":       CLIENT_ID,
            "database":        database,
            "table":           table,
            "pks":             pks,
            "pending_changes": type_c_changes,
        })
        try:
            sync_ack = await asyncio.wait_for(_recall_remote_q.get(), timeout=10.0)
            if not sync_ack.get("success", True):
                print(f"[proxy] RECALL: remote reported apply failure for '{table}'")
        except asyncio.TimeoutError:
            print("[proxy] RECALL: timeout waiting for SYNC_ACK from remote")
            return

        # Step 3: confirm to client so it can clear its pending_changes & purge rows
        # Pass the recalled PKs so client deletes exactly those rows, not the whole table.
        await send_msg(client_writer, {
            "type":                  "FLUSH_DONE",
            "database":              database,
            "tables":                [table],
            "recalled_pks_by_table": {table: pks},
        })
        try:
            await asyncio.wait_for(_recall_client_q.get(), timeout=5.0)
        except asyncio.TimeoutError:
            pass  # non-fatal

    finally:
        _recall_active = False

    _session["active_locks"].pop(table, None)
    print(f"[proxy] ✓ Lock for '{table}' released to remote requester")


# ─── Phase 1: CONNECT ─────────────────────────────────────────────────────────

async def do_connect(parsed, remote_writer, client_writer):
    database = parsed.params["database"]
    user     = parsed.params["user"]
    password = parsed.params["password"]

    if not password:
        loop = asyncio.get_event_loop()
        password = await loop.run_in_executor(
            None, getpass.getpass, f"Password for user {user}: "
        )

    print(f"\n[proxy] → Sending CONNECT to remote.py …")
    await send_msg(remote_writer, {
        "type":      "CONNECT",
        "client_id": CLIENT_ID,
        "database":  database,
        "user":      user,
        "password":  password,
    })

    print("[proxy] ← Waiting for SCHEMA_TRANSFER …")
    msg = await _remote_q.get()
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    if msg["type"] != "SCHEMA_TRANSFER":
        print(f"[proxy] Unexpected: {msg['type']}"); return

    schema      = msg["schema"]
    permissions = msg["permissions"]
    print(f"[proxy] ✓ Schema received — {len(schema)} table(s): {list(schema.keys())}")

    print("[proxy] → Sending INIT_DB to client.py …")
    await send_msg(client_writer, {
        "type":        "INIT_DB",
        "database":    database,
        "user":        user,
        "schema":      schema,
        "permissions": permissions,
    })
    ack = await _client_q.get()
    if ack["type"] == "ERROR":
        print(f"[proxy] ERROR from client: {ack['message']}"); return

    if ack["type"] == "INIT_DB_ACK" and ack.get("status") == "ok":
        _session["database"] = database
        _session["user"]     = user
        _session["schema"]   = schema
        print(f"\n[proxy] ✅  Connected to '{database}' as '{user}'.")
        print(f"[proxy]    Local replica is ready (schema only, no data yet).\n")
    else:
        print(f"[proxy] Unexpected response from client: {ack}")


# ─── Pre-flush helper (shared by Type A and voluntary lock release) ───────────

async def _pre_flush(database: str, tables: list,
                     remote_writer: asyncio.StreamWriter,
                     client_writer: asyncio.StreamWriter) -> bool:
    """
    Push pending inserts and type-C changes for the given tables to remote.
    Returns True on success (or if there was nothing to flush).
    """
    await send_msg(client_writer, {
        "type":     "FLUSH_PENDING",
        "database": database,
        "tables":   tables,
    })
    flush_ack = await _client_q.get()
    if flush_ack.get("type") == "ERROR":
        print(f"[proxy] Pre-flush error from client: {flush_ack['message']}")
        return False

    insert_changes    = flush_ack.get("insert_changes", [])
    type_c_changes    = flush_ack.get("type_c_changes", [])
    write_pks_by_tbl  = flush_ack.get("write_pks_by_table", {})
    all_changes       = insert_changes + type_c_changes

    if not all_changes:
        return True   # nothing dirty

    print(f"[proxy] Pre-flushing {len(all_changes)} pending change(s) "
          f"({len(insert_changes)} insert, {len(type_c_changes)} type-C) …")

    # Send all changes (inserts + type-C) to remote via APPLY_CHANGES
    await send_msg(remote_writer, {
        "type":     "APPLY_CHANGES",
        "database": database,
        "changes":  all_changes,
        "source":   "PRE_FLUSH",
    })
    apply_ack = await _remote_q.get()
    ok = apply_ack.get("type") == "APPLY_ACK"
    if not ok:
        print(f"[proxy] WARNING: pre-flush apply failed: {apply_ack}")
        return False

    # If type-C changes were included, also release the write locks on remote
    for tbl, pks in write_pks_by_tbl.items():
        await send_msg(remote_writer, {
            "type":            "LOCK_RELEASE",
            "client_id":       CLIENT_ID,
            "database":        database,
            "table":           tbl,
            "pks":             pks,
            "pending_changes": [],   # already applied above via APPLY_CHANGES
        })
        await _remote_q.get()   # consume SYNC_ACK
        _session["active_locks"].pop(tbl, None)

    # Confirm to client: pass the written PKs so it purges only those rows.
    await send_msg(client_writer, {
        "type":                  "FLUSH_DONE",
        "database":              database,
        "tables":                tables,
        "recalled_pks_by_table": write_pks_by_tbl,
    })
    await _client_q.get()   # consume FLUSH_DONE_ACK
    return True


# ─── Phase 2: SELECT Type A — always remote, pre-flush dirty tables ───────────

async def do_select_type_a(parsed, remote_writer, client_writer):
    database = _session["database"]
    table    = parsed.table   # may be "" for complex multi-table queries

    # Determine which tables to pre-flush
    if table:
        tables_to_flush = [table]
    else:
        # No single table detected — flush everything that's dirty
        tables_to_flush = (
            list(_session["active_locks"].keys()) +
            list({e["table"] for e in []})   # pending_inserts unknown here; flush all schema tables
        )
        if not tables_to_flush:
            tables_to_flush = list(_session["schema"].keys())

    print(f"[proxy] Type A → pre-flushing: {tables_to_flush}")
    await _pre_flush(database, tables_to_flush, remote_writer, client_writer)

    print(f"[proxy] Type A query → remote")
    await send_msg(remote_writer, {
        "type":       "QUERY",
        "client_id":  CLIENT_ID,
        "database":   database,
        "query_type": "A",
        "sql":        parsed.raw,
        "table":      table,
    })
    msg = await _remote_q.get()
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    _print_table(msg.get("rows", []), msg.get("columns", []))


# ─── Phase 2: SELECT Type B — cache-aware ─────────────────────────────────────

async def do_select_type_b(parsed, remote_writer, client_writer):
    database    = _session["database"]
    fingerprint = parsed.fingerprint
    sql         = parsed.raw
    table       = parsed.table

    # Flush any pending local changes for this table before checking cache,
    # so the cache and remote are always consistent.
    if table:
        await _pre_flush(database, [table], remote_writer, client_writer)

    await send_msg(client_writer, {
        "type":        "CACHE_CHECK",
        "database":    database,
        "fingerprint": fingerprint,
        "sql":         sql,
    })
    ck = await _client_q.get()
    if ck["type"] == "CACHE_HIT":
        print(f"[proxy] Cache HIT (fp={fingerprint[:8]})")
        _print_table(ck.get("rows", []), ck.get("columns", []))
        return

    print(f"[proxy] Cache MISS → fetching from remote")
    await send_msg(remote_writer, {
        "type":         "QUERY",
        "client_id":    CLIENT_ID,
        "database":     database,
        "query_type":   "B",
        "sql":          sql,
        "table":        parsed.table,
        "where_clause": parsed.where_clause,
        "fingerprint":  fingerprint,
    })
    msg = await _remote_q.get()
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return

    rows    = msg.get("rows", [])
    cols    = msg.get("columns", [])
    pks     = msg.get("pks", [])
    pk_cols = msg.get("pk_cols", [])

    # Fix 3: skip caching if result is too large (> 10,000 rows)
    CACHE_ROW_LIMIT = 10_000
    if len(rows) > CACHE_ROW_LIMIT:
        print(f"[proxy] Result has {len(rows)} rows (> {CACHE_ROW_LIMIT}) — not caching")
        _print_table(rows, cols)
        return

    await send_msg(client_writer, {
        "type":        "CACHE_ROWS",
        "database":    database,
        "table":       parsed.table,
        "rows":        rows,
        "pks":         pks,
        "pk_cols":     pk_cols,
        "lock_type":   "READ",
        "fingerprint": fingerprint,
    })
    ack = await _client_q.get()
    if ack and ack.get("type") == "ERROR":
        print(f"[proxy] Cache store error: {ack['message']}")

    _print_table(rows, cols)


# ─── Phase 2: INSERT — lazy (local first) ────────────────────────────────────

async def do_insert(parsed, client_writer):
    database = _session["database"]
    print(f"[proxy] INSERT → local first (lazy remote sync)")
    await send_msg(client_writer, {
        "type":     "INSERT_LOCAL",
        "database": database,
        "table":    parsed.table,
        "sql":      parsed.raw,
    })
    msg = await _client_q.get()
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    print(f"INSERT {msg.get('rowcount', 0)}")
    print(f"[proxy] (change queued for background sync to remote)")


# ─── Phase 2: UPDATE / DELETE — lock + local apply ────────────────────────────

async def do_write(parsed, remote_writer, client_writer):
    database = _session["database"]
    table    = parsed.table
    schema   = _session["schema"].get(table, {})
    pk_cols  = schema.get("primary_keys", [])

    if not pk_cols:
        print(f"[proxy] ERROR: cannot determine PKs for table '{table}'.")
        return

    # Step 1: resolve matching PKs via a remote SELECT (Type A — no pre-flush here)
    pk_select = f'SELECT {", ".join(pk_cols)} FROM "{table}" WHERE {parsed.where_clause};'
    print(f"[proxy] Resolving PKs: {pk_select}")
    await send_msg(remote_writer, {
        "type":       "QUERY",
        "client_id":  CLIENT_ID,
        "database":   database,
        "query_type": "A",
        "sql":        pk_select,
    })
    pk_msg = await _remote_q.get()
    if pk_msg is None or pk_msg.get("type") == "ERROR":
        print(f"[proxy] ERROR resolving PKs: {pk_msg}"); return

    pk_rows = pk_msg.get("rows", [])
    if not pk_rows:
        print("(0 rows affected)")
        return

    pks = []
    for row in pk_rows:
        if len(pk_cols) == 1:
            pks.append(str(row[pk_cols[0]]))
        else:
            pks.append(json.dumps([str(row[c]) for c in pk_cols]))

    # Step 2: request WRITE lock from remote
    print(f"[proxy] Requesting WRITE lock on {len(pks)} PK(s) …")
    await send_msg(remote_writer, {
        "type":      "LOCK_REQUEST",
        "client_id": CLIENT_ID,
        "database":  database,
        "table":     table,
        "pks":       pks,
    })
    lock_msg = await _remote_q.get()
    if lock_msg is None or lock_msg.get("type") != "LOCK_GRANT":
        print(f"[proxy] Lock not granted: {lock_msg}"); return
    print(f"[proxy] ✓ WRITE lock granted")

    # Track active lock
    _session["active_locks"][table] = pks

    # Step 3: apply write to local DB
    await send_msg(client_writer, {
        "type":     "WRITE_LOCAL",
        "database": database,
        "table":    table,
        "sql":      parsed.raw,
        "pk_cols":  pk_cols,
        "pks":      pks,
    })
    ack = await _client_q.get()
    if ack is None:
        print("[proxy] ERROR: no response from client."); return
    if ack.get("type") == "ERROR":
        print(f"[proxy] ERROR applying write: {ack['message']}"); return

    verb = parsed.command_type.name
    print(f"{verb} {ack.get('rowcount', 0)}")
    print(f"[proxy] (changes held locally; synced to remote on lock release)")


# ─── Phase 2: META — psql backslash commands (→ remote, never cached) ──────────

async def do_meta(parsed, remote_writer):
    database = _session["database"]
    print(f"[proxy] Meta command → remote")
    await send_msg(remote_writer, {
        "type":      "META_QUERY",
        "client_id": CLIENT_ID,
        "database":  database,
        "meta_cmd":  parsed.raw,
    })
    msg = await _remote_q.get()
    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR: {msg['message']}"); return
    _print_table(msg.get("rows", []), msg.get("columns", []))


# ─── Phase 2: QUIT — release locks, clean local DB, disconnect ─────────────────────

async def do_quit(remote_writer: asyncio.StreamWriter,
                  client_writer: asyncio.StreamWriter):
    """
    Clean shutdown:
    1. Release all held write locks to remote (flush changes first).
    2. Tell client to purge all locally fetched data.
    3. Return so the REPL loop can break.
    """
    database = _session.get("database")

    async with _op_lock:
        # Flush + release every active write lock
        for table, pks in list(_session["active_locks"].items()):
            print(f"[proxy] Releasing write lock on '{table}' before quit …")
            await send_msg(client_writer, {
                "type":     "FLUSH_PENDING",
                "database": database,
                "tables":   [table],
            })
            try:
                flush_ack = await asyncio.wait_for(_client_q.get(), timeout=10.0)
            except asyncio.TimeoutError:
                print(f"[proxy] Timeout flushing '{table}' on quit")
                continue
            type_c_changes = flush_ack.get("type_c_changes", [])
            write_pks_by_tbl = flush_ack.get("write_pks_by_table", {})

            await send_msg(remote_writer, {
                "type":            "LOCK_RELEASE",
                "client_id":       CLIENT_ID,
                "database":        database,
                "table":           table,
                "pks":             pks,
                "pending_changes": type_c_changes,
            })
            try:
                await asyncio.wait_for(_remote_q.get(), timeout=10.0)  # SYNC_ACK
            except asyncio.TimeoutError:
                print(f"[proxy] Timeout releasing lock on '{table}' on quit")

            await send_msg(client_writer, {
                "type":                  "FLUSH_DONE",
                "database":              database,
                "tables":                [table],
                "recalled_pks_by_table": {table: pks},
            })
            try:
                await asyncio.wait_for(_client_q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            _session["active_locks"].pop(table, None)

        # Tell client to wipe all locally fetched data for this session
        if database:
            await send_msg(client_writer, {
                "type":     "DISCONNECT",
                "database": database,
            })
            try:
                ack = await asyncio.wait_for(_client_q.get(), timeout=10.0)
                if ack.get("type") == "DISCONNECT_ACK":
                    print(f"[proxy] Local replica for '{database}' cleaned up.")
            except asyncio.TimeoutError:
                print("[proxy] Timeout waiting for DISCONNECT_ACK from client")

    print("[proxy] Goodbye.")

# ─── Bootstrap ────────────────────────────────────────────────────────────────

async def bootstrap(remote_writer, client_writer):
    init_msg = {"type": "INIT", "client_id": CLIENT_ID}

    await send_msg(remote_writer, init_msg)
    remote_ack = await _remote_q.get()
    if remote_ack and remote_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ remote.py handshake OK (client_id={CLIENT_ID})")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from remote: {remote_ack}")

    await send_msg(client_writer, init_msg)
    client_ack = await _client_q.get()
    if client_ack and client_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ client.py handshake OK")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from client: {client_ack}")


# ─── Main REPL ────────────────────────────────────────────────────────────────

async def main():
    global _remote_q, _client_q, _op_lock, _recall_client_q, _recall_remote_q
    _remote_q        = asyncio.Queue()
    _client_q        = asyncio.Queue()
    _op_lock         = asyncio.Lock()
    _recall_client_q = asyncio.Queue()
    _recall_remote_q = asyncio.Queue()

    print(f"[proxy] Connecting to remote.py at {REMOTE_HOST}:{REMOTE_PORT} …")
    remote_reader, remote_writer = await asyncio.open_connection(REMOTE_HOST, REMOTE_PORT)
    print("[proxy] Connected to remote.py")

    print(f"[proxy] Connecting to client.py at {CLIENT_HOST}:{CLIENT_PORT} …")
    client_reader, client_writer = await asyncio.open_connection(CLIENT_HOST, CLIENT_PORT)
    print("[proxy] Connected to client.py")

    # Start background reader tasks
    asyncio.create_task(_remote_reader_task(remote_reader, remote_writer, client_writer))
    asyncio.create_task(_client_reader_task(client_reader))
    asyncio.create_task(_background_insert_flush(remote_writer, client_writer))

    await bootstrap(remote_writer, client_writer)

    print("\n[proxy] Ready. Type SQL-like commands (e.g. CONNECT mydb USER alice;)")
    print("[proxy] Type 'exit' to quit.\n")

    loop = asyncio.get_event_loop()
    while True:
        try:
            raw = await loop.run_in_executor(None, input, "proxy> ")
        except (EOFError, KeyboardInterrupt):
            print("\n[proxy] Exiting.")
            break

        raw = raw.strip()
        if not raw:
            continue
        if raw.lower() in ("exit", "quit", r"\q"):
            await do_quit(remote_writer, client_writer)
            break

        parsed = qmod.parse(raw)

        async with _op_lock:
            if parsed.command_type == qmod.CommandType.CONNECT:
                await do_connect(parsed, remote_writer, client_writer)

            elif parsed.command_type == qmod.CommandType.SELECT:
                if not _session["database"]:
                    print("[proxy] Not connected. Use CONNECT first."); continue
                print(f"[proxy] Classified as {parsed.route_type.name} "
                      f"| table={repr(parsed.table)} | where={repr(parsed.where_clause)}")
                if parsed.route_type == qmod.RouteType.TYPE_B:
                    await do_select_type_b(parsed, remote_writer, client_writer)
                else:
                    await do_select_type_a(parsed, remote_writer, client_writer)

            elif parsed.command_type == qmod.CommandType.INSERT:
                if not _session["database"]:
                    print("[proxy] Not connected."); continue
                await do_insert(parsed, client_writer)

            elif parsed.command_type in (qmod.CommandType.UPDATE, qmod.CommandType.DELETE):
                if not _session["database"]:
                    print("[proxy] Not connected."); continue
                await do_write(parsed, remote_writer, client_writer)

            elif parsed.command_type == qmod.CommandType.META:
                if not _session["database"]:
                    print("[proxy] Not connected."); continue
                await do_meta(parsed, remote_writer)

            elif parsed.command_type == qmod.CommandType.UNKNOWN:
                print(f"[proxy] Unknown command.")

    remote_writer.close()
    client_writer.close()


if __name__ == "__main__":
    asyncio.run(main())
