"""
proxy.py — Proxy / User Shell
==============================
Acts as a psql-like shell. Coordinates between remote.py (port 5000)
and client.py (port 5001).

Phase 1 flow:
  1. On startup: connect to remote.py and client.py, send INIT handshakes
  2. User types: CONNECT <database> USER <user>;
  3. Proxy → remote.py: CONNECT message
  4. remote.py responds with SCHEMA_TRANSFER
  5. Proxy → client.py: INIT_DB message
  6. client.py responds with INIT_DB_ACK
  7. Proxy reports success to user
"""

import asyncio
import json
import os
import uuid
import getpass

import query as qmod   # our query.py


# ─── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE = "proxy_config.json"
if os.path.exists(CONFIG_FILE):
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
else:
    config = {
        "remote_host": "192.168.1.100",  # IP address of the remote laptop
        "remote_port": 5000,
        "client_host": "localhost",
        "client_port": 5001
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)

REMOTE_HOST = config.get("remote_host", "192.168.1.100")
REMOTE_PORT = config.get("remote_port", 5000)
CLIENT_HOST = config.get("client_host", "localhost")
CLIENT_PORT = config.get("client_port", 5001)

CLIENT_ID = f"proxy-{uuid.uuid4().hex[:8]}"


# ─── Message I/O ──────────────────────────────────────────────────────────────

async def send_msg(writer: asyncio.StreamWriter, msg: dict):
    data = (json.dumps(msg) + "\n").encode()
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


# ─── Phase 1 Handlers ─────────────────────────────────────────────────────────

async def do_connect(
    parsed:        qmod.ParsedCommand,
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    database = parsed.params["database"]
    user     = parsed.params["user"]
    password = parsed.params["password"]

    if not password:
        loop = asyncio.get_event_loop()
        password = await loop.run_in_executor(None, getpass.getpass, f"Password for user {user}: ")

    print(f"\n[proxy] → Sending CONNECT to remote.py …")
    await send_msg(remote_writer, {
        "type":      "CONNECT",
        "client_id": CLIENT_ID,
        "database":  database,
        "user":      user,
        "password":  password,
    })

    # ── Wait for SCHEMA_TRANSFER ───────────────────────────────────────────────
    print("[proxy] ← Waiting for SCHEMA_TRANSFER from remote.py …")
    msg = await recv_msg(remote_reader)
    if msg is None:
        print("[proxy] ERROR: remote.py closed connection unexpectedly.")
        return

    if msg["type"] == "ERROR":
        print(f"[proxy] ERROR from remote.py: {msg['message']}")
        return

    if msg["type"] != "SCHEMA_TRANSFER":
        print(f"[proxy] Unexpected message type: {msg['type']}")
        return

    schema      = msg["schema"]
    permissions = msg["permissions"]
    print(f"[proxy] ✓ Schema received — {len(schema)} table(s): {list(schema.keys())}")

    # ── Forward to client.py ───────────────────────────────────────────────────
    print("[proxy] → Sending INIT_DB to client.py …")
    await send_msg(client_writer, {
        "type":        "INIT_DB",
        "database":    database,
        "user":        user,
        "schema":      schema,
        "permissions": permissions,
    })

    # ── Wait for ACK ──────────────────────────────────────────────────────────
    print("[proxy] ← Waiting for INIT_DB_ACK from client.py …")
    ack = await recv_msg(client_reader)
    if ack is None:
        print("[proxy] ERROR: client.py closed connection unexpectedly.")
        return

    if ack["type"] == "ERROR":
        print(f"[proxy] ERROR from client.py: {ack['message']}")
        return

    if ack["type"] == "INIT_DB_ACK" and ack.get("status") == "ok":
        print(f"\n[proxy] ✅  Connected to '{database}' as '{user}'.")
        print(f"[proxy]    Local replica is ready (schema only, no data yet).")
    else:
        print(f"[proxy] Unexpected response from client.py: {ack}")


# ─── Bootstrap ────────────────────────────────────────────────────────────────

async def bootstrap(
    remote_reader: asyncio.StreamReader,
    remote_writer: asyncio.StreamWriter,
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
):
    """Send INIT handshakes to both servers."""
    init_msg = {"type": "INIT", "client_id": CLIENT_ID}

    await send_msg(remote_writer, init_msg)
    remote_ack = await recv_msg(remote_reader)
    if remote_ack and remote_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ remote.py handshake OK (client_id={CLIENT_ID})")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from remote: {remote_ack}")

    await send_msg(client_writer, init_msg)
    client_ack = await recv_msg(client_reader)
    if client_ack and client_ack.get("type") == "INIT_ACK":
        print(f"[proxy] ✓ client.py handshake OK")
    else:
        print(f"[proxy] WARNING: unexpected INIT response from client: {client_ack}")


# ─── Main REPL ────────────────────────────────────────────────────────────────

async def main():
    # Connect to remote.py
    print(f"[proxy] Connecting to remote.py at {REMOTE_HOST}:{REMOTE_PORT} …")
    remote_reader, remote_writer = await asyncio.open_connection(REMOTE_HOST, REMOTE_PORT)
    print("[proxy] Connected to remote.py")

    # Connect to client.py
    print(f"[proxy] Connecting to client.py at {CLIENT_HOST}:{CLIENT_PORT} …")
    client_reader, client_writer = await asyncio.open_connection(CLIENT_HOST, CLIENT_PORT)
    print("[proxy] Connected to client.py")

    # Handshake both
    await bootstrap(remote_reader, remote_writer, client_reader, client_writer)

    print("\n[proxy] Ready. Type SQL-like commands (e.g. CONNECT mydb USER alice;)")
    print("[proxy] Type 'exit' to quit.\n")

    # REPL
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
            print("[proxy] Goodbye.")
            break

        parsed = qmod.parse(raw)

        if parsed.command_type == qmod.CommandType.CONNECT:
            await do_connect(
                parsed,
                remote_reader, remote_writer,
                client_reader, client_writer,
            )

        elif parsed.command_type == qmod.CommandType.UNKNOWN:
            print(f"[proxy] Unknown command. (Phase 2 will handle queries.)")

        else:
            print(f"[proxy] Command type '{parsed.command_type.name}' not yet implemented (Phase 2).")

    remote_writer.close()
    client_writer.close()


if __name__ == "__main__":
    asyncio.run(main())
