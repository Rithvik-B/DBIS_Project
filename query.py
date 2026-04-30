"""
query.py — Query Router
=======================
Parses user-facing SQL-like commands and routes them.
Phase 1 scope: parse the CONNECT command.
Phase 2+ will add: SELECT/INSERT/UPDATE/DELETE routing with cache awareness.
"""

import re
from dataclasses import dataclass
from enum import Enum, auto


# ─── Command Types ────────────────────────────────────────────────────────────

class CommandType(Enum):
    CONNECT  = auto()
    SELECT   = auto()
    INSERT   = auto()
    UPDATE   = auto()
    DELETE   = auto()
    UNKNOWN  = auto()


@dataclass
class ParsedCommand:
    command_type: CommandType
    raw:          str
    params:       dict   # command-specific extracted params


# ─── Parser ───────────────────────────────────────────────────────────────────

# Pattern:  CONNECT <database> USER <user> [PASSWORD <password>];
_CONNECT_RE = re.compile(
    r"^\s*CONNECT\s+(\w+)\s+USER\s+(\w+)(?:\s+PASSWORD\s+(\S+))?\s*;?\s*$",
    re.IGNORECASE,
)


def parse(raw: str) -> ParsedCommand:
    """
    Parse a raw user command string and return a ParsedCommand.

    Supported in Phase 1:
        CONNECT <database> USER <user> [PASSWORD <password>];

    Everything else is returned as UNKNOWN (to be expanded in Phase 2).
    """
    stripped = raw.strip()

    # ── CONNECT ───────────────────────────────────────────────────────────────
    m = _CONNECT_RE.match(stripped)
    if m:
        database, user, password = m.group(1), m.group(2), m.group(3) or ""
        return ParsedCommand(
            command_type = CommandType.CONNECT,
            raw          = raw,
            params       = {
                "database": database,
                "user":     user,
                "password": password,
            },
        )

    # ── SELECT (stub for Phase 2) ─────────────────────────────────────────────
    if stripped.upper().startswith("SELECT"):
        return ParsedCommand(CommandType.SELECT, raw, {})

    # ── INSERT / UPDATE / DELETE (stubs) ──────────────────────────────────────
    if stripped.upper().startswith("INSERT"):
        return ParsedCommand(CommandType.INSERT, raw, {})
    if stripped.upper().startswith("UPDATE"):
        return ParsedCommand(CommandType.UPDATE, raw, {})
    if stripped.upper().startswith("DELETE"):
        return ParsedCommand(CommandType.DELETE, raw, {})

    return ParsedCommand(CommandType.UNKNOWN, raw, {})
