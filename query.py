"""
query.py — Query Router / Classifier
=====================================
Parses user-facing SQL commands and classifies them into routing types.

Type A — Non-cacheable: aggregates, no-WHERE, JOINs, subqueries, LIMIT/OFFSET,
          GROUP BY, ORDER BY without equality WHERE. Route direct to remote,
          BUT first pre-flush any dirty local state for the referenced table(s).
Type B — Cacheable read: single-table SELECT with at least one equality WHERE
          condition, no aggregates. Cache result set locally under a READ lock.
Type C — Write: UPDATE/DELETE (row-lock based, applied locally then synced).
INSERT  — Lazy write: applied to local DB immediately, synced to remote in
          background (every ~1 s) or before the next Type A on the same table.
"""

import re
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum, auto


# ─── Command / Route Types ────────────────────────────────────────────────────

class CommandType(Enum):
    CONNECT  = auto()
    SELECT   = auto()
    INSERT   = auto()
    UPDATE   = auto()
    DELETE   = auto()
    UNKNOWN  = auto()

class RouteType(Enum):
    TYPE_A   = auto()   # non-cacheable → remote (after pre-flush)
    TYPE_B   = auto()   # cacheable read → local if cached, else remote + cache
    TYPE_C   = auto()   # write → lock + local apply + deferred sync on lock release
    INSERT   = auto()   # lazy write → local first, background sync to remote
    CONNECT  = auto()
    UNKNOWN  = auto()


@dataclass
class ParsedCommand:
    command_type: CommandType
    route_type:   RouteType
    raw:          str
    params:       dict = field(default_factory=dict)
    # For Type B / Type C / INSERT:
    table:        str  = ""          # single table name (also filled for Type A when detectable)
    where_clause: str  = ""          # raw WHERE text (everything after WHERE)
    fingerprint:  str  = ""          # stable hash for cache lookup
    pk_cols:      list = field(default_factory=list)


# ─── Regex helpers ────────────────────────────────────────────────────────────

_CONNECT_RE = re.compile(
    r"^\s*CONNECT\s+(\w+)\s+USER\s+(\w+)(?:\s+PASSWORD\s+(\S+))?\s*;?\s*$",
    re.IGNORECASE,
)

# Tokens that make a SELECT non-cacheable
_AGGREGATE_RE  = re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', re.IGNORECASE)
_JOIN_RE       = re.compile(r'\bJOIN\b', re.IGNORECASE)
_SUBQUERY_RE   = re.compile(r'\bSELECT\b.+\bSELECT\b', re.IGNORECASE | re.DOTALL)
_LIMIT_RE      = re.compile(r'\bLIMIT\b', re.IGNORECASE)
_GROUP_BY_RE   = re.compile(r'\bGROUP\s+BY\b', re.IGNORECASE)
_ORDER_BY_RE   = re.compile(r'\bORDER\s+BY\b', re.IGNORECASE)
_WHERE_EQ_RE   = re.compile(r'\bWHERE\b.+\b\w+\s*=\s*[^\s<>!]', re.IGNORECASE | re.DOTALL)

# Identifier patterns
_IDENT     = r'(?:"[^"]*"|\w+)'
_TABLE_PAT = r'(?:' + _IDENT + r'\.)?(' + _IDENT + r')'

# SELECT ... FROM <table> [WHERE ...]
_SELECT_FROM_RE = re.compile(
    r'^\s*SELECT\s+.+?\s+FROM\s+' + _TABLE_PAT + r'\s*(?:WHERE\s+(.+?))?\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)

# UPDATE <table> SET ... WHERE ...
_UPDATE_RE = re.compile(
    r'^\s*UPDATE\s+' + _TABLE_PAT + r'\s+SET\s+.+?\s+WHERE\s+(.+?)\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)

# DELETE FROM <table> WHERE ...
_DELETE_RE = re.compile(
    r'^\s*DELETE\s+FROM\s+' + _TABLE_PAT + r'\s+WHERE\s+(.+?)\s*;?\s*$',
    re.IGNORECASE | re.DOTALL,
)

# INSERT INTO <table> ...
_INSERT_RE = re.compile(
    r'^\s*INSERT\s+INTO\s+' + _TABLE_PAT + r'(?=\s|\(|;|$)',
    re.IGNORECASE,
)


def _unquote(name: str) -> str:
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1]
    return name


def _fingerprint(table: str, where: str) -> str:
    key = json.dumps([table.lower(), where.strip().lower()])
    return hashlib.sha1(key.encode()).hexdigest()[:16]


def _is_type_a(sql: str) -> bool:
    if _AGGREGATE_RE.search(sql):
        return True
    if _JOIN_RE.search(sql):
        return True
    if _SUBQUERY_RE.search(sql):
        return True
    if _LIMIT_RE.search(sql):
        return True
    if _GROUP_BY_RE.search(sql):
        return True
    if _ORDER_BY_RE.search(sql) and not _WHERE_EQ_RE.search(sql):
        return True
    return False


# ─── Public parse() ──────────────────────────────────────────────────────────

def parse(raw: str) -> ParsedCommand:
    stripped = raw.strip()
    upper    = stripped.upper()

    # ── CONNECT ───────────────────────────────────────────────────────────────
    m = _CONNECT_RE.match(stripped)
    if m:
        database, user, password = m.group(1), m.group(2), m.group(3) or ""
        return ParsedCommand(
            command_type = CommandType.CONNECT,
            route_type   = RouteType.CONNECT,
            raw          = raw,
            params       = {"database": database, "user": user, "password": password},
        )

    # ── SELECT ────────────────────────────────────────────────────────────────
    if upper.startswith("SELECT"):
        if _is_type_a(stripped):
            # Still try to extract the table so proxy can pre-flush the right rows
            m = _SELECT_FROM_RE.match(stripped)
            table = _unquote(m.group(1)) if m else ""
            return ParsedCommand(
                command_type = CommandType.SELECT,
                route_type   = RouteType.TYPE_A,
                raw          = raw,
                table        = table,
            )
        # Try to parse as Type B
        m = _SELECT_FROM_RE.match(stripped)
        if m:
            table      = _unquote(m.group(1))
            where_text = (m.group(2) or "").strip()
            if where_text and _WHERE_EQ_RE.search("WHERE " + where_text):
                return ParsedCommand(
                    command_type = CommandType.SELECT,
                    route_type   = RouteType.TYPE_B,
                    raw          = raw,
                    table        = table,
                    where_clause = where_text,
                    fingerprint  = _fingerprint(table, where_text),
                )
        # No WHERE or no equality → Type A (with table extracted if possible)
        table = _unquote(m.group(1)) if m else ""
        return ParsedCommand(
            command_type = CommandType.SELECT,
            route_type   = RouteType.TYPE_A,
            raw          = raw,
            table        = table,
        )

    # ── INSERT — lazy (local first, background sync) ─────────────────────────
    if upper.startswith("INSERT"):
        m = _INSERT_RE.match(stripped)
        table = _unquote(m.group(1)) if m else ""
        return ParsedCommand(
            command_type = CommandType.INSERT,
            route_type   = RouteType.INSERT,
            raw          = raw,
            table        = table,
        )

    # ── UPDATE — Type C ───────────────────────────────────────────────────────
    if upper.startswith("UPDATE"):
        m = _UPDATE_RE.match(stripped)
        if m:
            table, where_text = _unquote(m.group(1)), m.group(2).strip()
            return ParsedCommand(
                command_type = CommandType.UPDATE,
                route_type   = RouteType.TYPE_C,
                raw          = raw,
                table        = table,
                where_clause = where_text,
                fingerprint  = _fingerprint(table, where_text),
            )
        return ParsedCommand(CommandType.UPDATE, RouteType.TYPE_C, raw)

    # ── DELETE — Type C ───────────────────────────────────────────────────────
    if upper.startswith("DELETE"):
        m = _DELETE_RE.match(stripped)
        if m:
            table, where_text = _unquote(m.group(1)), m.group(2).strip()
            return ParsedCommand(
                command_type = CommandType.DELETE,
                route_type   = RouteType.TYPE_C,
                raw          = raw,
                table        = table,
                where_clause = where_text,
                fingerprint  = _fingerprint(table, where_text),
            )
        return ParsedCommand(CommandType.DELETE, RouteType.TYPE_C, raw)

    return ParsedCommand(CommandType.UNKNOWN, RouteType.UNKNOWN, raw)
