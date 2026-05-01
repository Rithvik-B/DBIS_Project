PSQL_META_COMMANDS = {
    r"\dt", r"\d", r"\l", r"\c", r"\du", r"\dn",
    r"\df", r"\dv", r"\di", r"\ds", r"\copy",
    r"\q", r"\?", r"\h"
}

def is_valid_psql_meta(cmd):
    cmd = cmd.strip().split()[0]  # ignore args
    return cmd in PSQL_META_COMMANDS