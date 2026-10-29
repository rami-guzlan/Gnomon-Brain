"""The heartbeat: the one scheduled loop of Release 1 (the auto default, section 4).

Every few minutes (5, 15, 30 or 60; quiet hours respected) the governing CLI reads, read-only,
what changed in each connector since its cursor, then judges each new item in one model call:
which region of the Brain it belongs to, which project, and what it updates (a sheet cell, a
project's progress, a task, a note), each with its reason. Code checks every write before it
is made (in scope, not into a sealed project, not a private item, not over a must-hold) and
makes it through the Brain's one proposal path, with the item as its source: governance says
whether it goes in at once with an undo point or waits for the owner. Then the beat's record:
what it read and wrote, what waits, what it held back, and what it used.

A beat never sends, posts, replies or spends: it can only start the CLI with the read-only
tools Gnomon listed itself (connectors.py), and it calls no orchestrator.

Regions are the Brain's areas. Each keeps a headline (written by the same model call, only
when a line changed) and the lines the beats wrote, newest first. Undoing a line undoes its
write the way any undo does, and records that the next beat must not write it again.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa

from workspace_core.clock import iso, parse_iso
from workspace_core.db import schema
from workspace_core.domain import states
from workspace_core.domain.policy import Principal
from workspace_core.domain.schedule import zone
from workspace_core.errors import (
    Conflict,
    NotFound,
    PermissionDenied,
    ValidationFailed,
    WorkspaceError,
)
from workspace_core.ids import new_id
from workspace_core.services.orchestrators import gate_kind
from workspace_core.workspace import Workspace

from ..runners import RUNNERS, Runners, mask, usage_of
from ..scout import Scout
from . import connectors as conn
from . import learning, memory, n8n
from . import n8n_templates as templates

log = logging.getLogger(__name__)
CLI_CADENCE = 1440  # minutes between reads of a new connector that reads through a CLI: daily
FIRST_LOOK = "the last 7 days"  # how far back a connector's first read looks
ACCOUNT_TTL = 1800.0  # seconds a listing of the Claude account's connectors is kept
ACCOUNT_RECHECKS = 2  # fresh listings for an account connector still connecting
ACCOUNT_PLACE = "Your Claude account"

CADENCES = (5, 15, 30, 60)
OWN_CADENCES = (
    *CADENCES,
    240,
    1440,
)  # a connector may read less often: 4 hours, daily (default)
QUIET_GAP = 2880  # minutes: a connector with nothing new for long is still read this often
MEMORY_CADENCE = 1440  # minutes between memory reads after onboarding: daily
MEMORY_ITEMS = 40  # context packets one memory read may bring
ASK_CHARS = 1000  # characters of what the owner asks a read to look for
SKIP_WORDS = 20  # what a read leaves out: at most this many words or phrases
QUIET = {"off": "Off", "night": "7pm to 7am", "nights_weekends": "Nights and weekends"}
DEFAULTS: dict[str, Any] = {
    "cadence": 15,
    "quiet": "night",
    "paused": False,
    "cli": None,
    # Heartbeat learning: beats only capture and a distill learns nightly ("per_beat": each
    # beat judges what it read, as before), at this local time.
    "learn": "nightly",
    "distill_at": "19:00",
}
CLI_LABELS = {
    "claude": "Claude Code",
    "codex": "Codex CLI",
    "gemini": "Gemini CLI",
    "gnomon": "Gnomon",
    "n8n": "n8n",
}
HOW_LABELS = {
    "mcp": "MCP",
    "computer": "Computer use",
    "folder": "Folder",
    "n8n": "n8n",
    "memory": "Memory and chats",
}
TOKENLESS = ("folder", "n8n")  # reads that cost no tokens: Scout, and n8n workflows
INBOX_MAX = 200  # items one post may bring
LOCK_MINUTES = 20  # a beat that holds the lock longer than this has stopped: its lock expires
BACKOFF_MINUTES = 30  # after a provider limit, scheduled beats wait this long
MAX_ITEMS = 25  # items read from one connector in one beat
READ_OPENS = 3  # items a read opens in full, newest first, after one listing of what changed
FILE_WORDS = 4000  # characters of a Scout file the judge reads


def _file_words(ws: Any, path: str, name: str) -> str:
    """The words of a file Scout found, as Capture would read them, cut to FILE_WORDS: the judge
    decides from what the file says, not from its name."""
    try:
        with open(path, "rb") as handle:
            data = handle.read(8_000_000)
    except OSError:
        return ""
    extractor = getattr(ws.rt, "extractor", None)
    text = extractor.extract(data, name, None) if extractor is not None else None
    if text is None:
        text = data.decode("utf-8", errors="replace")
    text = text.strip()
    return text if len(text) <= FILE_WORDS else text[:FILE_WORDS] + " [cut to fit]"


WRITE_KINDS = ("sheet", "progress", "task", "note")
LIMIT = re.compile(
    r"rate.?limit|usage limit|quota|\b429\b|too many requests|limit (?:reached|exceeded)|"
    r"overloaded|out of (?:credits|tokens)",
    re.I,
)
# What a Brain or a CLI that cannot answer now raises: the beat goes on without that part.
UNREADABLE = (WorkspaceError, OSError, ValueError, RuntimeError, KeyError, AttributeError)
SIGN_IN = re.compile(
    r"login no longer works|not signed in|sign in again|authenticat|unauthori[sz]ed|\b401\b",
    re.I,
)


def _hash(*parts: Any) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()


def _flat(text: Any, limit: int) -> str:
    flat = " ".join(str(text or "").split())
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def json_answer(text: str) -> Any:
    """The JSON object in a model's answer (fenced or not); ValueError when there is none."""
    body = str(text or "").strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", body, re.S)
    if fenced:
        body = fenced.group(1).strip()
    start, end = body.find("{"), body.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object in the answer")
    return json.loads(body[start : end + 1])


def tokens_of(usage: dict[str, Any] | None) -> int | None:
    if not usage:
        return None
    return sum(
        int(usage.get(k) or 0) for k in ("tokens_in", "tokens_out", "cache_read", "cache_write")
    )


def skip_words(text: Any) -> list[str]:
    """What a read leaves out, from the owner's words: comma or line separated, lowercased,
    each once, in their order."""
    out: list[str] = []
    for word in re.split(r"[,\n]", str(text or "")):
        word = " ".join(word.split()).lower()[:80]
        if word and word not in out:
            out.append(word)
    return out[:SKIP_WORDS]


def classify(message: str) -> str:
    """The connector status a failed read means."""
    if LIMIT.search(message):
        return "provider_limit"
    if SIGN_IN.search(message):
        return "unhealthy_sign_in"
    return "error"


class Heartbeat:
    def __init__(self, ws: Workspace, runners: Runners) -> None:
        self.ws = ws
        self.runners = runners
        self._running: dict[str, Any] | None = None  # the beat under way: what it reads now
        self._guard = threading.Lock()
        # Gnomon itself lists a server's tools (tests stand in a fake server's listing here).
        self.list_tools = conn.list_tools
        self.discover = conn.discover
        self.find = conn.find
        self._accounts: tuple[float, list[conn.Found]] | None = None
        # Gnomon's own n8n, when it hosts one (open_runtime sets it), and where n8n on this
        # computer reaches Gnomon's inbox.
        self.n8n_host: Any = None
        # The Test Bench after each distill (regression.py), set by the app.
        self.regression: Any = None
        self.inbox_url = "http://127.0.0.1:8787"
        # Memory pasted from another AI app at onboarding, for the read that carries it.
        self._pasted: dict[str, str] = {}

    def _account(self, who: Principal, *, fresh: bool = False) -> list[conn.Found] | None:
        """The Claude account's connectors (claude.ai), as Claude Code lists them when it
        starts; kept for ACCOUNT_TTL so a beat does not start Claude Code once per connector.
        None when Claude Code could not say (signed out, not here): nothing is changed then."""
        now = time.monotonic()
        if not fresh and self._accounts and now - self._accounts[0] < ACCOUNT_TTL:
            return self._accounts[1]
        try:
            listed = conn.from_account(self.runners.account_servers(who))
        except (WorkspaceError, OSError) as exc:
            log.info("the Claude account's connectors could not be listed: %s", exc)
            return self._accounts[1] if self._accounts else None
        self._accounts = (now, listed)
        return listed

    def _checked(
        self, who: Principal, row: dict[str, Any]
    ) -> tuple[conn.Found | None, tuple[str, str, list[str], list[str]]]:
        """A connector as a read finds it, and its check. An account connector still
        connecting (they load in the background, so a listing a moment old can miss it) is
        listed again, fresh, up to ACCOUNT_RECHECKS times before the beat leaves it."""
        found = self._find(who, row)
        if found is None:
            return None, ("off", "", [], [])
        checked = self._check(found)
        tries = 0
        while (
            checked[0] in ("error", "connecting")
            and found.spec.get("account")
            and tries < ACCOUNT_RECHECKS
        ):
            tries += 1
            again = next(
                (f for f in self._account(who, fresh=True) or [] if f.id == row["id"]), None
            )
            if again is None:
                break
            found, checked = again, self._check(again)
        if checked[0] == "connecting":
            # Still not connected when it was to be read: now that is a failure.
            checked = (
                "error",
                "Claude Code could not reach it just now. The next look tries again.",
                [],
                [],
            )
        return found, checked

    def _find(self, who: Principal, row: dict[str, Any]) -> conn.Found | None:
        if row.get("found_in") == ACCOUNT_PLACE:
            return next((f for f in self._account(who) or [] if f.id == row["id"]), None)
        return self.find(row["id"])

    # ---------------------------------------------------------------- settings

    def settings(self) -> dict[str, Any]:
        with self.ws.rt.read() as tx:
            row = tx.one(schema.workspaces, self.ws.rt.workspace_id, "workspace")
        found = dict((row["settings"] or {}).get("heartbeat") or {})
        return {**DEFAULTS, **found, "timezone": row["timezone"] or "UTC"}

    def _save(
        self, who: Principal, changes: dict[str, Any], event: str, data: dict[str, Any]
    ) -> None:
        with self.ws.rt.write() as tx:
            row = tx.one(schema.workspaces, self.ws.rt.workspace_id, "workspace")
            settings = dict(row["settings"] or {})
            settings["heartbeat"] = {**dict(settings.get("heartbeat") or {}), **changes}
            tx.update(schema.workspaces, row, {"settings": settings})
            tx.emit(
                event,
                actor_id=who.actor_id,
                subject_type="workspace",
                subject_id=row["id"],
                data=data,
            )

    def cli(self, settings: dict[str, Any] | None = None) -> str:
        """The governing CLI: the one chosen for the heartbeat, else the knowledge agent's, else
        Claude Code. A beat never switches it."""
        chosen = (settings or self.settings()).get("cli")
        if chosen in RUNNERS:
            return str(chosen)
        try:
            model = self.ws.agent.chosen_model() or ""
        except UNREADABLE:  # no Brain: the default
            model = ""
        named = model.partition(":")[0]
        return named if named in RUNNERS else "claude"

    def in_quiet_hours(self, settings: dict[str, Any], moment: datetime | None = None) -> bool:
        quiet = settings.get("quiet") or "off"
        if quiet == "off":
            return False
        local = (moment or self.ws.rt.clock.now()).astimezone(zone(settings["timezone"]))
        night = local.hour >= 19 or local.hour < 7
        if quiet == "nights_weekends":
            return night or local.weekday() >= 5
        return night

    def configure(
        self,
        who: Principal,
        *,
        cadence: int | None = None,
        quiet: str | None = None,
        paused: bool | None = None,
        cli: str | None = None,
        connectors: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        who.require("workspace.admin")
        changes: dict[str, Any] = {}
        if cadence is not None:
            if int(cadence) not in CADENCES:
                raise ValidationFailed(
                    f"Cadence is one of {', '.join(map(str, CADENCES))} minutes.", field="cadence"
                )
            changes["cadence"] = int(cadence)
        if quiet is not None:
            if quiet not in QUIET:
                raise ValidationFailed(
                    f"Quiet hours are one of {', '.join(QUIET)}.", field="quiet_hours"
                )
            changes["quiet"] = quiet
        if paused is not None:
            changes["paused"] = bool(paused)
            if not paused:
                changes["backoff"] = None  # resuming tries again at once
        if cli is not None:
            if cli not in RUNNERS:
                raise ValidationFailed(f"The CLI is one of {', '.join(RUNNERS)}.", field="cli")
            changes["cli"] = cli
        if changes:
            self._save(who, changes, "heartbeat.configured", {k: v for k, v in changes.items()})
        for item in connectors or []:
            self.set_connector(
                who,
                str(item.get("id") or ""),
                enabled=item.get("enabled"),
                feeds=item.get("feeds"),
                cadence_minutes=item.get("cadence_minutes"),
                ask=item.get("ask"),
            )
        return self.status(who)

    # ---------------------------------------------------------------- connectors

    def _connectors(self) -> list[dict[str, Any]]:
        with self.ws.rt.read() as tx:
            return tx.rows(
                sa.select(schema.heartbeat_connectors)
                .where(schema.heartbeat_connectors.c.workspace_id == self.ws.rt.workspace_id)
                .order_by(schema.heartbeat_connectors.c.cli, schema.heartbeat_connectors.c.name)
            )

    def _connector_view(self, row: dict[str, Any]) -> dict[str, Any]:
        own = row["cadence_minutes"]
        how = HOW_LABELS.get(row["how"], row["how"])
        return {
            "id": row["id"],
            "name": row["name"],
            "cli": row["cli"],
            "cli_label": CLI_LABELS.get(row["cli"], row["cli"]),
            "how": row["how"],
            "how_label": how + (f", every {own} min" if own else ""),
            "cadence_minutes": own,
            "feeds": list(row["feeds"] or []),
            "enabled": bool(row["enabled"]),
            "last_read_at": row["last_read_at"],
            "last_items": int(row["last_items"] or 0),
            "status": row["status"],
            "status_line": row["status_line"],
            "tools": list(row["tools"] or []),
            "found_in": row["found_in"],
            "present": bool(row["present"]),
            "workflow_id": row.get("workflow_id"),
            "has_workflow": bool(row.get("workflow")),
            "fix": (row.get("workflow_fix") or {}).get("why") if row.get("workflow_fix") else None,
            "ask": row.get("ask") or "",
            "skip": row.get("skip") or "",
            "sign_in": sign_in(row),
        }

    def connectors(self, who: Principal) -> list[dict[str, Any]]:
        who.require("brain.read")
        return [self._connector_view(r) for r in self._connectors()]

    def _check(self, found: conn.Found) -> tuple[str, str, list[str], list[str]]:
        """(status, status line, read-only tools, other tools) of one MCP server, as Gnomon
        lists its tools itself."""
        if found.how == "computer":
            label = CLI_LABELS.get(found.cli, found.cli)
            return (
                "off",
                f"Reads through {label}'s computer use, which the heartbeat does not use yet.",
                [],
                [],
            )
        if found.spec.get("account"):
            # Listed by Claude Code from the account: names only, no annotations.
            said = found.spec.get("status")
            if said == "needs-auth":
                return (
                    "unhealthy_sign_in",
                    "It needs signing in again in Claude (claude.ai, Settings, Connectors). "
                    "It is not read until then.",
                    [],
                    [],
                )
            tools = [{"name": n} for n in found.spec.get("tools") or [] if isinstance(n, str)]
            if said != "connected" or not tools:
                # Discovery alone is never a failure: it had not connected yet when looked for.
                return (
                    "connecting",
                    "Found in your Claude account; it had not connected when Contxt looked. "
                    "Run a Test read to check it.",
                    [],
                    [],
                )
        else:
            try:
                tools = self.list_tools(found)
            except conn.ListingFailed as exc:
                return (
                    "cannot_confirm_read_only",
                    f"Its tools could not be listed ({exc}), so it cannot be confirmed "
                    "read-only. It is not read.",
                    [],
                    [],
                )
        allowed, refused = conn.keep_read_only(tools, found.name)
        if not allowed:
            return (
                "cannot_confirm_read_only",
                f"None of its {len(tools)} tools only reads, so it is not read.",
                [],
                refused,
            )
        return (
            "ok",
            f"Read-only: {len(allowed)} of {len(tools)} tools"
            + (f" ({len(refused)} that could change something are refused)" if refused else "")
            + ".",
            allowed,
            refused,
        )

    def scout_changed(self, who: Principal) -> None:
        """Scout's folders changed: its heartbeat row says what it reads now (the rest of the
        connectors wait for the next Find connectors)."""
        folders = Scout(self.ws).overview(who).get("folders") or []
        with self.ws.rt.write() as tx:
            row = tx.get(schema.heartbeat_connectors, "scout")
            if row is None or row["workspace_id"] != tx.workspace_id:
                return
            tx.update(
                schema.heartbeat_connectors,
                row,
                {
                    "status_line": (
                        f"Reads the {len(folders)} Scout folder(s) by content."
                        if folders
                        else "Choose a folder for Scout first."
                    ),
                    "status": "ok" if folders and row["enabled"] else "off",
                },
            )

    def rescan(self, who: Principal) -> dict[str, Any]:
        """Find connectors in my CLIs: read each CLI's settings again and list each server's
        tools. New connectors that only read start on; the owner's on/off choices stay."""
        who.require("workspace.admin")
        account = self._account(who, fresh=True)
        found = [*self.discover(), *(account or [])]
        checked = {f.id: (f, self._check(f)) for f in found}
        scout = Scout(self.ws).overview(who)
        has_folders = bool(scout.get("folders"))
        now = iso(self.ws.rt.clock.now())
        added: list[str] = []
        with self.ws.rt.write() as tx:
            rows = {
                r["id"]: r
                for r in tx.rows(
                    sa.select(schema.heartbeat_connectors).where(
                        schema.heartbeat_connectors.c.workspace_id == tx.workspace_id
                    )
                )
            }
            for cid, (f, (status, line, allowed, _refused)) in checked.items():
                row = rows.get(cid)
                values = {
                    "name": f.name,
                    "cli": f.cli,
                    "how": f.how,
                    "tools": allowed,
                    "found_in": f.found_in,
                    "present": True,
                }
                if row is None:
                    # One of the account's connectors reads the person's own account (their
                    # mail, their CRM): it starts off until they turn it on.
                    mine = bool(f.spec.get("account"))
                    on = status == "ok" and not mine and f.cli != "gemini"
                    if f.cli == "gemini":
                        line = (
                            "Found in Gemini CLI. Gnomon does not read through Gemini yet; "
                            "it can suggest an n8n workflow for it."
                        )
                    if mine and status == "ok":
                        line += " Off until you turn it on: it reads your own account."
                    tx.insert(
                        schema.heartbeat_connectors,
                        {
                            "id": cid,
                            "workspace_id": tx.workspace_id,
                            **values,
                            # A read through a CLI costs tokens: daily unless the owner
                            # says otherwise (section 17.4). Scout (no tokens) reads every
                            # beat.
                            "cadence_minutes": CLI_CADENCE,
                            "feeds": [],
                            "enabled": on,
                            "status": status if status != "ok" or on else "off",
                            "status_line": line,
                            "last_items": 0,
                            "created_by": who.actor_id,
                            "created_at": now,
                            "updated_at": now,
                        },
                    )
                    added.append(cid)
                else:
                    keep, line = kept_state(row, status, line)
                    tx.update(
                        schema.heartbeat_connectors,
                        row,
                        {**values, "status": keep, "status_line": line},
                    )
            for cid, row in rows.items():
                if account is None and row["found_in"] == ACCOUNT_PLACE:
                    continue  # Claude Code could not say: leave the account's connectors be
                if row["how"] in ("n8n", "memory"):
                    continue  # an n8n workflow or a CLI's memory: not found in its settings
                if cid != "scout" and cid not in checked and row["present"]:
                    tx.update(
                        schema.heartbeat_connectors,
                        row,
                        {
                            "present": False,
                            "status": "off",
                            "status_line": "No longer in the CLI's settings.",
                        },
                    )
            scout_line = (
                f"Reads the {len(scout['folders'])} Scout folder(s) by content."
                if has_folders
                else "Choose a folder for Scout first."
            )
            if "scout" not in rows:
                tx.insert(
                    schema.heartbeat_connectors,
                    {
                        "id": "scout",
                        "workspace_id": tx.workspace_id,
                        "name": "Scout folders",
                        "cli": "gnomon",
                        "how": "folder",
                        "tools": [],
                        "found_in": "Gnomon",
                        "present": True,
                        "cadence_minutes": None,
                        "feeds": [],
                        "enabled": has_folders,
                        "status": "ok" if has_folders else "off",
                        "status_line": scout_line,
                        "last_items": 0,
                        "created_by": who.actor_id,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                added.append("scout")
            else:
                tx.update(
                    schema.heartbeat_connectors,
                    rows["scout"],
                    {
                        "status_line": scout_line,
                        "status": (
                            rows["scout"]["status"]
                            if has_folders and rows["scout"]["enabled"]
                            else "off"
                        ),
                    },
                )
            for cli in memory.CLIS:
                self._memory_row(tx, who, rows.get(f"memory:{cli}"), cli, now, added)
            tx.emit(
                "heartbeat.connectors_found",
                actor_id=who.actor_id,
                subject_type="workspace",
                subject_id=tx.workspace_id,
                data={"found": len(checked), "added": added},
            )
        listed = self.connectors(who)
        by_cli: dict[str, int] = {}
        apps = [c for c in listed if c["present"] and c["how"] != "memory"]
        for c in apps:
            by_cli[c["cli_label"]] = by_cli.get(c["cli_label"], 0) + 1
        said = ", ".join(f"{n} in {label}" for label, n in by_cli.items()) or "none"
        new = [a for a in added if not a.startswith("memory:")]
        return {
            "found": len(apps),
            "added": added,
            "line": f"Found {len(apps)} connectors: {said}."
            + (f" {len(new)} new." if new else " Nothing new to add."),
            "connectors": listed,
        }

    def _memory_row(
        self,
        tx: Any,
        who: Principal,
        row: dict[str, Any] | None,
        cli: str,
        now: str,
        added: list[str],
    ) -> None:
        """A CLI's memory read: there while the CLI keeps its files on this computer. A new
        one starts off; onboarding (or the owner) turns it on."""
        here = memory.present(cli)
        label = CLI_LABELS[cli]
        if row is None:
            if not here:
                return
            tx.insert(
                schema.heartbeat_connectors,
                {
                    "id": f"memory:{cli}",
                    "workspace_id": tx.workspace_id,
                    "name": f"{label} memory and chats",
                    "cli": cli,
                    "how": "memory",
                    "tools": [],
                    "found_in": memory.home(cli),
                    "present": True,
                    "cadence_minutes": MEMORY_CADENCE,
                    "feeds": [],
                    "enabled": False,
                    "status": "off",
                    "status_line": (
                        f"What {label} keeps on you: its memory, what you said in its chats, "
                        "and what it remembers from its other apps. Off until you start "
                        "onboarding."
                    ),
                    "last_items": 0,
                    "created_by": who.actor_id,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            added.append(f"memory:{cli}")
        elif bool(row["present"]) != here:
            tx.update(
                schema.heartbeat_connectors,
                row,
                {"present": here, "found_in": memory.home(cli)}
                | ({} if here else {"status": "off", "status_line": f"{label} is not here."}),
            )

    def test_read(self, who: Principal, connector_id: str) -> dict[str, Any]:
        """The setup check for any connector (v4 backend plan, 17.2), the same for every
        source: found, signed in, which tools only read, and one real sample read with the
        items it found, the tokens, the time and the cost per beat and per day. Nothing is
        kept: no cursor, no seen items, nothing staged."""
        who.require("workspace.admin")
        row = next((r for r in self._connectors() if r["id"] == connector_id), None)
        steps: list[dict[str, Any]] = []

        def done() -> dict[str, Any]:
            return {"id": connector_id, "steps": steps, "passed": all(s["ok"] for s in steps)}

        if row is None or not row["present"]:
            steps.append(
                {"step": "found", "ok": False, "line": f"No connector {connector_id} was found."}
            )
            return done()
        steps.append({"step": "found", "ok": True, "line": f"Found in {row['found_in']}."})
        if row["how"] in ("folder", "n8n", "memory"):
            steps.append(
                {
                    "step": "test read",
                    "ok": True,
                    "line": "Read on this computer: no sign-in or model read to check.",
                }
            )
            return done()
        try:
            _found, (status, line, allowed, refused) = self._checked(who, row)
        except WorkspaceError as exc:
            status, line, allowed, refused = "error", str(exc), [], []
        signed = status != "unhealthy_sign_in"
        steps.append({"step": "signed in", "ok": signed, "line": "Signed in." if signed else line})
        if not signed:
            return done()
        steps.append(
            {
                "step": "read-only",
                "ok": bool(allowed),
                "line": f"{len(allowed)} tools only read; {len(refused)} refused."
                if allowed
                else line,
            }
        )
        if not allowed:
            return done()
        started = time.monotonic()
        got = self._read_cli(who, [{**row, "status": "ok", "tools": allowed}])[row["id"]]
        seconds = round(time.monotonic() - started, 1)
        if isinstance(got, BaseException):
            steps.append({"step": "test read", "ok": False, "line": str(got), "seconds": seconds})
            return done()
        usage = got.get("usage") or {}
        cost = usage.get("cost")
        beats = 1440 / max(1, int(row["cadence_minutes"] or CLI_CADENCE))
        steps.append(
            {
                "step": "test read",
                "ok": True,
                "items": len(got["items"]),
                "returned": got.get("returned", len(got["items"])),
                "left_out": got.get("left_out", 0),
                "tokens": tokens_of(usage),
                "seconds": seconds,
                "cost": cost,
                # At most: after the first read, a beat reads only what is new.
                "per_day": round(float(cost) * beats, 4) if isinstance(cost, int | float) else None,
                "line": f"{len(got['items'])} items in {seconds} s.",
            }
        )
        return done()

    def set_connector(
        self,
        who: Principal,
        connector_id: str,
        *,
        enabled: bool | None = None,
        feeds: list[str] | None = None,
        cadence_minutes: int | None = None,
        ask: str | None = None,
        skip: str | None = None,
    ) -> dict[str, Any]:
        who.require("workspace.admin")
        with self.ws.rt.write() as tx:
            row = tx.get(schema.heartbeat_connectors, connector_id)
            if row is None or row["workspace_id"] != tx.workspace_id:
                raise NotFound(f"No connector {connector_id}.", id=connector_id)
            values: dict[str, Any] = {}
            if enabled and row["cli"] == "gemini" and row["how"] != "memory":
                raise Conflict(
                    "Gnomon does not read through Gemini yet. Suggest an n8n workflow for it."
                )
            if enabled and row["how"] == "n8n":
                problems = n8n.check_workflow(row["workflow"], row["id"])
                if problems:
                    raise Conflict("Its workflow does not only read: " + " ".join(problems))
            if enabled is not None:
                if enabled and row["status"] == "cannot_confirm_read_only":
                    raise Conflict(
                        f"{row['name']} cannot be confirmed read-only, so it stays off: "
                        "find connectors again once its tools can be listed."
                    )
                if enabled and row["how"] == "computer":
                    raise Conflict(
                        f"{row['name']} reads through computer use, which the heartbeat does "
                        "not use yet."
                    )
                values["enabled"] = bool(enabled)
                values["status"] = "ok" if enabled else "off"
                if not enabled:
                    values["status_line"] = "Off. The heartbeat does not read it."
            if feeds is not None:
                values["feeds"] = [str(f) for f in feeds][:20]
            if cadence_minutes is not None:
                own = int(cadence_minutes)
                if own and own not in OWN_CADENCES:
                    raise ValidationFailed(
                        "A connector's own cadence is one of "
                        f"{', '.join(map(str, OWN_CADENCES))} minutes.",
                        field="cadence_minutes",
                    )
                values["cadence_minutes"] = own or None
            if (ask is not None or skip is not None) and row["how"] == "folder":
                raise ValidationFailed(
                    "A folder is read whole: it takes no goal or filters.", field="ask"
                )
            if ask is not None:
                values["ask"] = str(ask).strip()[:ASK_CHARS] or None
            if skip is not None:
                values["skip"] = ", ".join(skip_words(skip)) or None
            if row["how"] == "n8n" and enabled is not None:
                values.update(self._to_n8n(row, bool(enabled)))
            if values:
                row = tx.update(schema.heartbeat_connectors, row, values)
                tx.emit(
                    "heartbeat.connector_changed",
                    actor_id=who.actor_id,
                    subject_type="heartbeat_connector",
                    subject_id=connector_id,
                    data={k: v for k, v in values.items() if k != "status_line"},
                )
        return self._connector_view(row)

    # ---------------------------------------------------------------- reads through n8n

    def add_n8n(
        self,
        who: Principal,
        *,
        name: str,
        workflow: dict[str, Any],
        feeds: list[str] | None = None,
        connector_id: str | None = None,
        line: str | None = None,
    ) -> dict[str, Any]:
        """A connector read by an n8n workflow on this computer, with the person's own n8n
        credentials. Its workflow is checked by code to only read (and to post to this
        connector's inbox); it stays off until the owner turns it on. Returns the connector
        and the secret its workflow sends with each post, shown this once."""
        who.require("workspace.admin")
        name = _flat(name, 200)
        if not name:
            raise ValidationFailed("Name the connector.", field="name")
        cid = connector_id or "n8n:" + (re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "x")
        problems = n8n.check_workflow(workflow, cid)
        if problems:
            raise ValidationFailed(
                "This workflow cannot read for the heartbeat: " + " ".join(problems),
                field="workflow",
            )
        secret, kept = n8n.new_secret()
        now = iso(self.ws.rt.clock.now())
        values = {
            "name": name,
            "cli": "n8n",
            "how": "n8n",
            "feeds": [str(f) for f in feeds or []][:20],
            "enabled": False,
            "status": "off",
            "status_line": line
            or "Waits for your yes: it reads with your n8n sign-ins, read-only.",
            "tools": [],
            "found_in": "n8n on this computer",
            "present": True,
            "workflow": workflow,
            "secret_hash": kept,
        }
        with self.ws.rt.write() as tx:
            row = tx.get(schema.heartbeat_connectors, cid)
            if row is not None and row["workspace_id"] == tx.workspace_id:
                if row["how"] != "n8n":
                    raise Conflict(f"{cid} is a connector of another kind.")
                row = tx.update(schema.heartbeat_connectors, row, values)
            else:
                row = tx.insert(
                    schema.heartbeat_connectors,
                    {
                        "id": cid,
                        "workspace_id": tx.workspace_id,
                        **values,
                        "cadence_minutes": None,
                        "cursor": None,
                        "last_read_at": None,
                        "last_items": 0,
                        "workflow_id": None,
                        "created_by": who.actor_id,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            tx.emit(
                "heartbeat.n8n_workflow_set",
                actor_id=who.actor_id,
                subject_type="heartbeat_connector",
                subject_id=cid,
                data={"name": name, "nodes": len(workflow.get("nodes") or [])},
            )
        return {"connector": self._connector_view(row), "secret": secret}

    def _to_n8n(self, row: dict[str, Any], active: bool) -> dict[str, Any]:
        """Into Gnomon's own n8n, when it hosts one: the workflow, aimed at this connector's
        inbox with a fresh secret (n8n keeps it; Gnomon its hash), active or not. Without a
        hosted n8n, nothing changes there (the owner runs the workflow in their own)."""
        host = self.n8n_host
        if host is None or not host.installed() or not row.get("workflow"):
            return {}
        secret, kept = n8n.new_secret()
        copy = n8n.prepare(row["workflow"], row["id"], self.inbox_url, secret)
        try:
            if active and not host.error_secret_hash():
                # Once: Gnomon's own workflow that reports a failure of any of its workflows.
                said, said_hash = n8n.new_secret()
                host.put_workflow(n8n.error_reporter(self.inbox_url, said), active=False)
                host.keep_error_secret_hash(said_hash)
            workflow_id = host.put_workflow(copy, active=active)
        except (OSError, RuntimeError, ValueError) as exc:
            raise Conflict(f"n8n could not take the workflow: {exc}") from exc
        return {"workflow_id": workflow_id, "secret_hash": kept}

    # ---------------------------------------------------------------- learning: the buffer

    def configure_learning(
        self,
        who: Principal,
        *,
        goal: str | None = None,
        filters: list[dict[str, Any]] | None = None,
        buffer_days: int | None = None,
        learn: str | None = None,
        distill_at: str | None = None,
        live: dict[str, list[str]] | None = None,
    ) -> dict[str, Any]:
        """The goal relevance is scored against, the negative filters (each a pattern, words
        or a /regex/, with an optional source and field) and how long the buffer is kept."""
        who.require("workspace.admin")
        changes: dict[str, Any] = {}
        if goal is not None:
            changes["goal"] = _flat(goal, 600)
        if filters is not None:
            clean = []
            for raw in filters[:50]:
                pattern = _flat(raw.get("pattern"), 200) if isinstance(raw, dict) else ""
                if not pattern:
                    raise ValidationFailed("Each filter needs a pattern.", field="filters")
                clean.append(
                    {
                        "pattern": pattern,
                        "source": _flat(raw.get("source"), 240),
                        "field": _flat(raw.get("field"), 120),
                        "why": _flat(raw.get("why"), 200),
                    }
                )
            changes["filters"] = clean
        if learn is not None:
            if learn not in ("nightly", "per_beat"):
                raise ValidationFailed("Learn nightly or per beat.", field="learn")
            changes["learn"] = learn
        if live is not None:
            changes["live"] = {
                "connectors": [_flat(c, 240) for c in live.get("connectors") or []][:50],
                "sheets": [_flat(s, 200) for s in live.get("sheets") or []][:50],
            }
        if distill_at is not None:
            if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", distill_at):
                raise ValidationFailed("A distill time is HH:MM.", field="distill_at")
            changes["distill_at"] = distill_at
        if buffer_days is not None:
            if not 1 <= int(buffer_days) <= 365:
                raise ValidationFailed("Keep the buffer 1 to 365 days.", field="buffer_days")
            changes["buffer_days"] = int(buffer_days)
        if changes:
            self._save(who, changes, "heartbeat.learning_configured", {"keys": sorted(changes)})
        return self.learning_settings(who)

    def learning_settings(self, who: Principal) -> dict[str, Any]:
        who.require("brain.read")
        found = self.settings()
        return {
            "goal": found.get("goal") or "",
            "filters": list(found.get("filters") or []),
            "buffer_days": int(found.get("buffer_days") or learning.BUFFER_DAYS),
            "learn": found.get("learn") or "nightly",
            "distill_at": found.get("distill_at") or learning.DISTILL_AT,
            "live": found.get("live") or {"connectors": [], "sheets": []},
        }

    def distill_due(self, settings: dict[str, Any] | None = None) -> bool:
        """Nightly learning: past today's distill time, nothing distilled since, and something
        staged (a distill missed while the computer slept runs at the next start)."""
        settings = settings or self.settings()
        if settings.get("learn") != "nightly" or settings.get("paused"):
            return False
        local = self.ws.rt.clock.now().astimezone(zone(settings["timezone"]))
        hour, _, minute = str(settings.get("distill_at") or learning.DISTILL_AT).partition(":")
        at = local.replace(hour=int(hour), minute=int(minute or 0), second=0, microsecond=0)
        if local < at:
            at -= timedelta(days=1)
        last = next((b for b in self._beats(limit=50) if b["trigger"] == "distill"), None)
        if last is not None and parse_iso(last["started_at"]) >= at:
            return False
        table = schema.staged_changes
        with self.ws.rt.read() as tx:
            waiting = tx.conn.execute(
                sa.select(sa.func.count())
                .select_from(table)
                .where(table.c.workspace_id == tx.workspace_id, table.c.status == "staged")
            ).scalar()
        return bool(waiting)

    def _distill(self, beat_id: str, started: float) -> None:
        """The distill: learn from everything the buffer holds, then record the run."""
        table = schema.staged_changes
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(table).where(
                    table.c.workspace_id == tx.workspace_id, table.c.status == "staged"
                )
            )
        owner = self.ws.owner()
        learned = self._learn(owner, beat_id, rows)
        version, saved = self._tag_version(beat_id, learned)
        checked = self._after_distill(owner, saved)
        checked += self._world_overview(owner)
        routed = [o["route"] for o in learned["outcomes"].values()]
        details = [
            f"Distilled {len(rows)} changes into {learned['items']} items: "
            + ", ".join(f"{routed.count(r)} {r.replace('_', ' ')}" for r in learning.ROUTES)
            + ".",
            *learned["details"],
            *([version] if version else []),
            *checked,
        ]
        self._finish(
            beat_id,
            started,
            status="done",
            connectors=[],
            items=learned["items"],
            tokens=learned["tokens"],
            unknown=learned["unknown"],
            errors=[],
            details=details,
            **learned["counts"],
        )

    def _tag_version(self, beat_id: str, learned: dict[str, Any]) -> tuple[str, str]:
        """After a distill that changed the Brain, tag a Brain version (the Test Bench's 15.1),
        so a regression run can compare it with the one before and roll entries back: what
        to say, and the version's name ("" when none was saved)."""
        counts = learned["counts"]
        if not (counts["changes"] or counts["waits"] or counts["held"]):
            return "", ""
        try:
            found = self.ws.rt.brain.save_version(
                cause="distill", note=f"Heartbeat distill {beat_id}"
            )
        except WorkspaceError as exc:
            return f"No Brain version was saved: {_flat(str(exc), 160)}", ""
        made = found.get("version")
        name = str(made.get("name") if isinstance(made, dict) else made or "")
        with self.ws.rt.write() as tx:
            tx.emit(
                "brain.versioned",
                actor_id=self.ws.owner().actor_id,
                subject_type="beat",
                subject_id=beat_id,
                data={"cause": "distill", "version": name},
            )
        return f"Saved as Brain version {name}".strip() + ".", name

    def _after_distill(self, owner: Principal, version: str) -> list[str]:
        """The Test Bench's after-distill suites on the new version (15.6); never fails the
        distill."""
        if not version or self.regression is None:
            return []
        try:
            return list(self.regression.after_distill(owner, version)["details"])
        except Exception as exc:  # the bench or its grader: the distill stands
            log.warning("after-distill tests on %s stopped", version, exc_info=True)
            return [f"The Test Bench did not run: {_flat(str(exc), 160)}"]

    def _world_overview(self, owner: Principal) -> list[str]:
        """When the map changed enough (a new region, or a project closed), the ingest model
        drafts the world overview; the draft waits for the owner (13.4). Never fails the
        distill."""
        try:
            wanted = self.ws.rt.brain.world_draft()
            if not wanted.get("due"):
                return []
            model = self.ws.distill.settings(owner).get("model")
            found = self.runners.ask_measured(
                owner, str(wanted["prompt"]), job="world-draft", model=model
            )
            made = self.ws.rt.brain.propose_world(
                str(found.get("answer") or ""), wanted.get("regions") or {}
            )
        except Exception as exc:  # the model or the Brain: the distill stands
            log.warning("world overview draft stopped", exc_info=True)
            return [f"The world overview was not drafted: {_flat(str(exc), 160)}"]
        if made.get("status") != "proposed":
            return [f"The world overview draft was not kept: {made.get('reason') or 'empty'}"]
        return ["Drafted a new world overview: it waits for you in Review."]

    def _learn(
        self,
        owner: Principal,
        beat_id: str,
        rows: list[dict[str, Any]],
        *,
        only_sheets: set[str] | None = None,
    ) -> dict[str, Any]:
        """Net the buffer rows by code, route them with one model call per batch (the ingest
        model) against the goal, the filters and the Brain's alignment, apply code's rules,
        write through the one proposal path, and mark each entry with what it became. With
        `only_sheets` (apply during the day), only writes to those sheets go in now; the rest
        of an item stays in the buffer for tonight. Nothing in the buffer changes until its
        outcome is written."""
        settings = self.settings()
        cli = self.cli(settings)
        table = schema.staged_changes
        names = {r["id"]: r for r in self._connectors()}
        details: list[str] = []
        outcomes: dict[str, dict[str, Any]] = {}  # staged id: outcome
        items: list[dict[str, Any]] = []
        for change in learning.net(rows):
            if change.net_zero:
                for r in change.rows:
                    outcomes[r["id"]] = {"route": "discard", "reason": "undone the same day"}
                continue
            connector = names.get(change.connector_id) or {}
            last = change.rows[-1]
            items.append(
                {
                    "id": change.item_id,
                    "key": f"connector:{change.connector_id}:{change.item_id}",
                    "connector_id": change.connector_id,
                    "connector_name": connector.get("name") or change.connector_id,
                    "cli": connector.get("cli") or "",
                    "feeds": list(connector.get("feeds") or []),
                    "title": last["title"],
                    "text": change.new or "",
                    "before": change.old,
                    "edits": change.edits,
                    "deleted": change.deleted,
                    "unclear": change.unclear,
                    "at": last["changed_at"] or last["seen_at"],
                    "url": last["url"],
                    "speaker": "",
                    "private": False,
                    "rows": [r["id"] for r in change.rows],
                }
            )
        counts = {"changes": 0, "waits": 0, "held": 0}
        tokens: int | None = None
        unknown = 0
        brain = self._brain()
        for start in range(0, len(items), learning.BATCH):
            batch = items[start : start + learning.BATCH]
            answer, spent = self._ask(owner, cli, learning.distill_prompt(batch, brain, settings))
            if spent is None:
                unknown += 1
            else:
                tokens = (tokens or 0) + spent
            try:
                judged = json_answer(answer)
            except ValueError as exc:
                raise Conflict(
                    "The distill's answer could not be read; nothing was written."
                ) from exc
            by_key = {i["key"]: i for i in batch}
            kept = []
            for verdict in judged.get("items") or []:
                if not isinstance(verdict, dict):
                    continue
                item = by_key.get(str(verdict.get("item") or ""))
                if item is None:
                    continue
                route, reason = learning.route_of(verdict, item)
                if only_sheets is not None:
                    # During the day only what lands in a live sheet goes in; the rest of the
                    # item waits for tonight's distill, still in the buffer.
                    live = [
                        w
                        for w in verdict.get("writes") or []
                        if isinstance(w, dict)
                        and w.get("kind") == "sheet"
                        and str(w.get("sheet") or "") in only_sheets
                    ]
                    if route != "short" or not live:
                        continue
                    verdict = {**verdict, "writes": live}
                    reason = f"applied during the day: {reason}".strip()
                for staged_id in item["rows"]:
                    outcomes[staged_id] = {"route": route, "reason": reason}
                if route == "discard":
                    continue
                item["review"] = route == "catch_up"
                kept.append(verdict)
            if kept:
                self._replace_waiting(owner, [by_key[str(v["item"])] for v in kept], details)
                written = self._write_all(
                    owner, beat_id, batch, {**judged, "items": kept}, brain, details
                )
                for key in counts:
                    counts[key] += written.get(key, 0)
            if only_sheets is None:
                for pattern in judged.get("patterns") or []:
                    self._lesson(owner, pattern, details)
        with self.ws.rt.write() as tx:
            proposals = {
                line["item_id"]: line["proposal_id"]
                for line in tx.rows(
                    sa.select(schema.heartbeat_lines).where(
                        schema.heartbeat_lines.c.beat_id == beat_id
                    )
                )
            }
            by_row = {r["id"]: r for r in rows}
            for staged_id, outcome in outcomes.items():
                row = by_row[staged_id]
                key = f"connector:{row['connector_id']}:{row['item_id']}"
                tx.conn.execute(
                    table.update()
                    .where(table.c.id == staged_id)
                    .values(status="distilled", outcome=outcome, proposal_id=proposals.get(key))
                )
        return {
            "items": len(items),
            "counts": counts,
            "tokens": tokens,
            "unknown": unknown,
            "details": details,
            "outcomes": outcomes,
        }

    def _apply_live(
        self, owner: Principal, beat_id: str, settings: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Apply during the day: what this beat captured from a live connector is distilled
        now, whole; for a live sheet, only what lands in it goes in now. A small call on the
        ingest model, only on beats with such changes; nothing else changes before tonight."""
        live = settings.get("live") or {}
        connectors = set(live.get("connectors") or [])
        sheets = set(live.get("sheets") or [])
        if not connectors and not sheets:
            return None
        table = schema.staged_changes
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(table).where(
                    table.c.workspace_id == tx.workspace_id,
                    table.c.beat_id == beat_id,
                    table.c.status == "staged",
                )
            )
        whole = [r for r in rows if r["connector_id"] in connectors]
        rest = [r for r in rows if r["connector_id"] not in connectors]
        total: dict[str, Any] = {
            "counts": {"changes": 0, "waits": 0, "held": 0},
            "tokens": None,
            "unknown": 0,
            "details": [],
        }
        for part, only in ((whole, None), (rest, sheets or None)):
            if not part or (only is None and part is rest):
                continue
            learned = self._learn(owner, beat_id, part, only_sheets=only)
            for key in total["counts"]:
                total["counts"][key] += learned["counts"][key]
            if learned["tokens"] is not None:
                total["tokens"] = (total["tokens"] or 0) + learned["tokens"]
            total["unknown"] += learned["unknown"]
            total["details"] += learned["details"]
        return total

    def _replace_waiting(
        self, owner: Principal, items: list[dict[str, Any]], details: list[str]
    ) -> None:
        """A later net change on an item replaces what still waits from an earlier distill:
        that proposal is turned down, so Catch up holds only the newest."""
        table = schema.staged_changes
        replaced = 0
        for item in items:
            with self.ws.rt.read() as tx:
                earlier = tx.rows(
                    sa.select(table.c.proposal_id, table.c.outcome).where(
                        table.c.workspace_id == tx.workspace_id,
                        table.c.connector_id == item["connector_id"],
                        table.c.item_id == item["id"],
                        table.c.status == "distilled",
                        table.c.proposal_id.is_not(None),
                    )
                )
            for row in earlier:
                if (row["outcome"] or {}).get("route") != "catch_up":
                    continue
                try:
                    self.ws.brain.decide(
                        owner,
                        str(row["proposal_id"]),
                        approve=False,
                        reason="A later change replaced it (the heartbeat's distill).",
                    )
                    replaced += 1
                except WorkspaceError:
                    continue  # already decided: nothing waits
        if replaced:
            details.append(f"Replaced {replaced} waiting in Catch up with a later change.")

    def still_waiting(self, who: Principal) -> dict[str, Any]:
        """What the distill sent to Catch up over 14 days ago and still waits: one card, so
        nothing expires silently."""
        who.require("brain.read")
        since = iso(self.ws.rt.clock.now() - timedelta(days=learning.STALE_DAYS))
        table = schema.staged_changes
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(table).where(
                    table.c.workspace_id == tx.workspace_id,
                    table.c.status == "distilled",
                    table.c.proposal_id.is_not(None),
                    table.c.seen_at < since,
                )
            )
        waiting = {p.get("id") for p in self.ws.brain.proposals(who)}
        names = {r["id"]: r["name"] for r in self._connectors()}
        stale = [
            learning.view(r, names)
            for r in rows
            if (r["outcome"] or {}).get("route") == "catch_up" and r["proposal_id"] in waiting
        ]
        return {"count": len(stale), "days": learning.STALE_DAYS, "items": stale}

    def _lesson(self, owner: Principal, pattern: Any, details: list[str]) -> None:
        """A pattern across the day's changes becomes a lesson proposal (it waits in Catch
        up: a lesson shapes every AI)."""
        if not isinstance(pattern, dict) or not _flat(pattern.get("title"), 120):
            return
        try:
            self.ws.rt.brain.propose_lesson(
                title=_flat(pattern.get("title"), 120),
                story=_flat(pattern.get("story") or pattern.get("pattern"), 1200),
                pattern=_flat(pattern.get("pattern"), 600),
                # The words that bring it up: the model's, else the title's own.
                words=[
                    _flat(w, 40)
                    for w in pattern.get("words")
                    or sorted(learning.content_words(str(pattern.get("title") or "")))[:4]
                    if _flat(w, 40)
                ][:8],
                evidence="Seen across a day of changes by the heartbeat's distill.",
                capture_origin="connector",
            )
            details.append(f"Proposed a lesson: {_flat(pattern.get('title'), 120)}")
        except WorkspaceError as exc:
            details.append(f"A lesson could not be proposed: {_flat(str(exc), 160)}")

    def staged(self, who: Principal, *, status: str | None = None) -> list[dict[str, Any]]:
        """Heartbeat › Today: what the buffer holds (newest first), each with its source,
        old and new value, relevance and what a filter said."""
        who.require("brain.read")
        table = schema.staged_changes
        query = sa.select(table).where(table.c.workspace_id == self.ws.rt.workspace_id)
        if status:
            query = query.where(table.c.status == status)
        with self.ws.rt.read() as tx:
            rows = tx.rows(query.order_by(table.c.seen_at.desc(), table.c.id.desc()).limit(500))
        names = {r["id"]: r["name"] for r in self._connectors()}
        return [learning.view(r, names) for r in rows]

    def _set_staged(self, who: Principal, staged_id: str, status: str, event: str) -> dict:
        who.require("workspace.admin")
        table = schema.staged_changes
        with self.ws.rt.write() as tx:
            row = tx.get(table, staged_id)
            if row is None or row["workspace_id"] != tx.workspace_id:
                raise NotFound(f"No change {staged_id} in the buffer.", id=staged_id)
            if row["status"] == "distilled":
                raise Conflict("The distill has already used it: undo what it wrote instead.")
            tx.conn.execute(table.update().where(table.c.id == staged_id).values(status=status))
            tx.emit(
                event,
                actor_id=who.actor_id,
                subject_type="staged_change",
                subject_id=staged_id,
                data={"connector": row["connector_id"]},
            )
            row = dict(row, status=status)
        return learning.view(row, {r["id"]: r["name"] for r in self._connectors()})

    def forget_staged(self, who: Principal, staged_id: str) -> dict[str, Any]:
        """Forget this: out of the buffer before the distill sees it (an undo signal)."""
        return self._set_staged(who, staged_id, "forgotten", "heartbeat.staged_forgotten")

    def bring_back(self, who: Principal, staged_id: str) -> dict[str, Any]:
        """Bring back what a filter dropped: the distill sees it like any other change."""
        return self._set_staged(who, staged_id, "staged", "heartbeat.staged_brought_back")

    def n8n_status(self, who: Principal) -> dict[str, Any]:
        who.require("brain.read")
        if self.n8n_host is None:
            return {"installed": False, "running": False, "line": "n8n is not available here."}
        status: dict[str, Any] = self.n8n_host.status()
        return status

    def n8n_act(self, who: Principal, action: str) -> dict[str, Any]:
        """Host n8n here: install it, start it or stop it."""
        who.require("workspace.admin")
        host = self.n8n_host
        if host is None:
            raise Conflict("n8n is not available here.")
        try:
            if action == "install":
                result: dict[str, Any] = host.install()
            elif action == "start":
                result = host.start()
                host.remember(True)
            elif action == "stop":
                result = host.stop()
                host.remember(False)
            else:
                raise ValidationFailed("Install, start or stop.", field="action")
        except RuntimeError as exc:
            raise Conflict(str(exc)) from exc
        return result

    def suggest_n8n(self, who: Principal) -> dict[str, Any]:
        """The connector search, then an n8n workflow suggested for each app it found that
        Gnomon has one for. A suggestion is off until the person signs in to that app in n8n
        and turns it on here; one already suggested is left as it is."""
        who.require("workspace.admin")
        self.rescan(who)
        rows = self._connectors()
        have = {r["id"] for r in rows if r["how"] == "n8n"}
        suggested: list[str] = []
        already: list[str] = []
        without: set[str] = set()
        apps: set[str] = set()
        for row in rows:
            if row["how"] in TOKENLESS:
                continue
            app = templates.app_of(row["name"])
            if app is None:
                without.add(row["name"])
                continue
            label = templates.label_of(app)
            if app in apps:
                continue
            apps.add(app)
            cid = f"n8n:{app}"
            if cid in have:
                already.append(label)
                continue
            where = CLI_LABELS.get(row["cli"], row["cli"])
            self.add_n8n(
                who,
                name=f"{label} through n8n",
                workflow=templates.workflow_for(app, cid, self.inbox_url),
                feeds=list(row["feeds"] or []),
                connector_id=cid,
                line=(
                    f"Suggested from {row['name']} in {where}. In n8n, sign in to {label} on "
                    f"its Read {label} node, then turn it on here."
                ),
            )
            suggested.append(label)
        said = (
            f"Suggested {len(suggested)} n8n workflow{'s' if len(suggested) != 1 else ''}: "
            f"{', '.join(suggested)}."
            if suggested
            else "No new workflow to suggest."
        )
        if without:
            said += f" No workflow yet for {', '.join(sorted(without))}."
        return {
            "suggested": suggested,
            "already": already,
            "without_workflow": sorted(without),
            "line": said,
        }

    # ---------------------------------------------------------------- upkeep of n8n reads

    def n8n_failed(self, secret: str, body: Any) -> dict[str, Any]:
        """Gnomon's error-report workflow says one of its workflows failed: that connector
        shows the error, until it works again (a post to its inbox) or the owner fixes it."""
        host = self.n8n_host
        if host is None or not n8n.secret_ok(secret, host.error_secret_hash()):
            raise PermissionDenied("Not a report this secret may send.")
        workflow_id, why = n8n.failure_of(body)
        with self.ws.rt.write() as tx:
            rows = tx.rows(
                sa.select(schema.heartbeat_connectors).where(
                    schema.heartbeat_connectors.c.workspace_id == tx.workspace_id,
                    schema.heartbeat_connectors.c.workflow_id == workflow_id,
                )
            )
            for row in rows:
                tx.update(
                    schema.heartbeat_connectors,
                    row,
                    {
                        "status": "error",
                        "status_line": f"n8n: {why}. Fix it in n8n, or ask for a suggested fix.",
                    },
                )
        return {"connectors": [r["id"] for r in rows]}

    def suggest_fix(self, who: Principal, connector_id: str) -> dict[str, Any]:
        """The knowledge agent drafts a fix for an n8n read whose workflow failed: checked to
        only read, then waiting for the owner to keep or skip it (one model call, on request)."""
        who.require("workspace.admin")
        row = self._n8n_row(connector_id)
        prompt = (
            "An n8n workflow that reads an app for Gnomon failed. Fix it so it works, changing "
            "as little as you can. It must stay read-only: app nodes may only use their read "
            "operations, no Code or Function node, and its one HTTP request posts to the same "
            'inbox. Reply with one JSON object only: {"why": "<one plain sentence on what '
            'you changed>", "workflow": <the whole workflow>}.\n'
            f"What n8n said: {row['status_line']}\n" + json.dumps(row["workflow"])
        )
        answer, _spent = self._ask(who, self.cli(), prompt)
        try:
            found = json_answer(answer)
        except ValueError as exc:
            raise Conflict("The suggested fix could not be read. Try again.") from exc
        workflow = found.get("workflow")
        problems = n8n.check_workflow(workflow, connector_id)
        if problems:
            raise Conflict(
                "The suggested fix would not only read, so it was dropped: " + " ".join(problems)
            )
        fix = {"workflow": workflow, "why": _flat(found.get("why"), 300) or "A suggested fix."}
        with self.ws.rt.write() as tx:
            current = tx.one(schema.heartbeat_connectors, connector_id, "connector")
            current = tx.update(schema.heartbeat_connectors, current, {"workflow_fix": fix})
        return self._connector_view(current)

    def decide_fix(self, who: Principal, connector_id: str, keep: bool) -> dict[str, Any]:
        """Keep the suggested fix (checked again; into n8n if the read is on) or skip it."""
        who.require("workspace.admin")
        row = self._n8n_row(connector_id)
        fix = row.get("workflow_fix")
        if not fix:
            raise Conflict("No suggested fix waits for this connector.")
        values: dict[str, Any] = {"workflow_fix": None}
        if keep:
            problems = n8n.check_workflow(fix["workflow"], connector_id)
            if problems:
                raise Conflict("That fix does not only read: " + " ".join(problems))
            values.update(
                workflow=fix["workflow"], status_line="Fixed. It reads again on its next run."
            )
            if row["enabled"]:
                values.update(self._to_n8n({**row, "workflow": fix["workflow"]}, True))
                values["status"] = "ok"
        with self.ws.rt.write() as tx:
            current = tx.one(schema.heartbeat_connectors, connector_id, "connector")
            current = tx.update(schema.heartbeat_connectors, current, values)
        return self._connector_view(current)

    def _n8n_row(self, connector_id: str) -> dict[str, Any]:
        with self.ws.rt.read() as tx:
            row = tx.get(schema.heartbeat_connectors, connector_id)
        if row is None or row["how"] != "n8n":
            raise NotFound(f"No n8n read {connector_id}.", id=connector_id)
        return dict(row)

    def receive(self, connector_id: str, secret: str, body: Any) -> dict[str, Any]:
        """What an n8n workflow posted, kept for the next beat. Only an approved n8n read,
        with its own secret, may post."""
        items = n8n.items_of(body)[:INBOX_MAX]
        with self.ws.rt.write() as tx:
            row = tx.get(schema.heartbeat_connectors, connector_id)
            if row is None or row["how"] != "n8n" or not n8n.secret_ok(secret, row["secret_hash"]):
                raise PermissionDenied("Not a connector this secret may post to.")
            if not row["enabled"]:
                raise Conflict(f"{row['name']} is off: nothing it posts is kept.")
            if row["status"] == "error":
                tx.update(
                    schema.heartbeat_connectors,
                    row,
                    {"status": "ok", "status_line": "Working again: n8n posted what it read."},
                )
            for item in items:
                tx.insert(
                    schema.heartbeat_inbox,
                    {
                        "id": tx.new_id("hbi"),
                        "workspace_id": tx.workspace_id,
                        "connector_id": connector_id,
                        "item": item,
                        "received_at": tx.now,
                        "beat_id": None,
                    },
                )
        return {"kept": len(items)}

    def _read_n8n(self, row: dict[str, Any]) -> dict[str, Any]:
        """A read through n8n: what its workflow posted since the last beat took (0 tokens)."""
        if n8n.check_workflow(row["workflow"], row["id"]):
            raise ConnectorProblem("off", "Its workflow no longer only reads, so it is off.")
        table = schema.heartbeat_inbox
        with self.ws.rt.read() as tx:
            waiting = tx.rows(
                sa.select(table)
                .where(table.c.connector_id == row["id"], table.c.beat_id.is_(None))
                .order_by(table.c.received_at)
                .limit(MAX_ITEMS * 4)
            )
        items, _cursor = self._items({"items": [w["item"] for w in waiting]}, row)
        return {
            "items": items,
            "cursor": row["cursor"],
            "usage": None,
            "inbox": [w["id"] for w in waiting],
        }

    # ---------------------------------------------------------------- status

    def _beats(self, *, since: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        table = schema.heartbeat_beats
        query = sa.select(table).where(table.c.workspace_id == self.ws.rt.workspace_id)
        if since:
            query = query.where(table.c.started_at >= since)
        with self.ws.rt.read() as tx:
            return tx.rows(
                query.order_by(table.c.started_at.desc(), table.c.id.desc()).limit(limit)
            )

    @staticmethod
    def beat_view(row: dict[str, Any]) -> dict[str, Any]:
        changes = int(row["changes"] or 0)
        nothing = row["status"] == "done" and not (
            changes or row["waits"] or row["held"] or row["items"]
        )
        return {
            "id": row["id"],
            "status": row["status"],
            "trigger": row["trigger"],
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "connectors": list(row["connectors"] or []),
            "connectors_read": len([c for c in row["connectors"] or [] if c.get("read")]),
            "items": int(row["items"] or 0),
            "changes": changes,
            "waits": int(row["waits"] or 0),
            "held": int(row["held"] or 0),
            "tokens": row["tokens"],
            "tokens_known": row["tokens"] is not None and not row["tokens_unknown"],
            "tokens_unknown": int(row["tokens_unknown"] or 0),
            "duration_ms": row["duration_ms"],
            "errors": list(row["errors"] or []),
            "details": list(row["details"] or []),
            "nothing_new": nothing,
        }

    def beats(
        self, who: Principal, *, since: str | None = None, limit: int = 20
    ) -> list[dict[str, Any]]:
        who.require("brain.read")
        return [self.beat_view(r) for r in self._beats(since=since, limit=max(1, min(limit, 100)))]

    def next_beat_at(self, settings: dict[str, Any], last: dict[str, Any] | None) -> str | None:
        if settings.get("paused") or not self.set_up():
            return None
        now = self.ws.rt.clock.now()
        due = (
            parse_iso(last["started_at"]) + timedelta(minutes=int(settings["cadence"]))
            if last
            else now
        )
        backoff = settings.get("backoff") or {}
        if backoff.get("until"):
            due = max(due, parse_iso(backoff["until"]))
        due = max(due, now)
        for _ in range(24 * 4 * 3):  # past quiet hours, a quarter hour at a time
            if not self.in_quiet_hours(settings, due):
                break
            due = due + timedelta(minutes=15)
        return iso(due)

    def status(self, who: Principal) -> dict[str, Any]:
        """Cadence, quiet hours, the last and next beat, paused or not, why a beat would not
        run now, and the connectors."""
        who.require("brain.read")
        settings = self.settings()
        rows = self._beats(limit=1)
        last = self.beat_view(rows[0]) if rows else None
        listed = [self._connector_view(r) for r in self._connectors()]
        cli = self.cli(settings)
        label = CLI_LABELS.get(cli, cli)
        signed_in: bool | None = None
        detail = ""
        try:
            found = self.runners.status(RUNNERS[cli])
            signed_in = False if not found.installed else found.signed_in
            detail = found.detail
        except UNREADABLE:  # cannot tell now
            signed_in = None
        backoff = settings.get("backoff") or {}
        now = iso(self.ws.rt.clock.now())
        backing = backoff if backoff.get("until") and backoff["until"] > now else {}
        quiet = self.in_quiet_hours(settings)
        if settings.get("paused"):
            state, line = "paused", "Paused. Nothing is read or changed until you resume."
        elif signed_in is False:
            state = "unhealthy_sign_in"
            line = f"{label} is not signed in, so the heartbeat waits. {detail}".strip()
        elif backing:
            state, line = str(backing.get("reason")), str(backing.get("line"))
        elif not any(c["enabled"] for c in listed):
            state = "no_connectors"
            line = (
                "Off until a connector is on. Find connectors in your CLIs in Settings, Approval."
                if listed
                else "Off until you set up a connector. Find connectors in your CLIs in "
                "Settings, Approval."
            )
        elif quiet:
            state = "quiet"
            line = f"Quiet hours ({QUIET[settings['quiet']]}). The next beat waits until they end."
        else:
            state, line = "ready", "Ready."
        running = dict(self._running) if self._running else None
        return {
            "state": state,
            "line": line,
            "cadence": int(settings["cadence"]),
            "cadences": list(CADENCES),
            "own_cadences": list(OWN_CADENCES),
            "quiet_hours": settings["quiet"],
            "quiet_options": [{"id": k, "label": v} for k, v in QUIET.items()],
            "in_quiet_hours": quiet,
            "paused": bool(settings.get("paused")),
            "cli": cli,
            "cli_label": label,
            "signed_in": signed_in,
            "backoff": backing or None,
            "last_beat": last,
            "next_beat_at": self.next_beat_at(settings, rows[0] if rows else None),
            "running": running,
            "connectors": listed,
            "cost": self.cost(),
        }

    def cost(self) -> dict[str, Any]:
        """What a beat uses, from the beats' own records: with changes and with none."""
        rows = [r for r in self._beats(limit=50) if r["status"] == "done"]
        changed = [r["tokens"] for r in rows if r["changes"] and r["tokens"] is not None]
        nothing = [r["tokens"] for r in rows if not r["changes"] and r["tokens"] is not None]

        def average(values: list[int]) -> int | None:
            return round(sum(values) / len(values)) if values else None

        return {
            "with_changes": average(changed),
            "nothing_new": average(nothing),
            "beats": len(rows),
            "unknown": len([r for r in rows if r["tokens"] is None]),
        }

    # ---------------------------------------------------------------- the beat

    def set_up(self) -> bool:
        """Whether any connector is on: until one is, the heartbeat is off (section 17.4)."""
        return any(
            r["enabled"] and r["present"] and r["how"] != "computer" for r in self._connectors()
        )

    def due(self) -> bool:
        """Whether the worker should beat now (a scheduled beat)."""
        settings = self.settings()
        if settings.get("paused") or self.in_quiet_hours(settings) or not self.set_up():
            return False
        rows = self._beats(limit=1)
        at = self.next_beat_at(settings, rows[0] if rows else None)
        return at is not None and at <= iso(self.ws.rt.clock.now())

    def _skip(self, reason: str, line: str) -> dict[str, Any]:
        return {"skipped": reason, "line": line}

    def start(self, who: Principal, trigger: str = "now") -> dict[str, Any]:
        """Take the lock and open the beat's record; a skip says why there is no beat."""
        who.require("brain.propose")
        settings = self.settings()
        if settings.get("paused"):
            return self._skip("paused", "The heartbeat is paused. Resume it first.")
        if trigger == "schedule" and self.in_quiet_hours(settings):
            return self._skip("quiet", "Quiet hours: no beat until they end.")
        now = self.ws.rt.clock.now()
        backoff = settings.get("backoff") or {}
        if trigger == "schedule" and backoff.get("until") and backoff["until"] > iso(now):
            return self._skip(str(backoff.get("reason")), str(backoff.get("line")))
        cli = self.cli(settings)
        try:
            found = self.runners.status(RUNNERS[cli])
        except UNREADABLE:  # the Brain or the CLI cannot say now
            found = None
        if found is not None and (not found.installed or found.signed_in is False):
            label = CLI_LABELS.get(cli, cli)
            line = (
                f"{label} is not installed on this computer, so the heartbeat waits."
                if not found.installed
                else f"{label} is not signed in, so the heartbeat waits. Sign in: {found.sign_in}"
            )
            return self._skip("unhealthy_sign_in", line)
        beat_id = new_id("beat", now)
        with self.ws.rt.write() as tx:
            lock = tx.get(schema.heartbeat_locks, tx.workspace_id)
            if lock is not None and lock["expires_at"] > tx.now:
                return self._skip("busy", "Another beat is running. It finishes first.")
            until = iso(now + timedelta(minutes=LOCK_MINUTES))
            if lock is None:
                tx.insert(
                    schema.heartbeat_locks,
                    {"id": tx.workspace_id, "beat_id": beat_id, "expires_at": until},
                )
            else:  # a stale lock: the beat that held it stopped
                tx.conn.execute(
                    schema.heartbeat_locks.update()
                    .where(schema.heartbeat_locks.c.id == tx.workspace_id)
                    .values(beat_id=beat_id, expires_at=until)
                )
            tx.insert(
                schema.heartbeat_beats,
                {
                    "id": beat_id,
                    "workspace_id": tx.workspace_id,
                    "status": states.BEAT.initial,
                    "trigger": trigger,
                    "started_at": tx.now,
                    "connectors": [],
                    "errors": [],
                    "details": [],
                    "created_by": who.actor_id,
                    "created_at": tx.now,
                    "updated_at": tx.now,
                },
            )
            tx.emit(
                "heartbeat.beat_started",
                actor_id=who.actor_id,
                subject_type="heartbeat_beat",
                subject_id=beat_id,
                data={"trigger": trigger},
            )
        self._running = {"beat_id": beat_id, "reading": None, "read": []}
        return {"beat_id": beat_id}

    def beat(self, who: Principal, trigger: str = "now", *, wait: bool = True) -> dict[str, Any]:
        """One beat: read what changed, judge it, write it through governance, record it.
        `wait` False starts it in the background and returns its id."""
        opened = self.start(who, trigger)
        if "skipped" in opened:
            return opened
        beat_id = opened["beat_id"]
        if not wait:
            threading.Thread(
                target=self._run_safely, args=(beat_id,), name="heartbeat", daemon=True
            ).start()
            return {"beat_id": beat_id, "status": "running"}
        self._run_safely(beat_id)
        return self.get_beat(who, beat_id)

    def get_beat(self, who: Principal, beat_id: str) -> dict[str, Any]:
        who.require("brain.read")
        with self.ws.rt.read() as tx:
            row = tx.get(schema.heartbeat_beats, beat_id)
        if row is None or row["workspace_id"] != self.ws.rt.workspace_id:
            raise NotFound(f"No beat {beat_id}.", id=beat_id)
        return self.beat_view(row)

    def _run_safely(self, beat_id: str) -> None:
        started = time.monotonic()
        try:
            if self._trigger(beat_id) == "distill":
                self._distill(beat_id, started)
            else:
                self._run(beat_id, started)
        except Exception as exc:  # the beat's record says what stopped it
            log.warning("beat %s failed", beat_id, exc_info=True)
            self._finish(beat_id, started, status="failed", errors=[_flat(str(exc), 300)])
        finally:
            with self.ws.rt.write() as tx:
                tx.conn.execute(
                    schema.heartbeat_locks.delete().where(
                        schema.heartbeat_locks.c.id == tx.workspace_id,
                        schema.heartbeat_locks.c.beat_id == beat_id,
                    )
                )
            self._running = None

    def _due_connectors(self, trigger: str, cadence: int) -> list[dict[str, Any]]:
        now = self.ws.rt.clock.now()
        out = []
        rows = self._connectors()
        # An app n8n reads (no tokens) is not also read through a CLI (section 17.4).
        by_n8n = {r["id"].partition(":")[2] for r in rows if r["how"] == "n8n" and r["enabled"]}
        for row in rows:
            if not row["enabled"] or not row["present"] or row["how"] == "computer":
                continue
            if row["how"] not in TOKENLESS and templates.app_of(row["name"]) in by_n8n:
                continue
            if trigger == "onboarding":
                if row["how"] == "memory":
                    out.append(row)
                continue
            if row["status"] == "cannot_confirm_read_only":
                continue
            own = int(row["cadence_minutes"] or 0)
            needed = max(own, cadence) if trigger == "schedule" else own
            if trigger == "schedule" and row["how"] not in TOKENLESS and not row["last_items"]:
                # Every read that costs tokens backs off while nothing is newer (17.4).
                needed = max(needed, self._quiet_gap(row, now))
            if row["last_read_at"] and needed:
                elapsed = (now - parse_iso(row["last_read_at"])).total_seconds() / 60
                if elapsed < needed - 0.5:
                    continue
            out.append(row)
        return out

    def _quiet_gap(self, row: dict[str, Any], now: Any) -> int:
        """Minutes between scheduled reads of a connector whose last read found nothing new:
        half the time it has been quiet, at most QUIET_GAP (two days). A read through a CLI
        costs tokens even when nothing is new; anything new brings its own cadence back."""
        seen = schema.heartbeat_seen
        with self.ws.rt.read() as tx:
            last = tx.conn.execute(
                sa.select(sa.func.max(seen.c.seen_at)).where(
                    seen.c.workspace_id == tx.workspace_id, seen.c.connector_id == row["id"]
                )
            ).scalar()
        since = last or row["created_at"]
        if not since:
            return 0
        quiet = (now - (parse_iso(since) if isinstance(since, str) else since)).total_seconds()
        return min(QUIET_GAP, int(quiet / 120))

    def _read_prompt(self, parts: list[tuple[Any, ...]]) -> str:
        """What a read asks for: per connector (server, name, since, its tools, and the
        owner's goal and what to leave out, when set), what changed."""

        def line(server: str, name: str, since: str, tools: list[str], *rest: Any) -> str:
            ask, skip = (list(rest) + ["", ""])[:2]
            return (
                f"- {name} (server {server}), since {since}. Its tools: {', '.join(tools)}."
                + (f" Look for: {ask}." if ask else "")
                + (f" Leave out: {skip}." if skip else "")
            )

        listed = "\n".join(line(*part) for part in parts)
        return (
            "Read, read-only, what changed in each of these connectors since the time given. "
            "Make one list or search call for what changed since then; then open at most "
            f"{READ_OPENS} items a connector, the newest first, all in one step, and stop: no "
            "further calls. "
            "When the list or search shows each item's date and none is newer than that time, "
            "answer at once with an empty list; when it shows no dates, open the newest to "
            "check. "
            "Use only the tools listed for each. Never send, post, reply, create, change, "
            "delete or pay for anything; you have no tool that can. Reply with one JSON object "
            'only: {"connectors": {"<server>": {"cursor": "<the newest time or marker you '
            'saw>", "items": [{"id": "<the item\'s own id>", "title": "...", "text": "<what it '
            'says or what changed, at most 600 characters>", "at": "<ISO 8601 time>", "url": '
            '"", "speaker": "<who wrote it, or empty>", "private": false}]}}}, with an entry '
            "for every server below. Mark private true for anything marked private or "
            f"confidential. At most {MAX_ITEMS} items a connector, newest first; none: an "
            "empty list.\n" + listed
        )

    @staticmethod
    def _part(
        parts: dict[str, Any], answer: dict[str, Any], server: str, name: str, single: bool
    ) -> Any:
        """One connector's part of a read's answer: under its server, else under its name
        (a model often answers by the name it was shown), else the only part or the whole
        answer when the read was of this connector alone."""
        if parts.get(server) is not None:
            return parts[server]

        def key(text: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", str(text).lower().replace("claude.ai", ""))

        wanted = {key(server.replace("claude_ai_", "")), key(name)}
        for label, part in parts.items():
            if key(label) in wanted:
                return part
        if single:
            return next(iter(parts.values())) if len(parts) == 1 else (None if parts else answer)
        return None

    def _items(
        self,
        part: Any,
        row: dict[str, Any],
        *,
        most: int = MAX_ITEMS,
        chars: int = 600,
        stats: dict[str, int] | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        """(items, cursor) from one connector's part of a read's answer. An item without an
        id is known by its link, else its title and time. `stats` gets how many the answer
        returned, how many were kept and how many named what the owner leaves out."""
        part = part if isinstance(part, dict) else {}
        returned = part.get("items") if isinstance(part.get("items"), list) else []
        leave_out = skip_words(row.get("skip"))
        items = []
        left_out = 0
        for raw in returned[:most]:
            if not isinstance(raw, dict):
                continue
            said = f"{raw.get('title') or ''} {raw.get('text') or ''}".lower()
            if any(word in said for word in leave_out):
                left_out += 1
                continue
            ident = str(raw.get("id") or raw.get("url") or "").strip() or " ".join(
                str(raw.get(k) or "").strip() for k in ("title", "at") if raw.get(k)
            )
            if not ident:
                continue
            items.append(
                {
                    "id": _flat(ident, 300),
                    "title": _flat(raw.get("title"), 200),
                    "text": _flat(raw.get("text"), chars),
                    "at": _flat(raw.get("at"), 32),
                    "url": _flat(raw.get("url"), 300),
                    "speaker": _flat(raw.get("speaker"), 80),
                    "private": raw.get("private") is True,
                }
            )
        if stats is not None:
            stats.update(returned=len(returned), kept=len(items), left_out=left_out)
        return items, _flat(part.get("cursor"), 200) or row["cursor"]

    def _read_cli(self, owner: Principal, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """Read every due connector of the CLIs: {connector id: {items, cursor, usage} or the
        problem}. Claude Code reads all of its connectors in one run (one for those in its
        settings files, one for the Claude account's), so it starts, and loads its tools, once
        a beat; the run's tokens are counted once. Codex reads one connector a run."""
        out: dict[str, Any] = {}
        groups: dict[str, list[tuple[dict[str, Any], conn.Found, list[str], list[str]]]] = {}
        for row in rows:
            found, (status, line, allowed, refused) = self._checked(owner, row)
            if found is None:
                out[row["id"]] = ConnectorProblem(
                    "off", "No longer in the CLI's settings.", present=False
                )
                continue
            if status != "ok":
                out[row["id"]] = ConnectorProblem(status, line, tools=allowed)
                continue
            key = (
                f"codex:{row['id']}"
                if found.cli == "codex"
                else ("claude:account" if found.spec.get("account") else "claude:settings")
            )
            groups.setdefault(key, []).append((row, found, allowed, refused))
        for key, members in groups.items():
            servers = {conn.safe_server(f.name): (r, f, a, x) for r, f, a, x in members}
            prompt = self._read_prompt(
                [
                    (server, f.name, r["cursor"] or FIRST_LOOK, a, r.get("ask"), r.get("skip"))
                    for server, (r, f, a, _x) in servers.items()
                ]
            )
            try:
                if key.startswith("codex:"):
                    ((server, (r, f, a, x)),) = servers.items()
                    got = self.runners.read_through(
                        owner,
                        "codex",
                        server=server,
                        spec=f.spec,
                        allowed=a,
                        refused=x,
                        prompt=prompt,
                    )
                else:
                    got = self.runners.read_many(
                        owner,
                        servers={s: m[1].spec for s, m in servers.items()},
                        allowed={s: m[2] for s, m in servers.items()},
                        refused={s: m[3] for s, m in servers.items()},
                        prompt=prompt,
                        model=self.ws.distill.settings(owner).get("model"),
                    )
                answer = json_answer(got.get("text") or "")
            except WorkspaceError as exc:
                for r, *_rest in members:
                    out[r["id"]] = exc
                continue
            except ValueError:
                for r, *_rest in members:
                    out[r["id"]] = ConnectorProblem("error", "Its answer could not be read.")
                continue
            parts = answer.get("connectors") if isinstance(answer.get("connectors"), dict) else {}
            loaded = got.get("loaded")
            first = True
            for server, (r, _f, a, _x) in servers.items():
                if loaded is not None and server not in loaded:
                    out[r["id"]] = ConnectorProblem(
                        "error",
                        "Claude Code could not load it from your account just now. The next "
                        "beat tries again.",
                    )
                    continue
                part = self._part(parts, answer, server, r["name"], len(servers) == 1)
                stats: dict[str, int] = {}
                items, cursor = self._items(part, r, stats=stats)
                out[r["id"]] = {
                    "returned": stats.get("returned", 0),
                    "left_out": stats.get("left_out", 0),
                    "items": items,
                    "cursor": cursor,
                    "usage": got.get("usage") if first else None,
                    "shared": not first,  # the run's tokens went with the first connector
                    "tools": a,
                }
                first = False
        return out

    def _read(self, owner: Principal, row: dict[str, Any]) -> dict[str, Any]:
        """What changed in one connector since its cursor: {items, cursor, usage}."""
        if row["how"] == "folder":
            return self._read_scout(owner, row)
        if row["how"] == "n8n":
            return self._read_n8n(row)
        if row["how"] == "memory":
            return self._read_memory(owner, row)
        got = self._read_cli(owner, [row])[row["id"]]
        if isinstance(got, BaseException):
            raise got
        return dict(got)

    def _read_scout(self, owner: Principal, row: dict[str, Any]) -> dict[str, Any]:
        try:
            found = Scout(self.ws).scan(owner)
        except ValidationFailed as exc:
            raise ConnectorProblem("off", str(exc)) from exc
        items = []
        for diff in found.get("found") or []:
            if diff.get("quiet"):
                continue
            name = str(diff["path"]).replace("\\", "/").rsplit("/", 1)[-1]
            words = _file_words(self.ws, str(diff["path"]), name)
            items.append(
                {
                    "id": str(diff["path"])[:300],
                    "title": name,
                    "text": f"{'A new file' if diff['kind'] == 'new' else 'A new version of'} "
                    f"{name} in a Scout folder."
                    + (f" It says:\n{words}" if words else " Its words could not be read."),
                    "at": found.get("last_inspection") or "",
                    "url": "",
                    "speaker": "",
                    "private": False,
                    "digest": _hash(diff["path"], diff.get("size"), found.get("last_inspection")),
                }
            )
        return {"items": items[:MAX_ITEMS], "cursor": found.get("last_inspection"), "usage": None}

    def _seen(self, connector_id: str, keys: list[tuple[str, str]]) -> set[tuple[str, str]]:
        if not keys:
            return set()
        table = schema.heartbeat_seen
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(table.c.item_id, table.c.digest, table.c.seen_at)
                .where(
                    table.c.connector_id == connector_id,
                    table.c.item_id.in_([k for k, _ in keys]),
                )
                .order_by(table.c.seen_at.desc(), table.c.id.desc())
            )
        # Each item's latest version only: an item back to what it said before has changed
        # again (and a change undone the same day nets to nothing in the distill).
        latest: dict[str, str] = {}
        for r in rows:
            latest.setdefault(r["item_id"], r["digest"])
        return set(latest.items())

    def _brain(self) -> dict[str, Any]:
        """What the judge is shown: the regions (the Brain's areas), its projects, its sheets
        and its must-holds."""
        brain = self.ws.rt.brain
        try:
            areas = [{"id": a.key, "name": a.label} for a in brain.catalog().areas]
        except UNREADABLE:  # the Brain or the CLI cannot say now
            areas = []
        try:
            projects = [
                {
                    "id": p.id,
                    "title": p.title,
                    "status": p.status,
                    "now": p.current,
                    "area": (p.topic or "").partition("/")[0],
                }
                for p in brain.projects()
            ]
        except UNREADABLE:  # the Brain or the CLI cannot say now
            projects = []
        try:
            sheets = [
                {
                    "id": s.get("id"),
                    "title": s.get("title"),
                    "key": s.get("key"),
                    "columns": [c.get("name") for c in s.get("columns") or []],
                }
                for s in brain.get_state().get("sheets") or []
            ]
        except UNREADABLE:  # the Brain or the CLI cannot say now
            sheets = []
        try:
            holds = [
                {"id": n["id"], "text": n.get("text") or n.get("title")}
                for group in brain.ladder().get("must_holds") or []
                for n in group.get("notes") or []
            ]
        except UNREADABLE:  # the Brain or the CLI cannot say now
            holds = []
        return {"regions": areas, "projects": projects, "sheets": sheets, "must_holds": holds}

    def _regions_now(self) -> dict[str, dict[str, Any]]:
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(schema.heartbeat_regions).where(
                    schema.heartbeat_regions.c.workspace_id == tx.workspace_id
                )
            )
        return {r["id"]: r for r in rows}

    def _ask(
        self, owner: Principal, cli: str, prompt: str, timeout: float = 300
    ) -> tuple[str, int | None]:
        """One answer from a CLI and the tokens it said it used (None: unknown)."""
        if cli == "claude":
            # The judge reads what came in: it runs on the ingest model (Haiku by default).
            model = self.ws.distill.settings(owner).get("model")
            found = self.runners.ask_json(owner, prompt, timeout=timeout, model=model)
            usage = usage_of(found)
            if usage is not None:
                self.runners.count_usage("heartbeat", usage)
            return str(found.get("result") or ""), tokens_of(usage)
        # Codex and Gemini CLI do not say what one answer used in this mode.
        answer = self.runners.ask(owner, prompt, runner=cli, job="heartbeat", timeout=timeout)
        return answer, None

    # ---------------------------------------------------------------- memory and onboarding

    def memory_prompt(self, row: dict[str, Any], found: memory.Gathered, pasted: str = "") -> str:
        """What a memory read asks the CLI: turn what it keeps on the person, what it remembers
        from its own other apps (Claude Code from Claude, Codex from ChatGPT, Gemini CLI from
        Gemini), and anything pasted from another AI app, into context packets."""
        cli = row["cli"]
        label = CLI_LABELS.get(cli, cli)
        lines = [
            f"You are {label}, helping the person you work with set up their Brain: the "
            "long-term context every AI app they use reads first. Below is what you keep on "
            "them on this computer: your memory files and what they said in your recent chats. "
            f"Also add what you remember about them from {memory.SURFACES[cli]} on the same "
            "account: saved memories, their instructions for you, what they told you there. "
            "Only what you can actually see; never guess, and say where each came from. If you "
            "can see none of it, add nothing for it.",
            "Turn all of it into context packets: durable things worth knowing in every future "
            "conversation. Who they are and their role; their projects and where each stands; "
            "decisions; preferences and rules for how to work with them; the people they work "
            "with; the tools and accounts they use; recurring work. One packet a topic; merge "
            "what repeats; when two disagree, the newer wins and the packet says when it "
            "changed. Keep who said it: an assistant's suggestion is not their decision. Leave "
            "out passwords, keys and tokens; mark private true for anything private or "
            "confidential.",
            "This is the first read: build the whole picture."
            if not row["cursor"]
            else "This read covers only what is new since the last one: leave out what is not "
            "new or changed.",
        ]
        if row.get("ask"):
            lines.append(f"The person asked you to look for: {row['ask']}")
        lines.append(
            'Reply with one JSON object only: {"packets": [{"id": "<a short stable slug for '
            'the topic>", "title": "...", "text": "<what is known, with dates, at most 1000 '
            'characters>", "at": "<ISO 8601 time of the newest thing it rests on>", "speaker": '
            '"<the person, or memory, or the app it came from>", "private": false}]}. At most '
            f"{MEMORY_ITEMS} packets, the most useful first; nothing worth keeping: an empty list."
        )
        if found.memory:
            lines.append("Your memory files:")
            lines += [f"--- {where}\n{text}" for where, text in found.memory]
        if found.said:
            lines.append("What they said in your chats, newest first:")
            lines += [
                f"- [{at}]" + (f" ({project})" if project else "") + f" {text}"
                for at, project, text in found.said
            ]
        if pasted:
            lines += ["What they pasted from another AI app's memory:", pasted]
        return "\n\n".join(lines)

    def _read_memory(self, owner: Principal, row: dict[str, Any]) -> dict[str, Any]:
        """A read of what one CLI keeps on the person: Gnomon gathers its files, the CLI makes
        the context packets. A later read with nothing new makes no CLI call and costs no
        tokens; the first always asks, for what the CLI remembers from its other apps."""
        pasted = self._pasted.pop(row["id"], "")
        found = memory.gather(row["cli"], since=row["cursor"])
        if row["cursor"] and found.empty() and not pasted:
            return {"items": [], "cursor": row["cursor"], "tokens": None}
        label = CLI_LABELS.get(row["cli"], row["cli"])
        try:
            status = self.runners.status(RUNNERS[row["cli"]])
        except UNREADABLE:  # the CLI cannot say now: the read finds out
            status = None
        if status is not None and (not status.installed or status.signed_in is False):
            raise ConnectorProblem(
                "unhealthy_sign_in",
                f"{label} is not signed in, so its memory is not read. Sign in: {status.sign_in}"
                if status.installed
                else f"{label} is not installed on this computer.",
            )
        text, spent = self._ask(
            owner, row["cli"], self.memory_prompt(row, found, pasted), timeout=900
        )
        try:
            answer = json_answer(text)
        except ValueError as exc:
            raise ConnectorProblem("error", "Its answer could not be read.") from exc
        packets = answer.get("packets") if isinstance(answer, dict) else None
        items, _cursor = self._items({"items": packets or []}, row, most=MEMORY_ITEMS, chars=1000)
        cursor = found.read_at or row["cursor"]
        return {"items": items, "cursor": cursor, "tokens": spent}

    def onboard(
        self,
        who: Principal,
        *,
        clis: list[str] | None = None,
        ask: str | None = None,
        pasted: str | None = None,
        wait: bool = False,
    ) -> dict[str, Any]:
        """The first, onboarding beat: each chosen CLI (every one found, by default) reads what
        it keeps on the person and makes context packets; later beats read what is new, daily.
        `pasted` is memory the person copied from another AI app; the governing CLI reads it."""
        who.require("workspace.admin")
        if not any(r["how"] == "memory" for r in self._connectors()):
            self.rescan(who)
        rows = [r for r in self._connectors() if r["how"] == "memory" and r["present"]]
        if clis:
            rows = [r for r in rows if r["cli"] in clis]
        if not rows:
            raise Conflict(
                "No AI app keeps its memory on this computer yet. Use Claude Code, Codex or "
                "Gemini CLI here first: one of them also turns what you paste from another AI "
                "app into context packets."
            )
        governing = self.cli()
        carrier = next((r for r in rows if r["cli"] == governing), rows[0])
        if pasted and pasted.strip():
            self._pasted[carrier["id"]] = mask(pasted.strip()[: memory.FIRST_BUDGET])
        for row in rows:
            self.set_connector(who, row["id"], enabled=True, ask=ask)
        started = self.beat(who, "onboarding", wait=wait)
        return {**started, "reading": [r["name"] for r in rows]}

    def judge_prompt(
        self, items: list[dict[str, Any]], brain: dict[str, Any], regions: dict[str, Any]
    ) -> str:
        shown = {
            "regions": [
                {**r, "headline": (regions.get(r["id"]) or {}).get("headline") or ""}
                for r in brain["regions"]
            ],
            "projects": brain["projects"],
            "sheets": brain["sheets"],
            "must_holds": brain["must_holds"],
            "items": [
                {
                    "item": i["key"],
                    "connector": i["connector_name"],
                    "feeds": i["feeds"],
                    "title": i["title"],
                    "text": i["text"],
                    "at": i["at"],
                    "speaker": i["speaker"],
                }
                for i in items
            ],
        }
        return (
            "You keep a person's Brain current. Below, as JSON: the Brain's regions, projects, "
            "context sheets and must-holds, and items that changed in their connected tools. "
            "For each item say which region it belongs to (one of the region ids; a connector's "
            "feeds are a hint), which project (an id, or null), one plain line saying what "
            "changed, and what it updates: none, or writes of these kinds, each with its "
            'reason: {"kind": "sheet", "sheet": id, "row": key, "column": name, "value": '
            'text}, {"kind": "progress", "project": id, "text": what moved, "next_up": text}, '
            '{"kind": "task", "project": id, "text": the task, "due": "YYYY-MM-DD" or "", '
            '"owner": ""}, {"kind": "note", "area": region id, "topic": a short '
            'lowercase-hyphenated name, "text": the fact, "revises": an entry id or null}. '
            "Prefer one write per item: the one place it belongs. "
            'Add "customer_facing": true to a write a customer would see, and "spends": true '
            "to one about spending money. Write only what the item says; never invent. "
            "If a must-hold protects what an item says (it would quote or change it), name it "
            'in "protected_by". Then a new headline (one short sentence) for each region that '
            "got a line, and a status line for each project that moved. Reply with one JSON "
            'object only: {"items": [{"item": key, "region": id, "project": id or null, '
            '"line": text, "protected_by": id or null, "writes": [...]}], "headlines": '
            '{region id: text}, "projects": {project id: text}}.\n\n'
            + json.dumps(shown, ensure_ascii=False)
        )

    def _run(self, beat_id: str, started: float) -> None:
        owner = self.ws.owner()
        settings = self.settings()
        cli = self.cli(settings)
        due = self._due_connectors(self._trigger(beat_id), int(settings["cadence"]))
        read_log: list[dict[str, Any]] = []
        errors: list[str] = []
        details: list[str] = []
        tokens: int | None = None
        unknown = 0
        fresh: list[dict[str, Any]] = []
        reads: dict[str, dict[str, Any]] = {}
        limited: str | None = None
        through_cli = [r for r in due if r["how"] not in (*TOKENLESS, "memory")]
        if through_cli and self._running is not None:
            self._running["reading"] = through_cli[0]["id"]
        batch = self._read_cli(owner, through_cli) if through_cli else {}
        for row in due:
            if self._running is not None:
                self._running["reading"] = row["id"]
            entry: dict[str, Any] = {"id": row["id"], "name": row["name"], "read": False}
            try:
                got = batch[row["id"]] if row["id"] in batch else self._read(owner, row)
                if isinstance(got, BaseException):
                    raise got
            except ConnectorProblem as problem:
                self._connector_problem(owner, row, problem)
                entry["status"] = problem.status
                if problem.status != "off":
                    errors.append(f"{row['name']}: {problem.line}")
                read_log.append(entry)
                continue
            except WorkspaceError as exc:
                status = classify(str(exc))
                if status == "provider_limit":
                    limited = str(exc)
                self._connector_problem(owner, row, ConnectorProblem(status, _flat(str(exc), 300)))
                entry["status"] = status
                errors.append(f"{row['name']}: {_flat(str(exc), 200)}")
                read_log.append(entry)
                continue
            if self._running is not None:
                self._running["read"].append(row["id"])
            spent = got["tokens"] if "tokens" in got else tokens_of(got.get("usage"))
            if row["how"] not in TOKENLESS and not got.get("shared"):
                if spent is None:
                    unknown += 1
                else:
                    tokens = (tokens or 0) + spent
            keys = [
                (i["id"], i.get("digest") or _hash(i["title"], i["text"], i["at"]))
                for i in got["items"]
            ]
            seen = self._seen(row["id"], keys)
            new = []
            for item, key in zip(got["items"], keys, strict=True):
                if key in seen:
                    continue
                new.append(
                    {
                        **item,
                        "digest": key[1],
                        "key": f"connector:{row['id']}:{item['id']}",
                        "connector_id": row["id"],
                        "connector_name": row["name"],
                        "cli": row["cli"],
                        "feeds": list(row["feeds"] or []),
                    }
                )
            # What the read's answer held against what was kept, for a read that loses items.
            entry.update(
                read=True,
                status="ok",
                returned=got.get("returned", len(got["items"])),
                left_out=got.get("left_out", 0),
                items=len(got["items"]),
                new=len(new),
            )
            read_log.append(entry)
            reads[row["id"]] = {"row": row, "got": got, "new": new}
            fresh.extend(new)
        if self._running is not None:
            self._running["reading"] = None
        if limited:
            self._back_off(
                owner, "provider_limit", f"A provider limit was hit: {_flat(limited, 160)}"
            )
        counts = {"changes": 0, "waits": 0, "held": 0}
        # Capture (heartbeat learning): relevance and the owner's filters, by code, no model.
        decisions = learning.decide(fresh, settings, self.ws.rt.brain.relevance)
        dropped = [d for d in decisions if d.dropped]
        if dropped:
            details.append(
                f"Dropped {len(dropped)} at capture by your filters; they stay in Today for "
                f"{int(settings.get('buffer_days') or learning.BUFFER_DAYS)} days."
            )
        fresh = [d.item for d in decisions if not d.dropped]
        # Onboarding's first look lands now, not tonight: it judges in the beat, as before.
        nightly = settings.get("learn") == "nightly" and self._trigger(beat_id) != "onboarding"
        if nightly and fresh:
            details.append(f"Captured {len(fresh)} for the distill.")
            fresh = []  # the distill learns from the buffer; the beat only captures
        if fresh:
            brain = self._brain()
            regions_before = self._regions_now()
            try:
                answer, spent = self._ask(
                    owner, cli, self.judge_prompt(fresh, brain, regions_before)
                )
            except WorkspaceError as exc:
                status = classify(str(exc))
                if status == "provider_limit":
                    self._back_off(
                        owner, "provider_limit", f"A provider limit was hit: {_flat(str(exc), 160)}"
                    )
                elif status == "unhealthy_sign_in":
                    self._back_off(owner, "unhealthy_sign_in", _flat(str(exc), 200))
                raise
            if spent is None:
                unknown += 1
            else:
                tokens = (tokens or 0) + spent
            try:
                judged = json_answer(answer)
            except ValueError as exc:
                raise Conflict(
                    "The judge's answer could not be read; nothing was written."
                ) from exc
            counts = self._write_all(owner, beat_id, fresh, judged, brain, details)
        elif not nightly or not decisions:
            details.append(
                f"Read {len([e for e in read_log if e['read']])} connectors. Nothing new."
                if read_log
                else "No connector was due."
            )
        # Only now are the items seen and the cursors moved: a beat that stopped before this
        # point reads the same items again next time.
        self._advance(beat_id, reads)
        # The buffer keeps every change, with what the beat made of it; old entries go.
        learning.stage(self.ws, beat_id, decisions, outcome="" if nightly else "beat")
        if nightly:
            applied = self._apply_live(owner, beat_id, settings)
            if applied is not None:
                for kind in counts:
                    counts[kind] += applied["counts"][kind]
                if applied["tokens"] is not None:
                    tokens = (tokens or 0) + applied["tokens"]
                unknown += applied["unknown"]
                details += applied["details"]
        learning.purge(self.ws, int(settings.get("buffer_days") or learning.BUFFER_DAYS))
        self._finish(
            beat_id,
            started,
            status="done",
            connectors=read_log,
            items=len(decisions),
            tokens=tokens,
            unknown=unknown,
            errors=errors,
            details=details,
            **counts,
        )

    def _trigger(self, beat_id: str) -> str:
        with self.ws.rt.read() as tx:
            row = tx.one(schema.heartbeat_beats, beat_id, "beat")
        return str(row["trigger"])

    def _connector_problem(
        self, owner: Principal, row: dict[str, Any], problem: ConnectorProblem
    ) -> None:
        with self.ws.rt.write() as tx:
            current = tx.one(schema.heartbeat_connectors, row["id"], "connector")
            values: dict[str, Any] = {"status": problem.status, "status_line": problem.line}
            if problem.present is not None:
                values["present"] = problem.present
            if problem.tools is not None:
                values["tools"] = problem.tools
            tx.update(schema.heartbeat_connectors, current, values)

    def _back_off(self, owner: Principal, reason: str, line: str) -> None:
        until = iso(self.ws.rt.clock.now() + timedelta(minutes=BACKOFF_MINUTES))
        self._save(
            owner,
            {
                "backoff": {
                    "reason": reason,
                    "line": line + " The heartbeat tries again later.",
                    "until": until,
                }
            },
            "heartbeat.backed_off",
            {"reason": reason, "until": until},
        )

    def _advance(self, beat_id: str, reads: dict[str, dict[str, Any]]) -> None:
        with self.ws.rt.write() as tx:
            for cid, read in reads.items():
                seen = schema.heartbeat_seen
                for item in read["new"]:
                    again = tx.conn.execute(
                        seen.update()
                        .where(
                            seen.c.connector_id == cid,
                            seen.c.item_id == item["id"],
                            seen.c.digest == item["digest"],
                        )
                        .values(beat_id=beat_id, seen_at=tx.now)
                    )
                    if again.rowcount:  # a version seen before is the latest again
                        continue
                    tx.insert(
                        seen,
                        {
                            "id": tx.new_id("hbs"),
                            "workspace_id": tx.workspace_id,
                            "connector_id": cid,
                            "item_id": item["id"],
                            "digest": item["digest"],
                            "beat_id": beat_id,
                            "seen_at": tx.now,
                        },
                    )
                taken = read["got"].get("inbox") or []
                if taken:  # what an n8n workflow posted: this beat took it
                    inbox = schema.heartbeat_inbox
                    tx.conn.execute(
                        inbox.update().where(inbox.c.id.in_(taken)).values(beat_id=beat_id)
                    )
                current = tx.one(schema.heartbeat_connectors, cid, "connector")
                values: dict[str, Any] = {
                    "cursor": read["got"].get("cursor"),
                    "last_read_at": tx.now,
                    "last_items": len(read["new"]),
                    "status": "ok",
                }
                if read["got"].get("tools") is not None:
                    values["tools"] = read["got"]["tools"]
                    values["status_line"] = (
                        f"Read-only: {len(read['got']['tools'])} tools. "
                        f"{len(read['new'])} new last beat."
                    )
                tx.update(schema.heartbeat_connectors, current, values)

    def _finish(self, beat_id: str, started: float, *, status: str, **fields: Any) -> None:
        with self.ws.rt.write() as tx:
            row = tx.one(schema.heartbeat_beats, beat_id, "beat")
            if row["status"] != "running":
                return
            states.BEAT.check(row["status"], status)
            values: dict[str, Any] = {
                "status": status,
                "finished_at": tx.now,
                "duration_ms": int((time.monotonic() - started) * 1000),
            }
            for key in ("connectors", "items", "changes", "waits", "held", "errors", "details"):
                if key in fields:
                    values[key] = fields[key]
            if "tokens" in fields:
                values["tokens"] = fields["tokens"]
                values["tokens_unknown"] = int(fields.get("unknown") or 0)
            row = tx.update(schema.heartbeat_beats, row, values)
            tx.emit(
                "heartbeat.beat_finished",
                actor_id=row["created_by"],
                subject_type="heartbeat_beat",
                subject_id=beat_id,
                data={
                    "status": status,
                    "items": row["items"],
                    "changes": row["changes"],
                    "waits": row["waits"],
                    "held": row["held"],
                    "tokens": row["tokens"],
                },
            )
        # The sheets' database and snapshots are committed once per beat (sheetstore.py).
        try:
            self.ws.rt.brain.commit_sheets(f"Sheets after beat {beat_id}")
        except (OSError, RuntimeError, ValueError):
            log.warning("Could not commit the sheets after beat %s", beat_id, exc_info=True)

    # ---------------------------------------------------------------- writing

    def _suppressed(self) -> set[str]:
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(schema.heartbeat_suppressions.c.fingerprint).where(
                    schema.heartbeat_suppressions.c.workspace_id == tx.workspace_id
                )
            )
        return {r["fingerprint"] for r in rows}

    def protecting(self, text: str) -> dict[str, Any] | None:
        """The must-hold an item's words would quote or change: one whose trigger words it
        uses, among those its domain brings into play."""
        try:
            found = self.ws.rt.brain.preview(text[:2000])
        except UNREADABLE:  # the Brain or the CLI cannot say now
            return None
        low = text.lower()
        for hold in found.get("must_holds") or []:
            words = [str(t).lower() for t in hold.get("triggers") or [] if str(t).strip()]
            if words and any(w in low for w in words):
                return hold
        return None

    @staticmethod
    def wrote_label(write: dict[str, Any], projects: dict[str, str]) -> str:
        kind = write.get("kind")
        title = projects.get(str(write.get("project") or ""), str(write.get("project") or ""))
        if kind == "sheet":
            where = f"{write.get('sheet')} › {write.get('row')} › {write.get('column')}"
            return f"{where}: {write.get('value')}"
        if kind == "progress":
            return f"{title} › latest progress"
        if kind == "task":
            return f"{title} › Tasks: {_flat(write.get('text'), 80)}"
        return f"{write.get('area')}/{write.get('topic')}"

    def _propose(
        self, owner: Principal, item: dict[str, Any], write: dict[str, Any], line: str
    ) -> Any:
        """Make one write through the Brain's one proposal path, with the item as its source."""
        kind = write["kind"]
        at = str(item.get("at") or "")[:10]
        # What must wait is enforced by code, not left to the model: a task or a sheet value
        # whose words send, post, reach a customer or spend money is flagged, so it waits for
        # the owner even in Auto (the recorded day's finding, Oct 25).
        flags = [f for f in ("customer_facing", "spends") if write.get(f) is True]
        if kind in ("task", "sheet"):
            said = f"{write.get('text') or ''} {write.get('value') or ''} {item.get('title') or ''}"
            gate = gate_kind("", said, [])
            if gate == "spend" and "spends" not in flags:
                flags.append("spends")
            elif gate in ("post", "customer_facing") and "customer_facing" not in flags:
                flags.append("customer_facing")
        cited: dict[str, Any] = {
            "title": _flat(line or item["title"] or item["text"], 120),
            "evidence": _flat(
                f"Why: {write.get('reason') or 'it changed'}. From {item['connector_name']}: "
                f"{item['text']}",
                1200,
            ),
            "sources": [item["key"]],
            "source_locator": _flat(item.get("url") or item.get("title") or item["id"], 80),
            "source_speaker": item.get("speaker") or None,
            "source_excerpt": item["text"] or item["title"],
            "as_of": at if re.fullmatch(r"\d{4}-\d{2}-\d{2}", at) else None,
            # A Catch up route from the distill waits for the owner, even in Auto.
            "capture_origin": None if item.get("review") else "connector",
            # A plain reason: an Auto change records it as why the governor kept it.
            "reason": _flat(
                f"{write.get('reason') or 'It changed'} (from {item['connector_name']}, "
                "read by the heartbeat)",
                300,
            ),
        }
        if kind == "sheet":
            value = _flat(write.get("value"), 300)
            return self.ws.brain.propose(
                owner,
                statement=f"{write.get('column')}: {value}",
                destination="sheet",
                place={k: write.get(k) for k in ("sheet", "row", "column")} | {"value": value},
                flags=flags or None,
                **cited,
            )
        if kind == "progress":
            return self.ws.brain.propose(
                owner,
                statement=_flat(write.get("text"), 600),
                destination="progress",
                place={"project": write.get("project"), "next_up": write.get("next_up") or ""},
                **cited,
            )
        if kind == "task":
            # A due date the model wrote another way is left out, not refused.
            due = str(write.get("due") or "")
            return self.ws.brain.propose(
                owner,
                statement=_flat(write.get("text"), 300),
                destination="todo",
                place={
                    "project": write.get("project") or "",
                    "owner": write.get("owner") or "",
                    "due": due if re.fullmatch(r"\d{4}-\d{2}-\d{2}", due) else "",
                },
                flags=flags or None,
                **cited,
            )
        name = re.sub(r"[^a-z0-9]+", "-", str(write.get("topic") or "").lower()).strip("-")
        topic = f"{write.get('area')}/{name}" if name else None
        return self.ws.brain.propose(
            owner,
            statement=_flat(write.get("text"), 1200),
            kind="fact",
            topic=None if write.get("revises") else topic,
            revises=write.get("revises") or None,
            flags=flags or None,
            **cited,
        )

    def _write_all(
        self,
        owner: Principal,
        beat_id: str,
        items: list[dict[str, Any]],
        judged: dict[str, Any],
        brain: dict[str, Any],
        details: list[str],
    ) -> dict[str, int]:
        region_ids = {r["id"] for r in brain["regions"]}
        projects = {p["id"]: p for p in brain["projects"]}
        titles = {p["id"]: p["title"] for p in brain["projects"]}
        sheets = {s["id"] for s in brain["sheets"]}
        holds = {h["id"]: h for h in brain["must_holds"]}
        by_key = {i["key"]: i for i in items}
        suppressed = self._suppressed()
        counts = {"changes": 0, "waits": 0, "held": 0}
        touched: dict[str, set[str]] = {}  # region: projects that moved
        lines: list[dict[str, Any]] = []
        for verdict in judged.get("items") or []:
            if not isinstance(verdict, dict):
                continue
            item = by_key.get(str(verdict.get("item") or ""))
            if item is None:
                continue
            region = str(verdict.get("region") or "")
            if region not in region_ids:
                details.append(
                    f"{item['connector_name']}: {item['title'] or item['id']} fits no region; "
                    "nothing written."
                )
                continue
            project = str(verdict.get("project") or "") or None
            if project and project not in projects:
                project = None
            text = _flat(verdict.get("line") or item["title"] or item["text"], 300)
            protected = holds.get(str(verdict.get("protected_by") or "")) or None
            writes = [w for w in verdict.get("writes") or [] if isinstance(w, dict)]
            if not writes:
                lines.append(self._line(beat_id, item, region, project, text, {}, "noted", "", ""))
                touched.setdefault(region, set())
                continue
            for write in writes:
                kind = str(write.get("kind") or "")
                reason = _flat(write.get("reason"), 300)
                target = write.get("project") or project
                if kind in ("progress", "task"):
                    write = {**write, "project": target}
                fingerprint = _hash(
                    item["key"],
                    kind,
                    {
                        k: write.get(k)
                        for k in (
                            "sheet",
                            "row",
                            "column",
                            "value",
                            "project",
                            "area",
                            "topic",
                            "revises",
                        )
                    },
                    _flat(write.get("text") or write.get("value"), 600).lower(),
                )
                if fingerprint in suppressed:
                    details.append(f"Not written again (you undid it): {text}")
                    continue
                status, wrote, problem = self._check_write(
                    write, kind, region_ids, projects, sheets, holds, item, protected
                )
                if status == "refused":
                    lines.append(
                        self._line(
                            beat_id,
                            item,
                            region,
                            project,
                            text,
                            write,
                            "refused",
                            f"Not written: {problem}",
                            reason,
                            fingerprint,
                        )
                    )
                    details.append(f"{item['connector_name']}: not written, {problem}")
                    continue
                if status == "held":
                    counts["held"] += 1
                    lines.append(
                        self._line(
                            beat_id,
                            item,
                            region,
                            project,
                            text,
                            write,
                            "held",
                            problem,
                            reason,
                            fingerprint,
                        )
                    )
                    details.append(f"{item['connector_name']}: {problem}")
                    touched.setdefault(region, set())
                    continue
                label = self.wrote_label(write, titles)
                try:
                    made = self._propose(owner, item, write, text)
                except WorkspaceError as exc:
                    lines.append(
                        self._line(
                            beat_id,
                            item,
                            region,
                            project,
                            text,
                            write,
                            "refused",
                            f"Not written: {_flat(str(exc), 200)}",
                            reason,
                            fingerprint,
                        )
                    )
                    details.append(f"{item['connector_name']}: not written, {_flat(str(exc), 160)}")
                    continue
                must_change = kind == "note" and str(write.get("revises") or "") in holds
                if made.status == "applied" and not must_change:
                    counts["changes"] += 1
                    line = self._line(
                        beat_id,
                        item,
                        region,
                        project,
                        text,
                        write,
                        "applied",
                        f"Wrote: {label}",
                        reason,
                        fingerprint,
                        proposal=made.proposal_id or made.entry_id,
                        checkpoint=made.checkpoint_id,
                    )
                    details.append(f"{item['connector_name']}: {text} → {label}")
                elif made.status in ("pending", "applied"):
                    counts["waits"] += 1
                    why = ["a must-hold change"] if must_change else list(made.waits or [])
                    said = f"Waits for you ({', '.join(why)}): " if why else "Waits for you: "
                    line = self._line(
                        beat_id,
                        item,
                        region,
                        project,
                        text,
                        write,
                        "waiting",
                        said + label,
                        reason,
                        fingerprint,
                        proposal=made.proposal_id,
                        checkpoint=made.checkpoint_id,
                    )
                    details.append(f"Waits for you: {text} ({item['connector_name']})")
                elif made.status == "duplicate":
                    details.append(f"Already waiting: {text}")
                    continue
                else:
                    line = self._line(
                        beat_id,
                        item,
                        region,
                        project,
                        text,
                        write,
                        "refused",
                        f"Not written: {_flat(made.message, 200)}",
                        reason,
                        fingerprint,
                    )
                    details.append(
                        f"{item['connector_name']}: not written, {_flat(made.message, 160)}"
                    )
                lines.append(line)
                if line["status"] in ("applied", "waiting"):
                    touched.setdefault(region, set())
                    if target and target in projects:
                        touched[region].add(str(target))
        self._keep_lines(owner, beat_id, lines, judged, touched, projects)
        return counts

    def _check_write(
        self,
        write: dict[str, Any],
        kind: str,
        regions: set[str],
        projects: dict[str, dict[str, Any]],
        sheets: set[str],
        holds: dict[str, dict[str, Any]],
        item: dict[str, Any],
        protected: dict[str, Any] | None,
    ) -> tuple[str, str, str]:
        """(go | held | refused, wrote, why): code's check of one write before it is made."""
        if kind not in WRITE_KINDS:
            return "refused", "", f"a write of kind {kind!r} is not one Gnomon makes"
        target = str(write.get("project") or "")
        if kind in ("progress", "task") and target not in projects:
            return "refused", "", "out of scope: no project it belongs to"
        if kind == "sheet" and str(write.get("sheet") or "") not in sheets:
            return "refused", "", "out of scope: no such sheet"
        if (
            kind == "note"
            and not write.get("revises")
            and str(write.get("area") or "") not in regions
        ):
            return "refused", "", "out of scope: no such region"
        if kind == "note" and write.get("revises") and str(write.get("revises")) in holds:
            return "go", "", ""  # a must-hold change: it goes in as a proposal that waits
        if target and str(projects.get(target, {}).get("status") or "") == "sealed":
            return "held", "", f"Held back: sealed project {projects[target]['title']}"
        if item.get("private"):
            return "held", "", "Held back: a private item"
        words = " ".join(str(write.get(k) or "") for k in ("value", "text")) + " " + item["text"]
        hold = protected or self.protecting(words)
        if hold:
            said = hold.get("line") or hold.get("text") or hold.get("title") or hold.get("id")
            return "held", "", f"Held back: must-hold: {_flat(said, 160)}"
        return "go", "", ""

    def _line(
        self,
        beat_id: str,
        item: dict[str, Any],
        region: str,
        project: str | None,
        text: str,
        write: dict[str, Any],
        status: str,
        wrote: str,
        reason: str,
        fingerprint: str = "",
        *,
        proposal: str | None = None,
        checkpoint: str | None = None,
    ) -> dict[str, Any]:
        return {
            "beat_id": beat_id,
            "region": region,
            "project": project,
            "text": text,
            "connector_id": item["connector_id"],
            "cli": item["cli"],
            "item_id": item["key"],
            "item_at": item.get("at") or None,
            "wrote": wrote,
            "write": write,
            "reason": reason,
            "status": status,
            "proposal_id": proposal,
            "checkpoint_id": checkpoint,
            "fingerprint": fingerprint or _hash(item["key"], text),
        }

    def _keep_lines(
        self,
        owner: Principal,
        beat_id: str,
        lines: list[dict[str, Any]],
        judged: dict[str, Any],
        touched: dict[str, set[str]],
        projects: dict[str, dict[str, Any]],
    ) -> None:
        said_heads, said_moved = judged.get("headlines"), judged.get("projects")
        headlines: dict[str, Any] = said_heads if isinstance(said_heads, dict) else {}
        moved: dict[str, Any] = said_moved if isinstance(said_moved, dict) else {}
        with self.ws.rt.write() as tx:
            for line in lines:
                line_id = tx.new_id("hbl")
                if line["status"] not in states.HEARTBEAT_LINE.states:
                    raise ValueError(line["status"])
                tx.insert(
                    schema.heartbeat_lines,
                    {
                        "id": line_id,
                        "workspace_id": tx.workspace_id,
                        **line,
                        "created_by": owner.actor_id,
                        "created_at": tx.now,
                        "updated_at": tx.now,
                    },
                )
                tx.emit(
                    "heartbeat.line_written",
                    actor_id=owner.actor_id,
                    subject_type="heartbeat_line",
                    subject_id=line_id,
                    data={
                        "beat_id": beat_id,
                        "region": line["region"],
                        "status": line["status"],
                        "source_id": line["item_id"],
                        "proposal_id": line["proposal_id"],
                        "checkpoint_id": line["checkpoint_id"],
                    },
                )
            # The headline is written again only where a line changed.
            for region, moved_projects in touched.items():
                row = tx.get(schema.heartbeat_regions, region)
                headline = _flat(headlines.get(region), 200)
                statuses = dict((row or {}).get("projects") or {})
                for pid in moved_projects:
                    said = _flat(moved.get(pid), 160)
                    if said:
                        statuses[pid] = {"status": said, "at": tx.now, "beat_id": beat_id}
                if row is None:
                    tx.insert(
                        schema.heartbeat_regions,
                        {
                            "id": region,
                            "workspace_id": tx.workspace_id,
                            "headline": headline,
                            "headline_at": tx.now if headline else None,
                            "projects": statuses,
                            "created_by": owner.actor_id,
                            "created_at": tx.now,
                            "updated_at": tx.now,
                        },
                    )
                else:
                    values: dict[str, Any] = {"projects": statuses}
                    if headline:
                        values.update(headline=headline, headline_at=tx.now)
                    tx.update(schema.heartbeat_regions, row, values)

    # ---------------------------------------------------------------- regions and undo

    def _line_view(
        self, row: dict[str, Any], names: dict[str, dict[str, Any]], latest: str | None
    ) -> dict[str, Any]:
        connector = names.get(row["connector_id"]) or {}
        held = row["status"] == "held"
        return {
            "id": row["id"],
            "beat_id": row["beat_id"],
            "region": row["region"],
            "project": row["project"],
            "text": row["text"],
            "connector_id": row["connector_id"],
            "connector": connector.get("name") or row["connector_id"],
            "how": connector.get("how_label") or "",
            "cli": row["cli"],
            "cli_label": CLI_LABELS.get(row["cli"], row["cli"]),
            "at": row["item_at"] or row["created_at"],
            "written_at": row["created_at"],
            "wrote": row["wrote"],
            "write": row["write"],
            "reason": row["reason"],
            "status": row["status"],
            "held": held,
            "new_this_beat": latest is not None and row["beat_id"] == latest,
            "proposal_id": row["proposal_id"],
            "checkpoint_id": row["checkpoint_id"],
            "source_id": row["item_id"],
            "can_undo": row["status"] in ("applied", "waiting"),
        }

    def regions(self, who: Principal) -> list[dict[str, Any]]:
        """Live status per region: headline, lines (text, connector, CLI, time, what it
        wrote), projects with their status."""
        who.require("brain.read")
        brain = self._brain()
        names = {c["id"]: c for c in (self._connector_view(r) for r in self._connectors())}
        kept = self._regions_now()
        last = next((b for b in self._beats(limit=10) if b["status"] == "done"), None)
        latest = last["id"] if last else None
        with self.ws.rt.read() as tx:
            rows = tx.rows(
                sa.select(schema.heartbeat_lines)
                .where(schema.heartbeat_lines.c.workspace_id == tx.workspace_id)
                .order_by(
                    schema.heartbeat_lines.c.created_at.desc(), schema.heartbeat_lines.c.id.desc()
                )
                .limit(500)
            )
        projects = {p["id"]: p for p in brain["projects"]}
        out = []
        for region in brain["regions"]:
            # A write the Brain refused stays in its beat's record, not in the region's lines.
            mine = [
                self._line_view(r, names, latest)
                for r in rows
                if r["region"] == region["id"] and r["status"] != "refused"
            ]
            row = kept.get(region["id"]) or {}
            statuses = dict(row.get("projects") or {})
            listed = []
            for pid, said in statuses.items():
                project = projects.get(pid)
                if project is None or project["status"] == "sealed":
                    continue
                sources = sorted({x["connector"] for x in mine if x["project"] == pid})
                listed.append(
                    {
                        "id": pid,
                        "title": project["title"],
                        "status": said.get("status") or "",
                        "at": said.get("at"),
                        "from": sources[:2],
                    }
                )
            out.append(
                {
                    "id": region["id"],
                    "name": region["name"],
                    "headline": row.get("headline") or "",
                    "headline_at": row.get("headline_at"),
                    "updated_at": mine[0]["written_at"] if mine else None,
                    "lines": mine[:50],
                    # Items, not writes: one item that moved two homes is one new line.
                    "new_this_beat": len(
                        {
                            (x["beat_id"], x["source_id"])
                            for x in mine
                            if x["new_this_beat"] and x["status"] != "undone"
                        }
                    ),
                    "projects": listed,
                }
            )
        return out

    def undo(self, who: Principal, line_id: str) -> dict[str, Any]:
        """Undo one line: its write is undone the way any undo is (an undo point taken back, or
        a waiting proposal turned down), the line is marked undone, and the next beat will not
        write it again."""
        who.require("brain.govern")
        with self.ws.rt.read() as tx:
            row = tx.get(schema.heartbeat_lines, line_id)
        if row is None or row["workspace_id"] != self.ws.rt.workspace_id:
            raise NotFound(f"No line {line_id}.", id=line_id)
        states.HEARTBEAT_LINE.check(row["status"], "undone")
        how = "noted"
        if row["status"] == "applied" and row["checkpoint_id"]:
            self.ws.brain.undo(who, row["checkpoint_id"])
            how = "undone"
        elif row["status"] in ("waiting", "applied") and row["proposal_id"]:
            try:
                self.ws.brain.decide(
                    who, row["proposal_id"], approve=False, reason="Undone from the heartbeat."
                )
                how = "turned down"
            except NotFound as exc:
                raise Conflict(
                    "That change was already decided in Catch up: undo it from Undo points."
                ) from exc
        with self.ws.rt.write() as tx:
            current = tx.one(schema.heartbeat_lines, line_id, "line")
            states.HEARTBEAT_LINE.check(current["status"], "undone")
            tx.update(schema.heartbeat_lines, current, {"status": "undone"})
            if (
                tx.first(
                    sa.select(schema.heartbeat_suppressions.c.id).where(
                        schema.heartbeat_suppressions.c.fingerprint == current["fingerprint"]
                    )
                )
                is None
            ):
                tx.insert(
                    schema.heartbeat_suppressions,
                    {
                        "id": tx.new_id("hbx"),
                        "workspace_id": tx.workspace_id,
                        "fingerprint": current["fingerprint"],
                        "line_id": line_id,
                        "created_at": tx.now,
                    },
                )
            tx.emit(
                "heartbeat.line_undone",
                actor_id=who.actor_id,
                subject_type="heartbeat_line",
                subject_id=line_id,
                data={
                    "how": how,
                    "proposal_id": current["proposal_id"],
                    "checkpoint_id": current["checkpoint_id"],
                },
            )
        names = {c["id"]: c for c in (self._connector_view(r) for r in self._connectors())}
        with self.ws.rt.read() as tx:
            done = tx.one(schema.heartbeat_lines, line_id, "line")
        out = self._line_view(done, names, None)
        out["line"] = "Undone. The next beat won't write that again."
        return out


def sign_in(row: dict[str, Any]) -> dict[str, Any] | None:
    """How to sign a connector in again, by where it comes from (v4 backend plan, 17.2), or
    None when it needs no sign-in. The same for every connector of a source: no connector is
    named in the code."""
    if row.get("status") != "unhealthy_sign_in":
        return None
    name = str(row.get("name") or "").removeprefix("claude.ai ").strip() or "it"
    cli = row.get("cli")
    if row.get("found_in") == ACCOUNT_PLACE:
        return {
            "steps": f"Open claude.ai, Settings, Connectors, and connect {name} again. Then "
            "choose Check again here.",
            "url": "https://claude.ai/settings/connectors",
        }
    if row.get("how") == "n8n" or cli == "n8n":
        return {
            "steps": f"Open n8n, Credentials, and reconnect the credential {name} uses. Then "
            "choose Check again here.",
            "url": None,
        }
    if cli == "codex":
        command = f"codex mcp login {name}"
    elif cli == "gemini":
        command = "gemini, then /auth"
    else:
        command = f"claude, then /mcp, choose {name} and Authenticate"
    return {
        "steps": f"In a terminal, run {command}. Then choose Check again here.",
        "url": None,
    }


def kept_state(row: dict[str, Any], status: str, line: str) -> tuple[str, str]:
    """A connector's state after a rescan. An account connector that has been read before
    and only did not load during this rescan (they load in the background) keeps its last
    state, so it doesn't flap to an error between good reads."""
    if (
        status in ("error", "connecting")
        and row.get("found_in") == ACCOUNT_PLACE
        and row.get("last_read_at")
        and row.get("status") in ("ok", "off")
    ):
        return str(row["status"]), str(row.get("status_line") or line)
    if status != "ok":
        return status, line
    return ("ok" if row.get("enabled") else "off"), line


class ConnectorProblem(Exception):
    def __init__(
        self,
        status: str,
        line: str,
        *,
        present: bool | None = None,
        tools: list[str] | None = None,
    ) -> None:
        super().__init__(line)
        self.status = status
        self.line = line
        self.present = present
        self.tools = tools


class HeartbeatWorker:
    """Runs the beats that are due while the computer is on (and the server with it)."""

    def __init__(self, heartbeat: Heartbeat, interval: float = 30.0) -> None:
        self.heartbeat = heartbeat
        self.interval = max(1.0, float(interval))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._looked = False  # whether it looked for connectors since the server started

    def run_once(self) -> dict[str, Any] | None:
        owner = self.heartbeat.ws.owner()
        if not self._looked and not self.heartbeat.connectors(owner):
            # A new Brain: find the connectors in the CLIs once, as "Find connectors" would.
            self._looked = True
            self.heartbeat.rescan(owner)
        self._looked = True
        if self.heartbeat.distill_due():
            return self.heartbeat.beat(owner, "distill")
        if not self.heartbeat.due():
            return None
        if not any(c["enabled"] for c in self.heartbeat.connectors(owner)):
            return None
        return self.heartbeat.beat(self.heartbeat.ws.owner(), "schedule")

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except UNREADABLE:  # the Brain or the CLI cannot say now
                log.warning("a scheduled beat could not run", exc_info=True)
            self._stop.wait(self.interval)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
