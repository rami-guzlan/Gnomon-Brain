"""The heartbeat: connectors found in the CLIs, read-only reads, the beat, regions and undo."""

import inspect
import io
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from workspace_core.db import schema
from workspace_core.ports import BrainProposal
from workspace_server import runners as runners_mod
from workspace_server.app import create_app
from workspace_server.heartbeat import connectors as conn
from workspace_server.heartbeat import memory as hb_memory
from workspace_server.heartbeat import service as hb_service
from workspace_server.heartbeat import service as heartbeat_service
from workspace_server.runners import Runners, RunnerStatus

from .conftest import Api

FAKE_SERVER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_mcp_server.py")
FAKE_CLI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_cli.py")
USAGE = {
    "input_tokens": 100,
    "output_tokens": 20,
    "cache_read_input_tokens": 0,
    "cache_creation_input_tokens": 0,
}
PROJECT = "state-focus-website-redesign"
ITEM = {
    "id": "msg-1",
    "title": "Morgan approved hero option B",
    "text": "Morgan replied: hero option B is approved for the website.",
    "at": "2026-09-28T12:40:00Z",
    "url": "",
    "speaker": "Morgan",
    "private": False,
}


PACKET = {
    "id": "role",
    "title": "Runs the website redesign",
    "text": "Leads the website redesign; hero option B is the one they chose.",
    "at": "2026-09-28T12:40:00Z",
    "speaker": "the person",
    "private": False,
}


class FakeStream:
    def __init__(self, proc):
        self.proc = proc

    def __iter__(self):
        return self.proc.streamed()


class FakeProc:
    def __init__(self, cli, argv, kwargs):
        self.cli, self.argv, self.kwargs = cli, list(argv), kwargs
        self.returncode = 0
        self.pid = 0
        cli.started.append({"argv": self.argv, "env": dict(kwargs.get("env") or {})})
        if "stream-json" in self.argv:  # Claude Code with the account's connectors
            self.stdin = io.StringIO()
            self.stdin.close = lambda: None  # keep what was written readable
            self.stdout = FakeStream(self)

    def streamed(self):
        """What Claude Code writes, line by line: its init message, then for a read (once the
        prompt is in) its result."""
        prompt = self.stdin.getvalue()
        reading = prompt.startswith("Read, read-only")
        if reading:
            self.cli.calls.append({"argv": self.argv, "prompt": prompt, "config": None})
        else:
            self.cli.listings.append(self.argv)
        yield json.dumps(self.cli.account_init) + "\n"
        if not reading:
            return
        answer = self.cli.read_answer(self.argv)
        yield json.dumps({"type": "result", "result": json.dumps(answer), "usage": USAGE}) + "\n"

    def poll(self):
        return 0

    def communicate(self, prompt=None, timeout=None):
        argv = self.argv
        if argv[-2:] == ["mcp", "list"]:
            return self.cli.account_list, ""
        config = None
        if "--mcp-config" in argv:
            with open(argv[argv.index("--mcp-config") + 1], encoding="utf-8") as handle:
                config = json.load(handle)
        self.cli.calls.append({"argv": argv, "prompt": prompt, "config": config})
        if "context packets" in str(prompt or ""):  # a memory read
            self.cli.memory_reads.append({"argv": argv, "prompt": prompt})
            answer = json.dumps({"packets": self.cli.packets})
            if "exec" in argv:
                with open(argv[argv.index("--output-last-message") + 1], "w") as handle:
                    handle.write(answer)
                return "", ""
            return json.dumps({"result": answer, "usage": USAGE}), ""
        if "exec" in argv:  # Codex
            last = argv[argv.index("--output-last-message") + 1]
            with open(last, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"cursor": "c1", "items": self.cli.items}))
            usage = {"type": "turn.completed", "usage": {"input_tokens": 50, "output_tokens": 5}}
            return json.dumps(usage) + "\n", ""
        if (config or {}).get("mcpServers") or str(prompt or "").startswith("Read, read-only"):
            if self.cli.read_error:
                return json.dumps({"is_error": True, "result": self.cli.read_error}), ""
            answer = self.cli.read_answer(argv)
            return json.dumps({"result": json.dumps(answer), "usage": USAGE}), ""
        return json.dumps({"result": json.dumps(self.cli.judge(prompt)), "usage": USAGE}), ""


class FakeCli:
    """Claude Code and Codex as the heartbeat starts them: what each call was given, and the
    answers a read and the judge give."""

    def __init__(self):
        self.calls = []
        self.listings = []  # Claude Code started to list the account's connectors
        self.started = []  # every start, with its environment
        self.account_init = {"type": "system", "subtype": "init", "mcp_servers": [], "tools": []}
        self.account_list = "No MCP servers configured.\n"  # `claude mcp list`
        self.items = [dict(ITEM)]
        self.items_by = None  # {server: [items]} when a read covers more than one
        self.distilled = []  # what each distill call was shown
        self.route = lambda item: ("short", 0.9)  # the distill's route and confidence
        self.read_error = None
        self.memory_reads = []
        self.packets = [dict(PACKET)]
        self.writes = lambda item: [
            {
                "kind": "progress",
                "project": PROJECT,
                "text": item["text"],
                "reason": "Morgan approved it by email.",
            }
        ]

    def read_answer(self, argv):
        """A read of every server the run may use: the first gets the items, unless
        `items_by` names each server's."""
        allowed = argv[argv.index("--allowedTools") + 1 :] if "--allowedTools" in argv else []
        servers = []
        for name in allowed:
            if not name.startswith("mcp__"):
                break
            server = name.split("__")[1]
            if server not in servers:
                servers.append(server)
        if "exec" in argv:
            servers = servers or ["tracker"]
        by = self.items_by or {servers[0] if servers else "": self.items}
        return {
            "connectors": {
                s: {"cursor": "2026-09-28T12:40:00Z", "items": by.get(s, [])} for s in servers
            }
        }

    def judge(self, prompt):
        shown = json.loads(prompt.split("\n\n", 1)[1])
        if prompt.startswith("You distill a day of changes"):
            self.distilled.append(shown["items"])
            return {
                "items": [
                    {
                        "item": i["item"],
                        "route": self.route(i)[0],
                        "confidence": self.route(i)[1],
                        "reason": "it matters to the goal",
                        "region": "state",
                        "project": PROJECT,
                        "line": i["title"],
                        "protected_by": None,
                        "writes": self.writes({"text": i["now"]}),
                    }
                    for i in shown["items"]
                ],
                "patterns": [],
                "headlines": {"state": "The website hero is settled."},
                "projects": {PROJECT: "Hero option B approved"},
            }
        return {
            "items": [
                {
                    "item": i["item"],
                    "region": "state",
                    "project": PROJECT,
                    "line": i["title"],
                    "protected_by": None,
                    "writes": self.writes(i),
                }
                for i in shown["items"]
            ],
            "headlines": {"state": "The website hero is settled."},
            "projects": {PROJECT: "Hero option B approved"},
        }

    def __call__(self, argv, **kwargs):
        return FakeProc(self, argv, kwargs)

    def reads(self):
        return [
            c
            for c in self.calls
            if "context packets" in str(c["prompt"] or "")
            or (c["config"] or {}).get("mcpServers")
            or "exec" in c["argv"]
            or str(c["prompt"] or "").startswith("Read, read-only")
        ]

    def judges(self):
        return [c for c in self.calls if c not in self.reads()]


def signed_in(spec, refresh=False):
    return RunnerStatus(
        id=spec.id,
        label=spec.label,
        provider=spec.provider,
        one_click=True,
        installed=True,
        version="9.9.9",
        signed_in=True,
        install="",
        sign_in=f"{spec.executable} login",
    )


@pytest.fixture
def cli_home(tmp_path, monkeypatch):
    """The CLIs' own settings in a scratch folder: Claude Code with a mail server that would
    allow a send (and a token in its environment), Gnomon's own Brain server, a computer-use
    server in a project; Codex with a tracker; and a repository's .mcp.json, never read."""
    claude, codex = tmp_path / "claude", tmp_path / "codex"
    claude.mkdir()
    codex.mkdir()
    calls = tmp_path / "tool-calls.txt"
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude))
    monkeypatch.setenv("CODEX_HOME", str(codex))
    monkeypatch.setenv("GEMINI_CLI_HOME", str(tmp_path / "gemini"))  # no Gemini CLI here
    monkeypatch.setenv("FAKE_MCP_CALLS", str(calls))
    server = {
        "type": "stdio",
        "command": sys.executable,
        "args": [FAKE_SERVER],
        "env": {"MAIL_TOKEN": "secret-mail-token-123"},
    }
    (claude / ".claude.json").write_text(
        json.dumps(
            {
                "mcpServers": {"mail": server, "contxt": {"command": "uv", "args": []}},
                "projects": {
                    str(tmp_path / "repo"): {
                        "mcpServers": {"computer-use": {"command": "computer-use-mcp"}}
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    forward = sys.executable.replace("\\", "/")
    (codex / "config.toml").write_text(
        f'[mcp_servers.tracker]\ncommand = "{forward}"\n'
        f'args = ["{FAKE_SERVER.replace(chr(92), "/")}"]\n',
        encoding="utf-8",
    )
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"repo-only": {"command": "x"}}}), encoding="utf-8"
    )
    monkeypatch.chdir(repo)
    return {"calls": calls}


@pytest.fixture
def fake_cli():
    return FakeCli()


@pytest.fixture
def client(runtime, cli_home, fake_cli):
    runtime.runners = Runners(
        runtime.ws,
        runtime.templates,
        base_url="http://127.0.0.1:8787",
        resolve=lambda spec: [sys.executable, FAKE_CLI],
        popen=fake_cli,
    )
    runtime.runners.status = signed_in
    with TestClient(create_app(runtime=runtime)) as test_client:
        yield test_client


@pytest.fixture
def hb(runtime, client):
    return runtime.heartbeat


def found(owner, *, codex=False, per_beat=True):
    """Find connectors in the CLIs; the Codex tracker stays off unless a test reads it. The
    mail server is read every beat here (a new one is read daily), so a test can beat
    twice."""
    result = owner.ok(owner.post("/api/heartbeat/connectors/rescan"))
    if not codex:
        owner.ok(owner.put("/api/heartbeat/connectors/codex:tracker", json={"enabled": False}))
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"cadence_minutes": 0}))
    if per_beat:  # these tests judge in the beat; the nightly distill has its own tests
        owner.ok(owner.put("/api/heartbeat/learning", json={"learn": "per_beat"}))
    return result


def beat(owner):
    return owner.ok(owner.post("/api/heartbeat/beat?wait=true"))


def rows(runtime, table):
    with runtime.ws.rt.read() as tx:
        return tx.rows(table.select())


# ---------------------------------------------------------------------- discovery


def test_connectors_are_found_in_each_cli_and_no_secret_is_kept(owner, runtime, cli_home):
    result = found(owner, codex=True)
    by_id = {c["id"]: c for c in result["connectors"]}
    assert set(by_id) == {
        "claude:mail",
        "claude:computer-use",
        "codex:tracker",
        "scout",
        "memory:claude",  # what each CLI keeps on the person: off until onboarding
        "memory:codex",
    }
    assert not by_id["memory:claude"]["enabled"] and by_id["memory:codex"]["how"] == "memory"
    mail = by_id["claude:mail"]
    assert mail["status"] == "ok" and mail["enabled"]
    assert mail["tools"] == ["list_messages", "get_thread"]
    assert by_id["claude:computer-use"]["how"] == "computer"
    assert not by_id["claude:computer-use"]["enabled"]
    assert "computer use" in by_id["claude:computer-use"]["status_line"]
    assert by_id["scout"]["status"] == "off"
    assert "Found 3 connectors" not in result["line"]
    stored = json.dumps(rows(runtime, schema.heartbeat_connectors), default=str)
    assert "secret-mail-token-123" not in stored
    assert "secret-mail-token-123" not in json.dumps(result)
    assert not cli_home["calls"].exists()  # listing its tools never called one


def test_only_tools_that_read_are_kept():
    assert conn.read_only({"name": "list_messages"})
    assert conn.read_only({"name": "searchIssues"})
    assert conn.read_only({"name": "slack_list_channels"}, "slack")
    assert conn.read_only({"name": "anything", "annotations": {"readOnlyHint": True}})
    assert not conn.read_only({"name": "send_message"})
    assert not conn.read_only({"name": "get_and_delete"})
    assert not conn.read_only({"name": "delete_all", "annotations": {"readOnlyHint": True}})
    assert not conn.read_only({"name": "list_items", "annotations": {"readOnlyHint": False}})
    assert not conn.read_only({"name": "draft_reply"})


def test_a_server_whose_tools_cannot_be_listed_is_not_read(owner, hb, cli_home):
    def broken(found):
        raise conn.ListingFailed("it did not answer in time")

    hb.list_tools = broken
    by_id = {c["id"]: c for c in found(owner)["connectors"]}
    assert by_id["claude:mail"]["status"] == "cannot_confirm_read_only"
    assert not by_id["claude:mail"]["enabled"]
    refused = owner.put("/api/heartbeat/connectors/claude:mail", json={"enabled": True})
    assert refused.status_code == 409


# ---------------------------------------------------------------------- read-only


def test_a_beat_reads_on_the_ingest_model_not_the_accounts_default(owner, fake_cli, runtime):
    # Unnamed, Claude Code read on the account's default (Opus): about 30 times Haiku's cost.
    found(owner)
    beat(owner)
    (read,) = fake_cli.reads()
    assert read["argv"][read["argv"].index("--model") + 1] == "haiku"


def test_a_beat_only_reads_with_a_connector_that_would_allow_a_send(owner, hb, fake_cli, cli_home):
    found(owner)
    done = beat(owner)
    assert done["status"] == "done", done
    (read,) = fake_cli.reads()
    argv = read["argv"]
    allowed = argv[argv.index("--allowedTools") + 1 : argv.index("--disallowedTools")]
    assert allowed == ["mcp__mail__list_messages", "mcp__mail__get_thread"]
    refused = argv[argv.index("--disallowedTools") + 1 : argv.index("--permission-mode")]
    assert "mcp__mail__send_message" in refused and "mcp__mail__delete_all" in refused
    assert not any("send_message" in a for a in allowed)
    assert argv[argv.index("--tools") + 1] == ""
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    assert "--strict-mcp-config" in argv
    assert list(read["config"]["mcpServers"]) == ["mail"]
    (judge,) = fake_cli.judges()
    assert judge["config"] == {"mcpServers": {}}  # the judge has no connector at all
    assert not cli_home["calls"].exists()
    # No code path of the heartbeat can call a tool itself.
    for module in (conn, hb_service):
        assert "tools/call" not in inspect.getsource(module)


def test_codex_reads_with_only_the_read_only_tools(owner, hb, fake_cli):
    found(owner, codex=True)
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"enabled": False}))
    beat(owner)
    (read,) = fake_cli.reads()
    argv = read["argv"]
    limit = argv[argv.index("-c") + 1]
    assert limit == 'mcp_servers.tracker.enabled_tools=["list_messages", "get_thread"]'
    assert "send_message" not in " ".join(argv)
    assert argv[argv.index("--sandbox") + 1] == "read-only"


# ---------------------------------------------------------------------- the beat


def test_a_beat_writes_through_governance_with_the_item_as_source(owner, runtime, fake_cli):
    found(owner)
    done = beat(owner)
    assert done["items"] == 1
    assert done["changes"] + done["waits"] == 1
    assert done["tokens"] == 240 and done["tokens_known"]  # the read and the judge, as said
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["item_id"] == "connector:claude:mail:msg-1"
    assert line["status"] in ("applied", "waiting")
    assert line["proposal_id"]
    waiting = [p for p in runtime.ws.brain.proposals(runtime.ws.owner())]
    cited = [p for p in waiting if "connector:claude:mail:msg-1" in json.dumps(p, default=str)]
    assert cited, waiting
    regions = {r["id"]: r for r in owner.ok(owner.get("/api/heartbeat/regions"))}
    state = regions["state"]
    assert state["headline"] == "The website hero is settled."
    assert state["new_this_beat"] == 1
    (shown,) = state["lines"]
    assert shown["connector"] == "mail" and shown["cli_label"] == "Claude Code"
    assert shown["wrote"].endswith("Website Redesign › latest progress")
    assert state["projects"][0]["status"] == "Hero option B approved"
    assert state["projects"][0]["from"] == ["mail"]


def test_the_same_items_twice_make_no_new_writes(owner, runtime, fake_cli):
    found(owner)
    beat(owner)
    proposals = len(runtime.ws.brain.proposals(runtime.ws.owner()))
    lines = len(rows(runtime, schema.heartbeat_lines))
    (mail,) = [r for r in rows(runtime, schema.heartbeat_connectors) if r["id"] == "claude:mail"]
    assert mail["cursor"] == "2026-09-28T12:40:00Z"
    again = beat(owner)
    assert again["items"] == 0 and again["changes"] == 0 and again["waits"] == 0
    assert again["nothing_new"]
    assert len(fake_cli.judges()) == 1  # nothing new: no model call
    assert len(rows(runtime, schema.heartbeat_lines)) == lines
    assert len(runtime.ws.brain.proposals(runtime.ws.owner())) == proposals
    assert len(rows(runtime, schema.heartbeat_beats)) == 2  # only its record


def test_a_beat_with_nothing_new_writes_only_its_record(owner, runtime, fake_cli):
    found(owner)
    fake_cli.items = []
    done = beat(owner)
    assert done["nothing_new"] and done["details"] == ["Read 1 connectors. Nothing new."]
    assert rows(runtime, schema.heartbeat_lines) == []
    assert rows(runtime, schema.heartbeat_regions) == []
    assert fake_cli.judges() == []


def test_each_beat_commits_the_context_sheets(owner, runtime, fake_cli, monkeypatch):
    found(owner)
    fake_cli.items = []
    committed = []
    monkeypatch.setattr(
        runtime.ws.rt.brain, "commit_sheets", lambda message: committed.append(message) or {}
    )
    beat(owner)
    assert len(committed) == 1 and committed[0].startswith("Sheets after beat")


def test_one_beat_at_a_time_and_a_stale_lock_expires(owner, runtime, clock):
    found(owner)
    with runtime.ws.rt.write() as tx:
        tx.insert(
            schema.heartbeat_locks,
            {
                "id": tx.workspace_id,
                "beat_id": "beat_other",
                "expires_at": (clock.now() + timedelta(minutes=5)).isoformat(),
            },
        )
    assert beat(owner)["skipped"] == "busy"
    assert rows(runtime, schema.heartbeat_beats) == []
    clock.advance(timedelta(minutes=10))  # the other beat stopped: its lock ran out
    assert beat(owner)["status"] == "done"
    assert rows(runtime, schema.heartbeat_locks) == []  # released


def test_pause_and_quiet_hours_skip(owner, runtime, hb, clock):
    found(owner)
    owner.ok(owner.put("/api/heartbeat/settings", json={"paused": True}))
    assert beat(owner)["skipped"] == "paused"
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert status["state"] == "paused" and status["next_beat_at"] is None
    owner.ok(owner.put("/api/heartbeat/settings", json={"paused": False, "quiet_hours": "night"}))
    clock.set(datetime(2026, 9, 29, 3, 0, tzinfo=UTC))  # 11pm in New York
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert status["state"] == "quiet" and status["in_quiet_hours"]
    assert status["next_beat_at"].startswith("2026-09-29T11:00")  # 7am there
    assert hb.start(runtime.ws.owner(), "schedule")["skipped"] == "quiet"
    assert hb_service.HeartbeatWorker(hb).run_once() is None
    assert rows(runtime, schema.heartbeat_beats) == []
    assert beat(owner)["status"] == "done"  # Beat now still beats


def test_a_scheduled_beat_runs_when_due(owner, runtime, hb, clock):
    found(owner)
    worker = hb_service.HeartbeatWorker(hb)
    assert worker.run_once()["status"] == "done"
    assert worker.run_once() is None  # not due again yet
    clock.advance(timedelta(minutes=15))
    assert worker.run_once()["trigger"] == "schedule"


# ---------------------------------------------------------------------- undo


def test_an_undone_line_is_not_written_again(owner, runtime, fake_cli):
    found(owner)
    beat(owner)
    (line,) = rows(runtime, schema.heartbeat_lines)
    undone = owner.ok(owner.post(f"/api/heartbeat/lines/{line['id']}/undo"))
    assert undone["status"] == "undone"
    assert "won't write that again" in undone["line"]
    waiting = runtime.ws.brain.proposals(runtime.ws.owner())
    left = [p for p in waiting if p.get("id") == line["proposal_id"]]
    assert left == []  # the waiting change was turned down
    # The item changes a little; the judge would make the same write again.
    fake_cli.items = [dict(ITEM, title="Morgan approved hero option B (edited)")]
    fake_cli.writes = lambda item: [
        {"kind": "progress", "project": PROJECT, "text": ITEM["text"], "reason": "again"}
    ]
    again = beat(owner)
    assert again["items"] == 1 and again["changes"] == 0 and again["waits"] == 0
    assert any("you undid it" in d for d in again["details"])
    assert [r["status"] for r in rows(runtime, schema.heartbeat_lines)] == ["undone"]
    assert owner.post(f"/api/heartbeat/lines/{line['id']}/undo").status_code == 409


def test_undoing_a_line_that_went_in_takes_its_undo_point_back(owner, runtime, hb, monkeypatch):
    found(owner)
    monkeypatch.setattr(
        hb,
        "_propose",
        lambda *a: BrainProposal(
            status="applied", message="in", proposal_id="p-1", checkpoint_id="cp-1"
        ),
    )
    taken = []
    monkeypatch.setattr(
        runtime.ws.brain, "undo", lambda who, cp, together=False: taken.append(cp) or {}
    )
    done = beat(owner)
    assert done["changes"] == 1 and done["waits"] == 0
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["status"] == "applied" and line["checkpoint_id"] == "cp-1"
    owner.ok(owner.post(f"/api/heartbeat/lines/{line['id']}/undo"))
    assert taken == ["cp-1"]


# ---------------------------------------------------------------------- what is held back


def test_an_item_a_must_hold_protects_is_held_back(owner, runtime, hb, fake_cli, monkeypatch):
    found(owner)
    monkeypatch.setattr(
        hb,
        "protecting",
        lambda text: (
            {"id": "rules-pricing", "line": "Never share pricing until it is final."}
            if "pricing" in text.lower()
            else None
        ),
    )
    fake_cli.items = [dict(ITEM, id="sheet-4", title="Pricing sheet v4", text="Draft pricing v4")]
    done = beat(owner)
    assert done["held"] == 1 and done["changes"] == 0 and done["waits"] == 0
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["status"] == "held"
    assert line["wrote"] == "Held back: must-hold: Never share pricing until it is final."
    assert line["proposal_id"] is None


def test_a_private_item_is_held_back(owner, runtime, fake_cli):
    found(owner)
    fake_cli.items = [dict(ITEM, private=True)]
    done = beat(owner)
    assert done["held"] == 1
    assert rows(runtime, schema.heartbeat_lines)[0]["wrote"] == "Held back: a private item"


def test_a_write_out_of_scope_is_refused(owner, runtime, fake_cli):
    found(owner)
    fake_cli.writes = lambda item: [{"kind": "send", "text": "Reply to Morgan"}]
    done = beat(owner)
    assert done["changes"] == done["waits"] == 0
    assert rows(runtime, schema.heartbeat_lines)[0]["status"] == "refused"


# ---------------------------------------------------------------------- backing off


def test_a_cli_signed_out_skips_the_beat_and_says_so(owner, runtime, hb):
    found(owner)

    def signed_out(spec, refresh=False):
        return RunnerStatus(**{**signed_in(spec).__dict__, "signed_in": False})

    runtime.runners.status = signed_out
    assert beat(owner)["skipped"] == "unhealthy_sign_in"
    assert owner.ok(owner.get("/api/heartbeat/status"))["state"] == "unhealthy_sign_in"


def test_a_provider_limit_backs_off_and_says_so(owner, runtime, hb, fake_cli):
    found(owner)
    fake_cli.read_error = "API Error: 429 rate limit reached for this organization"
    done = beat(owner)
    assert done["status"] == "done"
    assert any("429" in e for e in done["errors"])
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert status["state"] == "provider_limit"
    mail = next(c for c in status["connectors"] if c["id"] == "claude:mail")
    assert mail["status"] == "provider_limit"
    assert hb.start(runtime.ws.owner(), "schedule")["skipped"] == "provider_limit"


# ---------------------------------------------------------------------- settings and MCP


def test_settings_are_checked(owner):
    owner.ok(owner.put("/api/heartbeat/settings", json={"cadence": 30, "quiet_hours": "off"}))
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert status["cadence"] == 30 and status["quiet_hours"] == "off"
    assert owner.put("/api/heartbeat/settings", json={"cadence": 7}).status_code == 422
    assert owner.put("/api/heartbeat/settings", json={"quiet_hours": "lunch"}).status_code == 422


def test_the_app_mcp_tools_and_their_confirmation(model, owner):
    found(owner)
    names = {t["name"] for t in model.mcp("tools/list")["result"]["tools"]}
    assert {
        "heartbeat_status",
        "heartbeat_beat",
        "heartbeat_beats",
        "heartbeat_regions",
        "heartbeat_undo",
        "heartbeat_configure",
    } <= names
    status = model.tool("heartbeat_status")
    assert "every 15 min" in status["content"][0]["text"]
    asked = model.tool("heartbeat_configure", cadence=60)
    assert asked["isError"] and "needs_user_confirmation" in asked["content"][0]["text"]
    done = model.tool("heartbeat_configure", cadence=60, confirmed_by_user={"quote": "yes, hourly"})
    assert not done.get("isError")
    assert owner.ok(owner.get("/api/heartbeat/status"))["cadence"] == 60
    ran = model.tool("heartbeat_beat")
    assert "Beat beat_" in ran["content"][0]["text"]
    assert model.tool("heartbeat_regions")["structuredContent"]["regions"]


def test_a_model_cannot_change_settings_over_http(model):
    assert model.put("/api/heartbeat/settings", json={"paused": True}).status_code == 403


def test_beats_list_since(owner, clock):
    found(owner)
    beat(owner)
    clock.advance(timedelta(minutes=20))
    later = clock.now().isoformat()
    beat(owner)
    assert len(owner.ok(owner.get("/api/heartbeat/beats"))) == 2
    assert Api  # the conftest client is the one these tests speak through
    assert len(owner.ok(owner.get("/api/heartbeat/beats", params={"since": later}))) <= 1


def test_a_real_must_hold_protects_what_its_words_name(owner, runtime, hb):
    ws = runtime.ws
    made = ws.brain.propose(
        ws.owner(),
        title="Pricing stays private",
        statement="Never share pricing until it is final.",
        kind="rule",
        evidence="The owner said so.",
        topic="rules/delivery",
        must_hold={
            "applies_to": "facts",
            "triggers": "pricing, price sheet",
            "must_text": "Never share pricing until it is final.",
        },
    )
    assert made.proposal_id, made
    ws.brain.decide(ws.owner(), made.proposal_id, approve=True)
    held = hb.protecting("Pricing sheet v4 is edited, still on the Draft tab: new pricing.")
    assert held and "pricing" in str(held.get("line")).lower()
    assert hb.protecting("Morgan approved hero option B.") is None


def test_each_write_goes_through_the_one_proposal_path_as_a_connector_read(
    owner, runtime, monkeypatch
):
    found(owner)
    seen = []
    real = runtime.ws.brain.propose
    monkeypatch.setattr(
        runtime.ws.brain, "propose", lambda who, **kw: seen.append(kw) or real(who, **kw)
    )
    beat(owner)
    (call,) = seen
    assert call["capture_origin"] == "connector"
    assert call["sources"] == ["connector:claude:mail:msg-1"]
    assert call["source_speaker"] == "Morgan" and call["as_of"] == "2026-09-28"
    assert call["reason"].startswith("Morgan approved it by email.")
    assert call["destination"] == "progress" and call["place"]["project"] == PROJECT


def test_a_write_that_still_waits_in_auto_says_why(owner, runtime, hb, monkeypatch):
    found(owner)
    monkeypatch.setattr(
        hb,
        "_propose",
        lambda *a: BrainProposal(
            status="pending", message="It waits.", proposal_id="p-2", waits=["sealed project"]
        ),
    )
    done = beat(owner)
    assert done["waits"] == 1 and done["changes"] == 0
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["status"] == "waiting"
    assert line["wrote"] == "Waits for you (sealed project): Website Redesign › latest progress"


def test_a_new_brain_finds_its_connectors_on_the_first_look(owner, hb):
    worker = hb_service.HeartbeatWorker(hb)
    worker.run_once()
    names = {c["id"] for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert "claude:mail" in names


def test_in_auto_a_connector_change_goes_in_shows_on_home_undoes_and_is_not_repeated(
    owner, runtime, fake_cli
):
    """The definition of done's loop, against the real Brain: connector change, beat, the
    region line and the project's status with their source, undo, and the next beat leaves it."""
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner)
    done = beat(owner)
    assert done["changes"] == 1 and done["waits"] == 0, done
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["status"] == "applied" and line["checkpoint_id"]
    regions = {r["id"]: r for r in owner.ok(owner.get("/api/heartbeat/regions"))}
    (shown,) = regions["state"]["lines"]
    assert shown["connector"] == "mail" and shown["cli_label"] == "Claude Code"
    assert regions["state"]["projects"][0]["status"] == "Hero option B approved"
    owner.ok(owner.post(f"/api/heartbeat/lines/{line['id']}/undo"))
    fake_cli.items = [dict(ITEM, title="Morgan approved hero option B (edited)")]
    again = beat(owner)
    assert again["changes"] == 0 and again["waits"] == 0
    assert [r["status"] for r in rows(runtime, schema.heartbeat_lines)] == ["undone"]


def test_a_scout_file_reaches_the_judge_with_its_words(owner, fake_cli, tmp_path):
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "run-of-show.md").write_text(
        "# Run of show\n\nSession 1 moves to Thursday at 10:00.\n", encoding="utf-8"
    )
    owner.ok(owner.put("/api/scout", json={"folders": [{"path": str(docs)}]}))
    found(owner)
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"enabled": False}))
    owner.ok(owner.put("/api/heartbeat/connectors/scout", json={"enabled": True}))
    assert beat(owner)["items"] == 1
    (judge,) = fake_cli.judges()
    assert "Session 1 moves to Thursday at 10:00." in json.dumps(judge, default=str)


def test_a_refused_write_stays_in_the_beat_record_not_the_region(owner, fake_cli):
    found(owner)
    fake_cli.writes = lambda item: [
        {"kind": "progress", "project": PROJECT, "text": item["text"], "reason": "said so"},
        {
            "kind": "sheet",
            "sheet": "no-such-sheet",
            "row": "x",
            "column": "y",
            "value": "z",
            "reason": "a sheet that is not there",
        },
    ]
    done = beat(owner)
    assert any("not written" in d for d in done["details"]), done["details"]
    regions = {r["id"]: r for r in owner.ok(owner.get("/api/heartbeat/regions"))}
    assert [line["status"] for line in regions["state"]["lines"]] != []
    assert all(line["status"] != "refused" for line in regions["state"]["lines"])


def test_a_task_due_date_written_another_way_is_left_out_not_refused(owner, runtime, fake_cli):
    found(owner)
    fake_cli.writes = lambda item: [
        {
            "kind": "task",
            "project": PROJECT,
            "text": "Send the reminder emails.",
            "due": "Tuesday",
            "reason": "the run of show says so",
        }
    ]
    done = beat(owner)
    assert done["changes"] + done["waits"] == 1, done["details"]


def test_an_item_that_moved_two_homes_counts_once_as_new(owner, fake_cli):
    found(owner)
    fake_cli.writes = lambda item: [
        {"kind": "progress", "project": PROJECT, "text": item["text"], "reason": "said so"},
        {
            "kind": "task",
            "project": PROJECT,
            "text": "Send Morgan the hero files.",
            "reason": "it follows",
        },
    ]
    beat(owner)
    regions = {r["id"]: r for r in owner.ok(owner.get("/api/heartbeat/regions"))}
    assert len(regions["state"]["lines"]) == 2  # one per write, each with its own undo
    assert regions["state"]["new_this_beat"] == 1


NOTION = "claude_ai_Notion"
ACCOUNT_INIT = {
    "type": "system",
    "subtype": "init",
    "mcp_servers": [
        {"name": "claude.ai Notion", "status": "connected"},
        {"name": "claude.ai Gmail", "status": "needs-auth"},
        {"name": "mail", "status": "connected"},
    ],
    "tools": [
        "Bash",
        f"mcp__{NOTION}__notion-search",
        f"mcp__{NOTION}__notion-fetch",
        f"mcp__{NOTION}__notion-create-pages",
        f"mcp__{NOTION}__notion-update-page",
        "mcp__mail__list_messages",
    ],
}


ACCOUNT_LIST = (
    "claude.ai Notion: https://mcp.notion.com/mcp - Connected\n"
    "claude.ai Gmail: https://gmailmcp.googleapis.com/mcp/v1 - Needs authentication\n"
    "mail: python fake_mcp_server.py - Connected\n"
)


def test_claude_account_connectors_are_found_from_claude_codes_start_and_start_off(owner, fake_cli):
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    (listing,) = fake_cli.listings  # every connector named was there the first time
    assert "--strict-mcp-config" not in listing  # the account's connectors load
    (started,) = [c for c in fake_cli.started if "stream-json" in c["argv"]]
    assert started["env"]["ENABLE_CLAUDEAI_MCP_SERVERS"] == "true"
    assert started["env"]["MCP_CONNECTION_NONBLOCKING"] == "false"  # it waits for them
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    notion = rows[f"claude:{NOTION}"]
    assert notion["name"] == "claude.ai Notion" and notion["found_in"] == "Your Claude account"
    assert notion["enabled"] is False and notion["status"] == "off"
    assert sorted(notion["tools"]) == ["notion-fetch", "notion-search"]
    assert "turn it on" in notion["status_line"]
    assert rows["claude:claude_ai_Gmail"]["status"] == "unhealthy_sign_in"
    assert "claude:mail" in rows and not any(k.startswith("claude:claude_ai_mail") for k in rows)
    assert fake_cli.judges() == []  # listing makes no model call of its own


def test_a_connector_still_loading_is_tried_again_then_said_plainly(owner, fake_cli):
    fake_cli.account_init = {**ACCOUNT_INIT, "mcp_servers": [], "tools": []}
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    assert len(fake_cli.listings) == 4  # it loads in the background: tried again
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    notion = rows[f"claude:{NOTION}"]
    # Found by discovery but not connected yet: Connecting, with what to do; never an error.
    assert notion["status"] == "connecting" and "Test read" in notion["status_line"]


def test_an_account_connector_reads_with_only_its_read_only_tools(owner, fake_cli):
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"enabled": False}))
    owner.ok(owner.put(f"/api/heartbeat/connectors/claude:{NOTION}", json={"enabled": True}))
    assert beat(owner)["items"] == 1
    (read,) = fake_cli.reads()
    argv = read["argv"]
    assert "--strict-mcp-config" not in argv and read["config"] is None
    allowed = argv[argv.index("--allowedTools") + 1 : argv.index("--disallowedTools")]
    assert sorted(allowed) == [f"mcp__{NOTION}__notion-fetch", f"mcp__{NOTION}__notion-search"]
    refused = argv[argv.index("--disallowedTools") + 1 : argv.index("--permission-mode")]
    # A read lists what changed, opens the newest and answers: a 311,000-token read took 12.
    assert argv[argv.index("--max-turns") + 1] == str(runners_mod.READ_TURNS)
    assert f"mcp__{NOTION}__notion-create-pages" in refused
    assert argv[argv.index("--tools") + 1] == ""  # no built-in tools
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    (started,) = [c for c in fake_cli.started if c["argv"] == argv]
    assert started["env"]["ENABLE_CLAUDEAI_MCP_SERVERS"] == "true"


def test_a_server_name_before_the_verb_still_reads():
    assert conn.read_only({"name": "notion-search"}, "claude.ai Notion")
    assert conn.read_only({"name": "slack_list_channels"}, "slack")
    assert not conn.read_only({"name": "notion-create-pages"}, "claude.ai Notion")
    assert not conn.read_only({"name": "notion-duplicate-page"}, "claude.ai Notion")


def add_claude_server(name):
    """One more MCP server in Claude Code's own settings (the fake one, under another name)."""
    path = os.path.join(os.environ["CLAUDE_CONFIG_DIR"], ".claude.json")
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    data["mcpServers"][name] = dict(data["mcpServers"]["mail"])
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)


def test_due_connectors_in_claude_codes_settings_are_read_in_one_run(owner, fake_cli):
    add_claude_server("docs")
    found(owner)
    owner.ok(owner.put("/api/heartbeat/connectors/claude:docs", json={"enabled": True}))
    fake_cli.items_by = {
        "mail": [dict(ITEM)],
        "docs": [dict(ITEM, id="doc-1", title="Run of show v3", text="Session 1 moved.")],
    }
    done = beat(owner)
    (read,) = fake_cli.reads()  # one Claude Code run for both
    assert sorted(read["config"]["mcpServers"]) == ["docs", "mail"]
    assert "--strict-mcp-config" in read["argv"]
    assert done["items"] == 2 and done["connectors_read"] == 2
    assert done["tokens"] == 240  # one read and one judge, each counted once


def test_an_account_and_a_settings_connector_take_two_runs_not_more(owner, fake_cli):
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    owner.ok(owner.put(f"/api/heartbeat/connectors/claude:{NOTION}", json={"enabled": True}))
    beat(owner)
    reads = fake_cli.reads()
    assert len(reads) == 2
    fenced, account = sorted(reads, key=lambda r: r["config"] is None)
    assert list(fenced["config"]["mcpServers"]) == ["mail"]
    assert account["config"] is None and "--strict-mcp-config" not in account["argv"]


def test_a_connector_read_for_the_first_time_looks_back_a_week(owner, fake_cli):
    found(owner)
    beat(owner)
    (read,) = fake_cli.reads()
    assert "the last 7 days" in read["prompt"]
    fake_cli.calls.clear()
    beat(owner)
    (again,) = fake_cli.reads()
    assert "the last 7 days" not in again["prompt"] and "2026-09-28T12:40:00Z" in again["prompt"]


def test_a_new_connector_that_reads_through_a_cli_is_read_daily(owner):
    owner.ok(owner.post("/api/heartbeat/connectors/rescan"))
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert rows["claude:mail"]["cadence_minutes"] == 1440  # a read costs tokens: daily
    assert rows["scout"]["cadence_minutes"] is None  # Scout costs no tokens: every beat


def test_the_heartbeat_is_off_until_a_connector_is_set_up(owner, hb):
    worker = hb_service.HeartbeatWorker(hb)
    assert not hb.due()
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert status["state"] == "no_connectors" and status["next_beat_at"] is None
    assert status["line"].startswith("Off until")
    found(owner)
    assert hb.due()
    assert worker.run_once()["status"] == "done"


def test_an_app_n8n_reads_is_not_also_read_through_a_cli(owner, hb, fake_cli, monkeypatch):
    found(owner)
    monkeypatch.setattr(
        hb_service.templates, "app_of", lambda name: "gmail" if "mail" in name else None
    )
    hb.add_n8n(
        hb.ws.owner(),
        name="Gmail through n8n",
        workflow=n8n_workflow(),
        feeds=[],
        connector_id="n8n:gmail",
        line="test",
    )
    owner.ok(owner.put("/api/heartbeat/connectors/n8n:gmail", json={"enabled": True}))
    beat(owner)
    assert fake_cli.reads() == []  # no tokens spent on what n8n brings


# ---------------------------------------------------------------------- reads through n8n

from workspace_server.heartbeat import n8n as n8n_reads  # noqa: E402
from workspace_server.routes import heartbeat as hb_routes  # noqa: E402

N8N_ID = "n8n:gmail"


def n8n_workflow(*extra, connector=N8N_ID, url=None):
    """A schedule, a Gmail read, a filter and the post to this connector's inbox."""
    return {
        "name": "Gmail for Gnomon",
        "nodes": [
            {"name": "Every hour", "type": "n8n-nodes-base.scheduleTrigger", "parameters": {}},
            {
                "name": "Recent mail",
                "type": "n8n-nodes-base.gmail",
                "parameters": {"resource": "message", "operation": "getAll"},
            },
            {"name": "From people I know", "type": "n8n-nodes-base.filter", "parameters": {}},
            {
                "name": "To Gnomon",
                "type": "n8n-nodes-base.httpRequest",
                "parameters": {
                    "method": "POST",
                    "url": url or f"http://127.0.0.1:8787/api/heartbeat/inbox/{connector}",
                },
            },
            *extra,
        ],
    }


def test_a_workflow_runs_as_a_read_only_when_every_node_only_reads():
    assert n8n_reads.check_workflow(n8n_workflow(), N8N_ID) == []
    refused = {
        "a send": {
            "name": "Reply",
            "type": "n8n-nodes-base.gmail",
            "parameters": {"operation": "send"},
        },
        "code": {"name": "Anything", "type": "n8n-nodes-base.code", "parameters": {}},
        "another address": {
            "name": "Out",
            "type": "n8n-nodes-base.httpRequest",
            "parameters": {"method": "GET", "url": "https://example.com"},
        },
        "an unknown node": {
            "name": "X",
            "type": "n8n-nodes-base.somethingNew",
            "parameters": {"operation": "getAll"},
        },
    }
    for why, node in refused.items():
        assert n8n_reads.check_workflow(n8n_workflow(node), N8N_ID), why
    assert n8n_reads.check_workflow(n8n_workflow(connector="n8n:other"), N8N_ID)  # wrong inbox
    away = n8n_workflow(url="https://example.com/api/heartbeat/inbox/n8n:gmail")
    assert n8n_reads.check_workflow(away, N8N_ID)  # not this computer
    assert n8n_reads.check_workflow({"nodes": []}, N8N_ID)


def test_an_n8n_read_is_checked_off_until_turned_on_and_its_secret_is_shown_once(owner):
    bad = owner.post(
        "/api/heartbeat/n8n",
        json={
            "name": "Gmail",
            "workflow": n8n_workflow(
                {
                    "name": "Reply",
                    "type": "n8n-nodes-base.gmail",
                    "parameters": {"operation": "send"},
                }
            ),
        },
    )
    assert bad.status_code == 422 and "not a read" in bad.text
    made = owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    assert made["connector"]["id"] == N8N_ID and made["connector"]["enabled"] is False
    assert len(made["secret"]) > 30
    status = owner.ok(owner.get("/api/heartbeat/status"))
    assert made["secret"] not in json.dumps(status)  # kept as a hash only


def test_only_an_approved_n8n_read_with_its_secret_posts_from_this_computer(
    owner, client, monkeypatch
):
    made = owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    secret = {n8n_reads.SECRET_HEADER: made["secret"]}
    post = lambda headers: client.post(  # noqa: E731
        f"/api/heartbeat/inbox/{N8N_ID}", json={"items": [ITEM]}, headers=headers
    )
    assert post(secret).status_code == 403  # the test client is not this computer
    monkeypatch.setattr(hb_routes, "LOCAL", hb_routes.LOCAL | {"testclient"})
    assert post({n8n_reads.SECRET_HEADER: "wrong"}).status_code == 403
    assert post(secret).status_code == 409  # off until turned on
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    assert post(secret).json() == {"kept": 1}


@pytest.mark.parametrize(
    "forwarded",
    [
        {"X-Forwarded-For": "127.0.0.1"},
        {"X-Forwarded-For": "203.0.113.9"},
        {"Forwarded": "for=127.0.0.1"},
        {"X-Real-IP": "127.0.0.1"},
    ],
)
def test_a_post_that_came_through_a_proxy_is_not_from_this_computer(
    owner, client, monkeypatch, forwarded
):
    """A reverse proxy on this computer (Caddy, Tailscale Serve) connects from 127.0.0.1 for
    everyone on the internet; it says so with a forwarding header, so the inbox and the error
    route refuse such a post even with the right secret."""
    monkeypatch.setattr(hb_routes, "LOCAL", hb_routes.LOCAL | {"testclient"})
    made = owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    headers = {n8n_reads.SECRET_HEADER: made["secret"], **forwarded}
    posted = client.post(f"/api/heartbeat/inbox/{N8N_ID}", json={"items": [ITEM]}, headers=headers)
    assert posted.status_code == 403
    reported = client.post("/api/heartbeat/n8n/errors", json={}, headers=headers)
    assert reported.status_code == 403
    plain = {n8n_reads.SECRET_HEADER: made["secret"]}  # n8n itself posts with no such header
    assert client.post(
        f"/api/heartbeat/inbox/{N8N_ID}", json={"items": [ITEM]}, headers=plain
    ).json() == {"kept": 1}


def test_a_beat_takes_what_n8n_posted_once_and_spends_no_tokens_reading_it(
    owner, client, fake_cli, monkeypatch
):
    monkeypatch.setattr(hb_routes, "LOCAL", hb_routes.LOCAL | {"testclient"})
    owner.ok(owner.put("/api/heartbeat/learning", json={"learn": "per_beat"}))
    made = owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    headers = {n8n_reads.SECRET_HEADER: made["secret"]}
    client.post(f"/api/heartbeat/inbox/{N8N_ID}", json=[{"json": ITEM}], headers=headers)
    done = beat(owner)
    assert done["items"] == 1 and fake_cli.reads() == []  # no CLI read: n8n read it
    assert done["tokens"] == 120 and done["tokens_unknown"] == 0  # the judge alone
    again = beat(owner)
    assert again["items"] == 0 and again["nothing_new"]


def test_turning_an_n8n_read_on_puts_it_into_gnomons_n8n_active_with_a_fresh_secret(owner, hb):
    hosted = HostedN8n()
    hosted.error_hash = "set up already"
    hb.n8n_host = hosted
    made = owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    on = owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    ((workflow, active),) = hosted.put
    assert active is True and on["workflow_id"] == workflow["id"]
    post = next(n for n in workflow["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")
    secret = post["parameters"]["headerParameters"]["parameters"][0]["value"]
    assert secret != made["secret"]  # a fresh one, only in n8n's copy
    assert hb.receive(N8N_ID, secret, {"items": [ITEM]}) == {"kept": 1}
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": False}))
    assert hosted.put[-1][1] is False


from workspace_server.heartbeat import n8n_templates  # noqa: E402


def test_every_suggested_workflow_only_reads_and_posts_once_to_its_inbox():
    for app in n8n_templates.APPS:
        cid = f"n8n:{app}"
        workflow = n8n_templates.workflow_for(app, cid, "http://127.0.0.1:8787")
        assert n8n_reads.check_workflow(workflow, cid) == [], app
        (post,) = [n for n in workflow["nodes"] if n["type"] == "n8n-nodes-base.httpRequest"]
        assert post["executeOnce"] is True
    assert n8n_templates.app_of("claude.ai Gmail") == "gmail"
    assert n8n_templates.app_of("claude.ai Google Drive") == "google-drive"
    assert n8n_templates.app_of("node_repl") is None


def test_the_connector_search_suggests_a_workflow_per_known_app_off_until_turned_on(
    owner, fake_cli, tmp_path, monkeypatch
):
    gemini = tmp_path / "gemini"
    gemini.mkdir()
    (gemini / "settings.json").write_text(
        json.dumps({"mcpServers": {"slack": {"command": "slack-mcp"}}}), encoding="utf-8"
    )
    monkeypatch.setenv("GEMINI_CLI_HOME", str(gemini))
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found = owner.ok(owner.post("/api/heartbeat/n8n/suggest"))
    assert sorted(found["suggested"]) == ["Gmail", "Notion", "Slack"]
    assert "mail" in found["without_workflow"]
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    notion = rows["n8n:notion"]
    assert notion["enabled"] is False and "sign in to Notion" in notion["status_line"]
    assert rows["gemini:slack"]["enabled"] is False
    refused = owner.put("/api/heartbeat/connectors/gemini:slack", json={"enabled": True})
    assert refused.status_code == 409
    again = owner.ok(owner.post("/api/heartbeat/n8n/suggest"))
    assert again["suggested"] == [] and sorted(again["already"]) == ["Gmail", "Notion", "Slack"]


class HostedN8n:
    """Gnomon's n8n as the heartbeat uses it: what went in, and the error report's secret."""

    def __init__(self):
        self.put = []
        self.error_hash = None

    def installed(self):
        return True

    def put_workflow(self, workflow, *, active):
        self.put.append((workflow, active))
        return workflow["id"]

    def error_secret_hash(self):
        return self.error_hash

    def keep_error_secret_hash(self, kept):
        self.error_hash = kept

    def close(self):
        pass


def secret_in(workflow):
    post = next(n for n in workflow["nodes"] if n["type"] == "n8n-nodes-base.httpRequest")
    return post["parameters"]["headerParameters"]["parameters"][0]["value"]


def test_a_failed_n8n_read_shows_its_error_until_it_posts_again(owner, client, hb, monkeypatch):
    monkeypatch.setattr(hb_routes, "LOCAL", hb_routes.LOCAL | {"testclient"})
    hosted = HostedN8n()
    hb.n8n_host = hosted
    owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    (reporter, _), (read, _) = hosted.put  # the error report first, once
    assert reporter["id"] == n8n_reads.ERROR_WORKFLOW
    assert read["settings"]["errorWorkflow"] == n8n_reads.ERROR_WORKFLOW
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": False}))
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    assert [w["id"] for w, _ in hosted.put].count(n8n_reads.ERROR_WORKFLOW) == 1
    failure = {
        "workflow": {"id": read["id"]},
        "execution": {
            "error": {"message": "Credentials not found"},
            "lastNodeExecuted": "Recent mail",
        },
    }
    report = lambda secret: client.post(  # noqa: E731
        "/api/heartbeat/n8n/errors", json=failure, headers={n8n_reads.SECRET_HEADER: secret}
    )
    assert report("wrong").status_code == 403
    assert report(secret_in(reporter)).json() == {"connectors": [N8N_ID]}
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert rows[N8N_ID]["status"] == "error"
    assert "Credentials not found (at Recent mail)" in rows[N8N_ID]["status_line"]
    secret = secret_in(hosted.put[-1][0])
    hb.receive(N8N_ID, secret, {"items": [ITEM]})
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert rows[N8N_ID]["status"] == "ok"


def test_a_suggested_fix_is_checked_then_kept_only_with_the_owners_yes(owner, hb, monkeypatch):
    hosted = HostedN8n()
    hb.n8n_host = hosted
    owner.ok(
        owner.post("/api/heartbeat/n8n", json={"name": "Gmail", "workflow": n8n_workflow()}), 201
    )
    owner.ok(owner.put(f"/api/heartbeat/connectors/{N8N_ID}", json={"enabled": True}))
    fixed = n8n_workflow({"name": "Newest first", "type": "n8n-nodes-base.sort", "parameters": {}})
    unsafe = n8n_workflow(
        {"name": "Reply", "type": "n8n-nodes-base.gmail", "parameters": {"operation": "send"}}
    )
    answers = iter(
        [
            json.dumps({"why": "It sends now.", "workflow": unsafe}),
            json.dumps({"why": "Sorted newest first.", "workflow": fixed}),
        ]
    )
    monkeypatch.setattr(hb, "_ask", lambda who, cli, prompt: (next(answers), 900))
    refused = owner.post(f"/api/heartbeat/n8n/{N8N_ID}/fix")
    assert refused.status_code == 409 and "would not only read" in refused.text
    waiting = owner.ok(owner.post(f"/api/heartbeat/n8n/{N8N_ID}/fix"))
    assert waiting["fix"] == "Sorted newest first."
    before = len(hosted.put)
    kept = owner.ok(owner.put(f"/api/heartbeat/n8n/{N8N_ID}/fix", json={"keep": True}))
    assert kept["fix"] is None and len(hosted.put) == before + 1  # back into n8n, active
    assert any(n["name"] == "Newest first" for n in hosted.put[-1][0]["nodes"])
    assert owner.put(f"/api/heartbeat/n8n/{N8N_ID}/fix", json={"keep": True}).status_code == 409


def test_in_auto_a_connector_note_goes_in_with_undo(owner, runtime, fake_cli):
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner)
    fake_cli.writes = lambda item: [
        {
            "kind": "note",
            "area": "facts",
            "topic": "website",
            "text": item["text"],
            "reason": "Morgan approved it by email.",
        }
    ]
    done = beat(owner)
    assert done["changes"] == 1 and done["waits"] == 0, done["details"]
    (line,) = rows(runtime, schema.heartbeat_lines)
    assert line["status"] == "applied" and line["checkpoint_id"]


def test_choosing_a_scout_folder_updates_its_row_at_once(owner, tmp_path):
    owner.ok(owner.post("/api/heartbeat/connectors/rescan"))
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert rows["scout"]["status_line"] == "Choose a folder for Scout first."
    owner.ok(owner.put("/api/scout", json={"folders": [{"path": str(tmp_path)}]}))
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert rows["scout"]["status_line"] == "Reads the 1 Scout folder(s) by content."


# ---------------------------------------------------------------------- memory and onboarding


def write_memories():
    """What Claude Code and Codex keep on the person, as they write it: Claude Code's own
    CLAUDE.md, a project's memory file and what was typed (history.jsonl); a Codex session's
    rollout. Both also hold a sign-in file that is never read."""
    claude = os.environ["CLAUDE_CONFIG_DIR"]
    codex = os.environ["CODEX_HOME"]
    os.makedirs(os.path.join(claude, "projects", "C--dev-site", "memory"), exist_ok=True)
    with open(os.path.join(claude, "CLAUDE.md"), "w", encoding="utf-8") as handle:
        handle.write("Write plainly. No em dashes.")
    with open(
        os.path.join(claude, "projects", "C--dev-site", "memory", "role.md"), "w", encoding="utf-8"
    ) as handle:
        handle.write("Leads the website redesign at State Focus.")
    with open(os.path.join(claude, ".credentials.json"), "w", encoding="utf-8") as handle:
        handle.write('{"token": "sign-in-secret-999"}')
    typed = [
        {
            "display": "We picked hero option B for the website",
            "timestamp": 1790000000000,
            "project": "C:/dev/site",
        },
        {"display": "yes", "timestamp": 1790000001000, "project": "C:/dev/site"},
        {"display": "<command-name>/clear</command-name>", "timestamp": 1790000002000},
        {
            "display": "Use the key sk-ant-api03-abcdefghijklmnopqrstuvwxyz0123 for staging",
            "timestamp": 1790000003000,
            "project": "C:/dev/site",
        },
    ]
    with open(os.path.join(claude, "history.jsonl"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(json.dumps(t) for t in typed) + "\n")
    at = "2026-09-19T09:00:00Z"
    session = [  # a session's transcript, as the desktop app's sessions keep it
        {
            "type": "user",
            "timestamp": at,
            "cwd": "C:/dev/site",
            "message": {"role": "user", "content": "The client signs off every Friday"},
        },
        {
            "type": "user",
            "timestamp": at,
            "toolUseResult": {},
            "message": {"role": "user", "content": [{"type": "tool_result", "content": "ls"}]},
        },
        {
            "type": "user",
            "timestamp": at,
            "isMeta": True,
            "message": {"role": "user", "content": "Claude Code added this turn"},
        },
        {
            "type": "assistant",
            "timestamp": at,
            "message": {"role": "assistant", "content": [{"type": "text", "text": "Noted"}]},
        },
    ]
    with open(
        os.path.join(claude, "projects", "C--dev-site", "session-1.jsonl"), "w", encoding="utf-8"
    ) as handle:
        handle.write("\n".join(json.dumps(x) for x in session) + "\n")
    day = os.path.join(codex, "sessions", "2026", "09", "20")
    os.makedirs(day, exist_ok=True)
    lines = [
        {
            "type": "session_meta",
            "timestamp": "2026-09-20T10:00:00Z",
            "payload": {"cwd": "C:/dev/billing"},
        },
        {
            "type": "response_item",
            "timestamp": "2026-09-20T10:00:01Z",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "<environment_context>x"}],
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-09-20T10:01:00Z",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "Invoices go out on the 1st"}],
            },
        },
        {
            "type": "response_item",
            "timestamp": "2026-09-20T10:02:00Z",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "I suggest the 15th"}],
            },
        },
    ]
    with open(os.path.join(day, "rollout-a.jsonl"), "w", encoding="utf-8") as handle:
        handle.write("\n".join(json.dumps(x) for x in lines) + "\n")
    with open(os.path.join(codex, "auth.json"), "w", encoding="utf-8") as handle:
        handle.write('{"token": "sign-in-secret-999"}')


def onboard(owner, **body):
    return owner.ok(owner.post("/api/heartbeat/onboard?wait=true", json=body))


def test_a_cli_memory_is_gathered_from_its_own_files_with_secrets_masked(cli_home):
    write_memories()
    claude = hb_memory.gather("claude")
    assert [where for where, _ in claude.memory] == [
        "CLAUDE.md",
        os.path.join("projects", "C--dev-site", "memory", "role.md"),
    ]
    said = [text for _at, _project, text in claude.said]
    assert said[1] == "We picked hero option B for the website"
    assert "sk-ant-api03" not in said[0] and "[masked]" in said[0]
    assert not any(t in ("yes",) or t.startswith("<command") for t in said)
    assert claude.said[1][1] == "site"
    assert said[2:] == ["The client signs off every Friday"]  # the person's turn, nothing else
    codex = hb_memory.gather("codex")
    assert [t for _a, _p, t in codex.said] == ["Invoices go out on the 1st"]  # only the person
    assert codex.said[0][1] == "billing"
    everything = json.dumps([claude.memory, claude.said, codex.memory, codex.said])
    assert "sign-in-secret-999" not in everything
    later = hb_memory.gather("claude", since=claude.read_at)
    assert not later.said
    assert hb_memory.gather("gemini").empty()  # not on this computer


def test_onboarding_has_each_cli_build_context_packets_from_its_memory(
    owner, runtime, fake_cli, cli_home
):
    found(owner)
    write_memories()
    done = onboard(owner, pasted="ChatGPT remembers: prefers short answers.")
    assert done["trigger"] == "onboarding" and done["status"] == "done"
    assert {c["id"] for c in done["connectors"]} == {"memory:claude", "memory:codex"}
    assert done["items"] == 2 and done["changes"] + done["waits"] == 2  # a packet from each
    by_cli = {("exec" in r["argv"]): r["prompt"] for r in fake_cli.memory_reads}
    claude, codex = by_cli[False], by_cli[True]
    assert "Leads the website redesign" in claude and "hero option B" in claude
    assert "Claude (claude.ai" in claude and "ChatGPT (chatgpt.com" in codex
    assert "first read" in claude
    assert "prefers short answers" in claude and "prefers short answers" not in codex
    assert "Invoices go out on the 1st" in codex
    assert not [r for r in fake_cli.reads() if (r["config"] or {}).get("mcpServers")]
    by_id = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert by_id["memory:claude"]["enabled"] and by_id["memory:claude"]["cadence_minutes"] == 1440
    lines = rows(runtime, schema.heartbeat_lines)
    assert {line["item_id"] for line in lines} == {
        "connector:memory:claude:role",
        "connector:memory:codex:role",
    }


def test_a_later_memory_read_reads_only_what_is_new_and_nothing_new_costs_nothing(
    owner, fake_cli, cli_home
):
    found(owner)
    write_memories()
    onboard(owner, clis=["claude"])
    owner.ok(owner.put("/api/heartbeat/connectors/memory:claude", json={"cadence_minutes": 0}))
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"enabled": False}))
    first = len(fake_cli.memory_reads)
    beat(owner)
    assert len(fake_cli.memory_reads) == first  # nothing new: no CLI call
    history = os.path.join(os.environ["CLAUDE_CONFIG_DIR"], "history.jsonl")
    later = {"display": "The launch moved to November", "timestamp": time.time() * 1000 + 60000}
    with open(history, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(later) + "\n")
    owner.ok(
        owner.put(
            "/api/heartbeat/connectors/memory:claude", json={"ask": "Deadlines and launch dates"}
        )
    )
    beat(owner)
    prompt = fake_cli.memory_reads[-1]["prompt"]
    assert "The launch moved to November" in prompt and "hero option B" not in prompt
    assert "only what is new" in prompt
    assert "The person asked you to look for: Deadlines and launch dates" in prompt


def test_any_connector_read_takes_a_goal_and_what_to_leave_out(owner, cli_home):
    found(owner)
    row = owner.ok(
        owner.put(
            "/api/heartbeat/connectors/claude:mail",
            json={"ask": "client decisions", "skip": "newsletter, Receipt ,, newsletter"},
        )
    )
    assert row["ask"] == "client decisions"
    assert row["skip"] == "newsletter, receipt"
    cleared = owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"skip": ""}))
    assert cleared["skip"] == ""


def test_the_judge_runs_on_the_ingest_model(owner, fake_cli, runtime):
    found(owner)
    beat(owner)
    (judge,) = fake_cli.judges()
    assert judge["argv"][judge["argv"].index("--model") + 1] == "haiku"
    runtime.ws.distill.configure(runtime.ws.owner(), model="sonnet")
    fake_cli.calls.clear()
    fake_cli.items = [dict(ITEM, id="msg-2", title="Another")]
    beat(owner)
    (judge,) = fake_cli.judges()
    assert judge["argv"][judge["argv"].index("--model") + 1] == "sonnet"


# ---------------------------------------------------------------------- learning: the buffer


def staged(owner, status=None):
    return owner.ok(owner.get("/api/heartbeat/staged" + (f"?status={status}" if status else "")))


def test_every_change_a_beat_reads_goes_into_the_buffer_with_its_old_and_new_value(owner, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    (first,) = staged(owner)
    assert first["old_value"] is None and first["new_value"] == ITEM["text"]
    assert first["connector_id"] == "claude:mail" and first["status"] == "staged"
    fake_cli.items = [dict(ITEM, text="Morgan replied: hero option C instead.")]
    beat(owner)
    latest = staged(owner)[0]
    assert latest["old_value"] == ITEM["text"]
    assert latest["new_value"] == "Morgan replied: hero option C instead."


def test_a_filter_drops_at_capture_counts_on_the_beat_and_can_be_brought_back(owner, fake_cli):
    found(owner, per_beat=False)
    owner.ok(owner.put("/api/heartbeat/learning", json={"filters": [{"pattern": "hero option"}]}))
    done = beat(owner)
    assert any("Dropped 1 at capture" in d for d in done["details"])
    assert fake_cli.judges() == []  # nothing left to judge
    (dropped,) = staged(owner, "dropped")
    assert dropped["filter"] == "your filter: hero option"
    back = owner.ok(owner.post(f"/api/heartbeat/staged/{dropped['id']}/bring-back"))
    assert back["status"] == "staged"


def test_a_filter_match_the_goal_makes_relevant_goes_on_marked_unclear(owner, fake_cli):
    found(owner, per_beat=False)
    owner.ok(
        owner.put(
            "/api/heartbeat/learning",
            json={"goal": "website hero approval", "filters": [{"pattern": "hero"}]},
        )
    )
    done = beat(owner)
    assert not any("Dropped" in d for d in done["details"])
    (kept,) = staged(owner)
    assert kept["status"] == "staged" and kept["unclear"] and kept["score"] >= 0.35
    assert fake_cli.judges() == []  # nightly: the beat only captures


def test_forget_takes_a_change_out_of_the_buffer_before_the_distill(owner, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    (entry,) = staged(owner)
    assert owner.ok(owner.post(f"/api/heartbeat/staged/{entry['id']}/forget"))["status"] == (
        "forgotten"
    )
    assert staged(owner, "staged") == []


def test_the_buffer_keeps_its_retention_then_removes_old_entries(owner, runtime, fake_cli):
    found(owner, per_beat=False)
    owner.ok(owner.put("/api/heartbeat/learning", json={"buffer_days": 30}))
    with runtime.ws.rt.write() as tx:
        tx.insert(
            schema.staged_changes,
            {
                "id": "stg_old",
                "workspace_id": tx.workspace_id,
                "connector_id": "claude:mail",
                "item_id": "msg-0",
                "field": "text",
                "title": "Old",
                "url": "",
                "new_value": "An old change",
                "seen_at": "2026-08-01T00:00:00Z",
                "beat_id": "beat_old",
                "filter": "",
                "unclear": False,
                "status": "distilled",
            },
        )
    beat(owner)
    assert [e["item_id"] for e in staged(owner)] == ["msg-1"]  # the 58-day-old one is gone


def test_learning_settings_are_checked(owner):
    assert (
        owner.put("/api/heartbeat/learning", json={"filters": [{"source": "x"}]}).status_code == 422
    )
    assert owner.put("/api/heartbeat/learning", json={"buffer_days": 0}).status_code == 422
    saved = owner.ok(owner.put("/api/heartbeat/learning", json={"goal": "Run the Q4 webinars"}))
    assert saved == {
        "goal": "Run the Q4 webinars",
        "filters": [],
        "buffer_days": 30,
        "learn": "nightly",
        "distill_at": "19:00",
        "live": {"connectors": [], "sheets": []},
    }


# ---------------------------------------------------------------------- learning: the distill


def distill(owner):
    return owner.ok(owner.post("/api/heartbeat/distill?wait=true"))


def test_a_change_undone_the_same_day_is_discarded_without_a_model_call(owner, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    distill(owner)  # yesterday: the Brain learned what it said
    fake_cli.distilled.clear()
    fake_cli.items = [dict(ITEM, text="Morgan replied: hero option C instead.")]
    beat(owner)
    fake_cli.items = [dict(ITEM)]  # back to what it said before
    beat(owner)
    done = distill(owner)
    assert fake_cli.distilled == [] and done["changes"] == 0
    today = [e for e in staged(owner) if e["old_value"] is not None]
    assert {e["outcome"]["reason"] for e in today} == {"undone the same day"}
    assert {e["status"] for e in staged(owner)} == {"distilled"}  # archived, not deleted


def test_repeated_edits_collapse_to_one_item_with_its_count(owner, fake_cli):
    found(owner, per_beat=False)
    for text in ("v1", "v2", "v3"):
        fake_cli.items = [dict(ITEM, text=f"Morgan replied: {text}.")]
        beat(owner)
    distill(owner)
    ((shown,),) = fake_cli.distilled
    assert shown["changed"] == 3 and shown["now"] == "Morgan replied: v3."
    assert shown["before"] is None  # first seen today


def test_a_delete_wins_over_earlier_edits(owner, runtime, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    with runtime.ws.rt.write() as tx:
        tx.insert(
            schema.staged_changes,
            {
                "id": "stg_gone",
                "workspace_id": tx.workspace_id,
                "connector_id": "claude:mail",
                "item_id": "msg-1",
                "field": "text",
                "title": ITEM["title"],
                "url": "",
                "old_value": ITEM["text"],
                "new_value": None,
                "seen_at": "2026-09-28T13:30:00Z",
                "beat_id": "beat_x",
                "filter": "",
                "unclear": False,
                "status": "staged",
            },
        )
    distill(owner)
    ((shown,),) = fake_cli.distilled
    assert shown["deleted"] is True


def test_a_confident_route_goes_in_and_an_unsure_one_waits_in_catch_up(owner, fake_cli):
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner, per_beat=False)
    beat(owner)
    done = distill(owner)
    assert done["changes"] == 1 and done["waits"] == 0
    (entry,) = staged(owner)
    assert entry["outcome"]["route"] == "short" and entry["status"] == "distilled"
    fake_cli.items = [dict(ITEM, id="msg-2", title="Maybe a new date")]
    fake_cli.route = lambda item: ("short", 0.5)  # not sure
    beat(owner)
    unsure = distill(owner)
    assert unsure["waits"] == 1 and unsure["changes"] == 0  # waits even in Auto
    assert staged(owner)[0]["outcome"]["route"] == "catch_up"


def test_the_distill_runs_nightly_and_after_a_night_the_computer_slept(owner, hb, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    # 13:00 on Sep 28, nothing distilled since yesterday 19:00: it is due (a missed night).
    assert hb.distill_due()
    ran = hb_service.HeartbeatWorker(hb).run_once()
    assert ran["trigger"] == "distill" and ran["status"] == "done"
    assert not hb.distill_due()  # done for this night


def test_a_later_change_replaces_what_still_waits_from_an_earlier_distill(owner, runtime, fake_cli):
    found(owner, per_beat=False)
    fake_cli.route = lambda item: ("short", 0.5)  # unsure: Catch up
    beat(owner)
    distill(owner)
    (first,) = staged(owner)
    assert first["outcome"]["route"] == "catch_up"
    waiting = {p["id"] for p in runtime.ws.brain.proposals(runtime.ws.owner())}
    earlier = next(iter(waiting))
    fake_cli.items = [dict(ITEM, text="Morgan replied: hero option C instead.")]
    beat(owner)
    done = distill(owner)
    assert any("Replaced 1 waiting" in d for d in done["details"])
    now = {p["id"] for p in runtime.ws.brain.proposals(runtime.ws.owner())}
    assert earlier not in now and len(now) == 1  # only the newest waits


def test_what_waits_over_14_days_is_gathered_into_one_card(owner, runtime, fake_cli):
    found(owner, per_beat=False)
    fake_cli.route = lambda item: ("short", 0.5)
    beat(owner)
    distill(owner)
    assert owner.ok(owner.get("/api/heartbeat/still-waiting"))["count"] == 0
    with runtime.ws.rt.write() as tx:
        tx.conn.execute(schema.staged_changes.update().values(seen_at="2026-09-10T09:00:00Z"))
    stale = owner.ok(owner.get("/api/heartbeat/still-waiting"))
    assert stale["count"] == 1 and stale["days"] == 14


# ---------------------------------------------------------------------- apply during the day


def test_a_live_connectors_change_goes_in_during_the_day(owner, fake_cli):
    found(owner, per_beat=False)
    owner.ok(owner.put("/api/heartbeat/learning", json={"live": {"connectors": ["claude:mail"]}}))
    done = beat(owner)
    assert done["changes"] + done["waits"] == 1  # Governed here: it waits, but now, not tonight
    assert len(fake_cli.distilled) == 1  # distilled in the beat
    (entry,) = staged(owner)
    assert entry["status"] == "distilled" and entry["outcome"]["route"] == "short"


def test_for_a_live_sheet_only_what_lands_in_it_goes_in_and_the_rest_waits(owner, fake_cli):
    found(owner, per_beat=False)
    owner.ok(owner.put("/api/heartbeat/learning", json={"live": {"sheets": ["guests"]}}))
    beat(owner)  # the distill would write project progress, not the guests sheet
    (entry,) = staged(owner)
    assert entry["status"] == "staged"  # it waits for tonight
    fake_cli.items = [dict(ITEM, id="msg-2", title="Dana confirmed")]
    fake_cli.writes = lambda item: [
        {
            "kind": "sheet",
            "sheet": "guests",
            "row": "Dana Ruiz",
            "column": "Status",
            "value": "Confirmed",
            "reason": "she said so",
        }
    ]
    beat(owner)
    newest = staged(owner)[0]
    assert newest["status"] == "distilled"
    assert newest["outcome"]["reason"].startswith("applied during the day")


def test_without_live_sheets_or_connectors_a_beat_makes_no_model_call(owner, fake_cli):
    found(owner, per_beat=False)
    beat(owner)
    assert fake_cli.distilled == [] and fake_cli.judges() == []


# ---------------------------------------------------------------------- the recorded day


def test_the_recorded_day_runs_through_capture_and_the_distill_and_is_scored(hb, fake_cli):
    from workspace_server.heartbeat import day_eval

    path = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "scripts", "heartbeat_day.json"
    )
    day = day_eval.load(path)
    expect = {f"connector:day:{e['source']}:{e['id']}": e["expect"] for e in day["items"]}
    # A model that knows the labels: what remains wrong is code's (capture, netting, rules).
    fake_cli.route = lambda item: (expect[item["item"]].split("|")[0], 0.9)
    report = day_eval.run(hb, day)
    assert report["items"] == 50 and report["changes"] >= 70
    assert report["net_zero_caught"] == "10 of 10"  # by code, before any model call
    assert report["filter_precision"] == 1.0  # every capture-time drop was meant to be
    assert report["gates"]["nothing_must_wait_written"]
    assert report["route_accuracy"] >= 0.85, report["misses"]


def test_a_task_that_reaches_customers_or_spends_waits_whatever_the_model_said(owner, fake_cli):
    """The recorded day's finding: the model routed "email all attendees" and "raise the ad
    budget" as ordinary tasks without flags. Code flags them, so they wait even in Auto."""
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner, per_beat=False)
    fake_cli.items = [
        dict(ITEM, id="m-41", title="Send attendees new time"),
        dict(ITEM, id="m-42", title="Raise ad budget"),
    ]
    tasks = {
        "Send attendees new time": "Email all attendees that session 2 moved to Nov 20.",
        "Raise ad budget": "Raise the webinar ad budget to 900 a month.",
    }
    fake_cli.writes = lambda item: [
        {
            "kind": "task",
            "project": PROJECT,
            "text": tasks.get(item.get("title"), item["text"]),
            "reason": "the item asks for it",
        }
    ]
    original = fake_cli.judge

    def judge(prompt):
        answer = original(prompt)
        for verdict in answer.get("items") or []:
            title = (
                "Send attendees new time" if verdict["item"].endswith("m-41") else "Raise ad budget"
            )
            verdict["writes"] = fake_cli.writes({"title": title, "text": ""})
        return answer

    fake_cli.judge = judge
    beat(owner)
    done = distill(owner)
    assert done["changes"] == 0 and done["waits"] == 2


def test_a_distill_that_changed_the_brain_tags_a_brain_version(
    owner, runtime, fake_cli, brain_vault
):
    import subprocess

    top = os.path.dirname(brain_vault)  # versions are git tags: the Brain is a repository
    for args in (
        ["init", "-q", "-b", "main"],
        ["add", "-A", "--", os.path.basename(brain_vault)],
        ["-c", "user.name=T", "-c", "user.email=t@example.com", "commit", "-q", "-m", "start"],
    ):
        subprocess.run(["git", *args], cwd=top, check=True, capture_output=True)
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner, per_beat=False)
    before = len(runtime.ws.rt.brain.brain_versions())
    beat(owner)
    done = distill(owner)
    assert any(d.startswith("Saved as Brain version") for d in done["details"]), done["details"]
    versions = runtime.ws.rt.brain.brain_versions()
    assert len(versions) == before + 1 and versions[-1]["cause"] == "distill"
    quiet = distill(owner)  # nothing staged: no change, no new version
    assert not any("Brain version" in d for d in quiet["details"])
    assert len(runtime.ws.rt.brain.brain_versions()) == before + 1


def test_a_distill_that_saved_a_version_runs_the_after_distill_tests_on_it(
    owner, runtime, fake_cli, brain_vault, monkeypatch
):
    import subprocess

    top = os.path.dirname(brain_vault)
    for args in (
        ["init", "-q", "-b", "main"],
        ["add", "-A", "--", os.path.basename(brain_vault)],
        ["-c", "user.name=T", "-c", "user.email=t@example.com", "commit", "-q", "-m", "start"],
    ):
        subprocess.run(["git", *args], cwd=top, check=True, capture_output=True)
    owner.ok(owner.put("/api/brain/policies", json={"mode": "auto"}))
    found(owner, per_beat=False)
    ran = []

    def after_distill(who, version):
        ran.append(version)
        return {"details": [f"Test Bench on {version}: 1 ran, 0 unchanged."]}

    monkeypatch.setattr(runtime.heartbeat.regression, "after_distill", after_distill)
    beat(owner)
    done = distill(owner)
    version = runtime.ws.rt.brain.brain_versions()[-1]["name"]
    assert ran == [version]
    assert f"Test Bench on {version}: 1 ran, 0 unchanged." in done["details"]
    distill(owner)  # nothing changed: no version, no tests
    assert ran == [version]


def test_an_account_connector_that_connects_late_is_not_skipped(runtime, monkeypatch):
    # Claude Code loads the account's connectors in the background: a listing a moment old
    # can show one still connecting, though the next start has it.
    hb = runtime.heartbeat
    late = {"name": "claude.ai Drive", "status": "pending", "tools": []}
    ready = {
        "name": "claude.ai Drive",
        "status": "connected",
        "tools": ["search_files", "read_file"],
    }
    listings = [[late], [late], [ready]]
    monkeypatch.setattr(hb.runners, "account_servers", lambda who: listings.pop(0))
    hb._accounts = None
    owner = runtime.ws.owner()
    row = {"id": "claude:claude_ai_Drive", "found_in": heartbeat_service.ACCOUNT_PLACE}
    found, (status, _line, allowed, _refused) = hb._checked(owner, row)
    assert status == "ok" and found.spec["status"] == "connected"
    assert set(allowed) == {"search_files", "read_file"}


def test_an_account_connector_that_never_connects_is_skipped_after_a_bound(runtime, monkeypatch):
    hb = runtime.heartbeat
    late = {"name": "claude.ai Drive", "status": "pending", "tools": []}
    starts = []
    monkeypatch.setattr(hb.runners, "account_servers", lambda who: starts.append(1) or [late])
    hb._accounts = None
    row = {"id": "claude:claude_ai_Drive", "found_in": heartbeat_service.ACCOUNT_PLACE}
    _found, (status, _line, _a, _r) = hb._checked(runtime.ws.owner(), row)
    assert status == "error"
    assert len(starts) == 1 + heartbeat_service.ACCOUNT_RECHECKS


def test_a_read_answer_keyed_by_the_connectors_name_still_gives_its_items(runtime):
    # A real read answered under the connector's name, not its server: none of it was kept.
    hb = runtime.heartbeat
    part = {"items": [{"id": "p1", "title": "Launch plan"}], "cursor": "2026-10-27"}
    parts = {"claude.ai Notion": part}
    found = hb._part(parts, {"connectors": parts}, "claude_ai_Notion", "claude.ai Notion", True)
    assert found is part
    assert hb._part({"Notion": part}, {}, "claude_ai_Notion", "claude.ai Notion", True) is part
    assert hb._part({"other": part}, {}, "claude_ai_Notion", "claude.ai Notion", False) is None


def test_an_item_without_an_id_is_kept_by_its_link_or_title_and_counted(runtime):
    hb = runtime.heartbeat
    stats: dict = {}
    items, _cursor = hb._items(
        {
            "items": [
                {"title": "A", "url": "https://x/a"},
                {"title": "B", "at": "2026-10-27"},
                {"text": "no name"},
                "junk",
            ]
        },
        {"cursor": ""},
        stats=stats,
    )
    assert [i["id"] for i in items] == ["https://x/a", "B 2026-10-27"]
    assert stats == {"returned": 4, "kept": 2, "left_out": 0}


def test_an_item_that_names_what_to_leave_out_is_not_kept_and_is_counted(runtime):
    stats: dict = {}
    items, _cursor = runtime.heartbeat._items(
        {
            "items": [
                {"id": "1", "title": "Weekly Newsletter", "text": "news"},
                {"id": "2", "title": "Client call", "text": "They chose plan B"},
                {"id": "3", "title": "Your receipt", "text": "paid"},
            ]
        },
        {"cursor": "", "skip": "newsletter, receipt"},
        stats=stats,
    )
    assert [i["id"] for i in items] == ["2"]
    assert stats == {"returned": 3, "kept": 1, "left_out": 2}


def test_a_read_carries_each_connectors_goal_and_what_to_leave_out(runtime):
    prompt = runtime.heartbeat._read_prompt(
        [("s", "Mail", "2026-10-20", ["search"], "client decisions", "newsletter, receipt")]
    )
    assert "Look for: client decisions" in prompt
    assert "Leave out: newsletter, receipt" in prompt


def test_a_read_asks_to_list_first_and_open_only_the_newest(runtime):
    prompt = runtime.heartbeat._read_prompt([("s", "Notes", "2026-10-20", ["search", "fetch"])])
    assert "list or search" in prompt
    assert f"open at most {heartbeat_service.READ_OPENS}" in prompt
    # A quiet connector answers at once: a real Notion read opened six old pages to check dates.
    assert "none is newer than that time, answer at once" in prompt
    assert "when it shows no dates, open the newest" in prompt  # Notion's search shows none


def test_a_read_is_capped_in_turns(runtime, monkeypatch):
    seen = {}
    runners = runtime.runners
    monkeypatch.setattr(runners, "_ready", lambda spec: ["claude"])
    monkeypatch.setattr(
        runners,
        "_read_account",
        lambda spec, command, servers, limits, prompt, timeout: (
            seen.setdefault("limits", limits) and {"text": "{}"}
        ),
    )
    runners.read_many(
        runtime.ws.owner(),
        servers={"s": {"account": True}},
        allowed={"s": ["search"]},
        refused={},
        prompt="Read.",
    )
    limits = seen["limits"]
    assert limits[limits.index("--max-turns") + 1] == str(runners_mod.READ_TURNS)
    # A read carries its own short system prompt, not Claude Code's, on each of its turns.
    assert limits[limits.index("--system-prompt") + 1] == runners_mod.READ_SYSTEM
    assert "read-only" in runners_mod.READ_SYSTEM
    # And a dollar budget: a read that would spend more stops.
    assert limits[limits.index("--max-budget-usd") + 1] == str(runners_mod.READ_BUDGET_USD)


def test_a_rescan_that_misses_a_slow_account_connector_keeps_its_last_state():
    row = {
        "found_in": heartbeat_service.ACCOUNT_PLACE,
        "status": "ok",
        "enabled": True,
        "status_line": "Read-only: 3 tools. 10 new last beat.",
        "last_read_at": "2026-10-27T10:00:00Z",
    }
    keep = heartbeat_service.kept_state
    assert keep(row, "error", "Claude Code could not reach it just now.") == (
        "ok",
        "Read-only: 3 tools. 10 new last beat.",
    )
    assert keep(row, "unhealthy_sign_in", "Sign in again.") == (
        "unhealthy_sign_in",
        "Sign in again.",
    )
    never_read = {**row, "last_read_at": None}
    assert keep(never_read, "error", "x")[0] == "error"
    # Not connected yet when it was looked for: Connecting, never an error from discovery alone.
    assert keep(never_read, "connecting", "y") == ("connecting", "y")
    assert keep(row, "connecting", "y")[0] == "ok"


def test_an_account_connector_not_connected_at_discovery_is_connecting_not_failing(runtime):
    from workspace_server.heartbeat import connectors as conn_mod

    found = conn_mod.Found(
        id="claude:claude.ai Drive",
        name="claude.ai Drive",
        cli="claude",
        how="mcp",
        found_in=heartbeat_service.ACCOUNT_PLACE,
        spec={"account": True, "status": "pending", "tools": []},
    )
    status, line, _a, _r = runtime.heartbeat._check(found)
    assert status == "connecting"
    assert "Test read" in line


def test_a_test_read_reports_each_step_and_keeps_nothing(owner, runtime, fake_cli):
    # The setup check for any connector (v4 backend plan, 17.2): found, signed in, read-only
    # and one real sample read with its items, tokens, time and cost; no cursor, nothing seen.
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    done = owner.ok(owner.post(f"/api/heartbeat/connectors/claude:{NOTION}/test"))
    steps = {s["step"]: s for s in done["steps"]}
    assert list(steps) == ["found", "signed in", "read-only", "test read"]
    assert all(s["ok"] for s in done["steps"]) and done["passed"]
    read = steps["test read"]
    assert read["items"] >= 1 and read["seconds"] >= 0
    assert "per_day" in read and "returned" in read
    rows = {c["id"]: c for c in owner.ok(owner.get("/api/heartbeat/status"))["connectors"]}
    assert not rows[f"claude:{NOTION}"]["last_read_at"]  # nothing kept


def test_a_test_read_of_a_connector_that_is_not_there_fails_at_found(owner):
    done = owner.ok(owner.post("/api/heartbeat/connectors/claude:nothing_here/test"))
    assert not done["passed"]
    assert done["steps"][0] == {
        "step": "found",
        "ok": False,
        "line": "No connector claude:nothing_here was found.",
    }


def test_a_connector_that_needs_signing_in_says_how_for_its_source():
    # v4 backend plan, 17.2: the same "Sign in" for any connector, with its source's steps.
    sign_in = heartbeat_service.sign_in
    base = {"status": "unhealthy_sign_in", "how": "mcp", "found_in": "~/.claude.json"}
    account = sign_in(
        {
            **base,
            "cli": "claude",
            "name": "claude.ai Gmail",
            "found_in": heartbeat_service.ACCOUNT_PLACE,
        }
    )
    assert account["url"] == "https://claude.ai/settings/connectors"
    assert "Gmail" in account["steps"] and "claude.ai" in account["steps"]
    code = sign_in({**base, "cli": "claude", "name": "linear"})
    assert "/mcp" in code["steps"] and "linear" in code["steps"] and code["url"] is None
    assert "codex" in sign_in({**base, "cli": "codex", "name": "linear"})["steps"]
    assert "gemini" in sign_in({**base, "cli": "gemini", "name": "x"})["steps"].lower()
    assert "Credentials" in sign_in({**base, "cli": "n8n", "how": "n8n", "name": "Gmail"})["steps"]
    assert sign_in({**base, "status": "ok", "cli": "claude", "name": "x"}) is None


def test_the_connectors_say_how_to_sign_in_when_they_must(owner, fake_cli):
    fake_cli.account_init = ACCOUNT_INIT
    fake_cli.account_list = ACCOUNT_LIST
    found(owner)
    rows = owner.ok(owner.get("/api/heartbeat/status"))["connectors"]
    assert all("sign_in" in r for r in rows)


def test_a_connector_that_stays_quiet_is_read_less_often(owner, hb, clock, fake_cli):
    # A read costs tokens even when nothing is new: a connector that stays quiet is read less
    # often on the schedule (half the time it has been quiet, at most every two days), and its
    # own cadence comes back as soon as something new arrives.
    found(owner)
    owner.ok(owner.put("/api/heartbeat/settings", json={"quiet_hours": "off"}))
    owner.ok(owner.put("/api/heartbeat/connectors/claude:mail", json={"cadence_minutes": 60}))
    worker = hb_service.HeartbeatWorker(hb)

    def ticks(hours):
        read_at = []
        for _ in range(int(hours * 4)):
            clock.advance(timedelta(minutes=15))
            fake_cli.calls.clear()
            if worker.run_once() and fake_cli.reads():
                read_at.append(clock.now())
        return [(b - a).total_seconds() / 3600 for a, b in zip(read_at, read_at[1:], strict=False)]

    worker.run_once()  # the first read: the item is new
    fake_cli.items = []  # nothing new from here on
    assert ticks(30) == [1, 2, 4, 8]  # read hourly, it would be 29 reads
    fake_cli.items = [dict(ITEM, id="msg-2", title="Another")]
    assert ticks(14)[:2] == [1, 1]  # something new: hourly again, then spacing out
