# What Gnomon costs a day

Gnomon spends model tokens on a handful of kinds of work. This page gives what each one cost
when it was measured, what a light, typical and heavy day add up to, and the settings that
change it.

Background work runs on Claude Haiku 5.5 through Claude Code by default (the ingest model,
`--model haiku`). Its list prices are $0.10 per million input tokens, $0.50 per million output
tokens, $0.01 per million cached tokens read and $0.20 per million written to the one-hour
cache. On a Claude plan the tokens count against the plan's usage instead of costing dollars,
so this page gives both.

## What each piece of work costs

Measured on October 28, 2026 on the entrepreneur demo Brain, unless it says otherwise.

| Work | When | Tokens | Cost |
|---|---|---|---|
| Claude Code itself, as Gnomon starts it | in every run below | 3,900 | $0.0007 |
| A beat with no connector due | every beat | 0: no model call | none |
| Reads through n8n, folders and Scout | each beat | 0 | none |
| A connector read through Claude Code | each scheduled read | 33,000 to 209,000 | $0.004 to $0.010 |
| The nightly distill | once a night | 5,500 for 5 items, 25,000 for 50 | $0.0099 for 50 |
| An answer through the Brain, two turns | each time an AI app asks | 29,000 | $0.0034 |
| A Work with AI job or Run now, a five-line note | when you start one | 52,000 | $0.0035 |
| A Test bench run: no Brain, full Brain, custom | when you run one | 12,700 | $0.0013 |
| The side-by-side check of notes that may disagree, on Sonnet 5.5 | in the background, at most 60 pairs a day | 3,400 a call | about $0.009 a call |

About connector reads. A read lists what changed, opens the newest items and answers; each
turn reads Claude Code's prompt and the connector's own tools again, from cache, so a
connector with many large tools costs more even when nothing is new. Real Test reads, October
28, on Haiku 5.5:

| Connector | Items | Tokens | Cost | Time |
|---|---|---|---|---|
| Google Drive, average of 10 reads | 5 | 57,700 (48,000 to 84,000) | $0.0046 | 34 s |
| A production app | 7 | 51,000 | $0.005 | 25 s |
| Spotify | 6 | 33,000 | $0.005 | 26 s |
| HubSpot, a year back | 17 | 142,000 | $0.0066 | 35 s |
| HubSpot, nothing new | 0 | 91,000 | $0.0034 | 24 s |
| Notion, a year back | 5 | 209,000 | $0.010 | 40 s |
| Notion, nothing new | 0 | 191,000 | $0.0053 | 21 s |

- Most of those tokens are cache reads, which is why they cost so little.
- Reading several connectors in one run costs less than reading them one by one. A real
  beat reading Drive, Notion and HubSpot together took 277,000 tokens in 50 seconds, against
  about 400,000 for the three read apart.
- Since October 29 a read makes one list or search call, then opens at most the 3 newest
  items in one step (it opened 6 before) and answers: no tool loop. The table above was
  measured with 6 opens, so reads now cost at most that.
- Each read has a cap on turns and on spend ($0.05 a connector). A read that reaches either
  fails and says so, instead of passing as nothing new.
- A read whose list shows dates, none newer than the last read, answers at once: that halved
  HubSpot's quiet read (188,000 tokens to 91,000). Notion's search shows no dates, so its
  quiet read still opens pages to check them, which is why it costs the most.

About the side-by-side check: it runs on Sonnet 5.5. On the 80 labeled pairs of
`scripts/safeguards_eval.py`, Sonnet named the right relation in 77, Haiku in 69 and the
earlier default (Opus) in 79. Sonnet is enough, at half Opus's price ($0.018 a call).

What makes up a run's tokens:
- Claude Code's own prompt is about 3,900 tokens when Gnomon starts it: no built-in tools,
  none of your own settings, only the MCP servers the run needs.
- The Brain's tools are about 790 tokens: three tools (`context`, `brain`, `propose`) and
  their short introduction. They were about 6,600 (15 grouped tools) before October 29. The
  app's own tools, which a job uses, are at most about 7,200.
- Each further turn of a run reads all of that again, from cache. Fewer turns is the biggest
  saving. That is why a job's packet carries its project: before it did, the same note took
  13 tool calls and 142,000 tokens.

An AI app you use yourself (Claude Code, Claude Desktop, Codex) pays for its own prompt and
tools on every turn, as it would without Gnomon. The Brain adds its three tools (about 790
tokens) to each session, plus its packets. Measured on the knowledge-company starter Brain:

- the first packet is about 830 tokens when the app loaded the Brain's startup file, and
  1,450 when it did not;
- a new ask in the same conversation is about 450 tokens;
- an edit is about 80 tokens.

On a 258-note Brain the same three are about 2,050, 1,180 and 100 tokens. A writing request
there carries the person's own writing rules whole. Run headless with all of Claude Code's built-in
tools and your own settings, the same two-turn answer came to 105,000 tokens, not 29,000.

## A day

How often connectors are read is most of it. Since October 29:

- The heartbeat is off until a connector is on.
- A connector read through Claude Code is read daily unless you set it otherwise.
- A connector whose reads find nothing new spaces out on its own: half the time it has been
  quiet, at most every two days, and back to daily when something new comes in.
- An app that an n8n workflow reads is not also read through Claude Code.

The table is estimated from the October 28 measurements above, with one read a day. Before
October 29 the default was every 4 hours, about 5 reads a day per connector, and a typical
day came to about 1.6 million tokens.

| | Light | Typical | Heavy |
|---|---|---|---|
| Connectors read through Claude Code | 1 (Drive) | 3 (Drive, Notion, HubSpot) | 6 |
| Reads a day of each | 1 | 1 | 1 |
| Work with AI jobs | 0 | 2 | 10 |
| Test runs after the distill | 0 | 10 | 20 (the daily cap) |
| Side-by-side check calls | 3 | 10 | 30 |
| **Tokens** | **about 0.07 million** | **about 0.5 million** | **about 1.5 million** |
| **of which connector reads** | 0.06 million | 0.28 million | 0.6 million |
| **Cost on Haiku 5.5, the check on Sonnet 5.5** | **about $0.01** | **about $0.04 to $0.10** | **about $0.25** |

A typical day is one beat reading the three connectors together, at most about 277,000
tokens, and about 0.5 million tokens in all. That is roughly a third of the earlier default.
Notion stays the expensive one, because its search shows no dates. An n8n read of an app
costs nothing. The numbers come down further when a connector stays quiet.

## Settings that change it

- **Read through n8n.** A connector read by an n8n workflow on your computer costs no tokens.
  The Heartbeat card offers it for every app it can draft a workflow for.
- **A connector's own cadence.** In the Heartbeat card, each connector has its own: every 5,
  15, 30 or 60 minutes or every 4 hours for fresher news and more tokens, or daily (the
  default). Every 4 hours, the typical day's three connectors take about 1.4 million tokens.
  Turn off a connector you don't need. A quiet connector already spaces out its reads.
- **The ingest model.** The distill, the judge and the world drafts run on it: Haiku by
  default, which passed every gate of the recorded day.
- **Side-by-side checks.** The Brain's policy `model_checks` (on by default) turns the
  background check off. Checks 1 and 2, by words and figures, still run, and cost nothing.
- **The Test bench after a distill.** At most 20 test runs a day, graded on Haiku. A test
  whose packet has not changed keeps its last answer instead of running again.
- **Start a job from its project.** Its packet then holds where the project stands, so the
  run doesn't search for it.

## Your own numbers

- **Before you turn a connector on:** its Test read in the Heartbeat card (or
  `uv run contxt connectors check`) does one real read and shows the items, tokens, time and
  the estimated cost per beat and per day. Nothing is kept.
- **Afterwards:** `uv run contxt report` shows what context cost by day and by line, from the
  ledger that every run Gnomon starts writes to. Add `--claude-code` to count Claude Code's
  own transcripts on this computer exactly (counts only).
- **Each job:** the project's timeline shows each Work with AI job's model, tokens and cost.
