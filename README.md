# JLite

> A relational database engine built from scratch in Java — designed as a learning project and a platform for exploring how core RDBMS components work end-to-end.

JLite is the Java counterpart to [SharpLite](https://github.com/saifadin1/SharpLite). It goes further: virtual-thread-powered concurrency, a TCP wire protocol for remote access, an MCP Server that exposes the engine as an AI tool, and a natural-language query agent that translates plain English into SQL and executes it.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Module Map](#module-map)
- [Pre-Implementation Checklist](#pre-implementation-checklist)
- [Feature Status](#feature-status)
- [Getting Started](#getting-started)
- [SQL Support](#sql-support)
- [Virtual Threads & Connection Pool](#virtual-threads--connection-pool)
- [TCP Server (Remote Access)](#tcp-server-remote-access)
- [MCP Server](#mcp-server)
- [Natural Language Agent](#natural-language-agent)
- [Execution Plan Visualiser](#execution-plan-visualiser)
- [TODO / Roadmap](#todo--roadmap)
- [Contributing](#contributing)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│  CLIENT LAYER                                                │
│  CLI Shell │ JDBC Driver │ NL Agent │ MCP Server            │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  ACCESS LAYER                                                │
│  TCP Server (VThread/conn)  │  Connection Pool (VThread)     │
│                Session Manager                               │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  PARSING LAYER                                               │
│  Lexer  →  Parser (recursive-descent)  →  Semantic Analyser │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  EXECUTION LAYER                                             │
│  Query Planner  │  Cost-Based Optimiser                      │
│  Volcano Iterator Engine                                     │
│  Aggregation │ Join Engine │ Sort / Limit                    │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  STORAGE LAYER                                               │
│  Catalogue │ Index Manager (B-Tree, Hash) │ Buffer Pool      │
│  Storage Engine: Heap Files + WAL                            │
└──────────────────┬───────────────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────────────┐
│  CONCURRENCY LAYER                                           │
│  Transaction Manager (ACID, MVCC, 2PL)  │  Lock Manager     │
└──────────────────────────────────────────────────────────────┘
```

---

## Module Map

```
jlite/
├── jlite-core/                  # Engine, parser, storage, execution
│   ├── src/main/java/jlite/
│   │   ├── lexer/               # Tokeniser
│   │   ├── parser/              # Recursive-descent parser → AST
│   │   ├── ast/                 # AST node types
│   │   ├── analyser/            # Semantic analysis, type resolution
│   │   ├── planner/             # Logical → physical query plan
│   │   ├── optimiser/           # Cost-based optimiser
│   │   ├── executor/            # Volcano iterator model
│   │   │   ├── operators/       # Scan, Filter, Project, Join, Agg, Sort
│   │   ├── catalogue/           # Schema registry, table/column metadata
│   │   ├── index/               # B-Tree, Hash index
│   │   ├── storage/             # Heap files, page manager, buffer pool
│   │   ├── wal/                 # Write-Ahead Log
│   │   ├── transaction/         # ACID, MVCC, 2PL
│   │   └── lock/                # Lock manager, deadlock detection
│   └── src/test/java/jlite/
│
├── jlite-server/                # TCP wire protocol server
│   └── src/main/java/jlite/server/
│       ├── TcpServer.java       # Virtual thread per connection
│       ├── ConnectionPool.java  # VThread-managed pool
│       ├── SessionManager.java  # Auth, isolation level, TX state
│       ├── protocol/            # Request/response wire format
│       └── handler/             # Query dispatch
│
├── jlite-cli/                   # REPL / interactive shell
│
├── jlite-jdbc/                  # JDBC 4.2 driver implementation
│
├── jlite-mcp/                   # MCP Server (tool exposure)
│   └── src/main/java/jlite/mcp/
│       ├── McpServer.java
│       └── tools/               # execute_query, describe_table, list_tables …
│
├── jlite-agent/                 # Natural-language → SQL agent
│   └── src/main/java/jlite/agent/
│       ├── NlAgent.java
│       ├── SchemaContextBuilder.java
│       └── SqlValidator.java
│
└── jlite-visualiser/            # Execution plan visualiser (future)
```

---

## Pre-Implementation Checklist

Before coding features, lock these contracts so modules evolve consistently.

### 1) Dependency Rules (must hold)

- `jlite-core` depends on no JLite module.
- Adapter modules (`jlite-cli`, `jlite-server`, `jlite-jdbc`, `jlite-mcp`, `jlite-agent`, `jlite-visualiser`) may depend on `jlite-core`.
- Adapter modules should not depend on each other unless there is a strict, documented reason.
- Protocol/wire concerns stay in adapter modules; SQL semantics stay in core.

### 2) Communication Contract (single path)

All entry points should execute queries through one shared application boundary.

```
CLI/TCP/JDBC/MCP/NL Agent
          -> Query Application Service
          -> Parser -> Analyser -> Planner -> Optimiser -> Executor
          -> Storage/WAL/Transaction
```

This avoids duplicating execution logic in each adapter.

### 3) Error Contract (define once)

- Standardise error categories: parse, semantic, planning, execution, transaction, internal.
- Include stable fields in all responses: `code`, `message`, `details`, `correlationId`.
- Map these consistently to CLI text, TCP error frames, JDBC `SQLException`, and MCP tool errors.

### 4) Implementation Order (recommended)

1. Parser + minimal AST for `SELECT ... FROM ... WHERE ...`
2. Semantic analyser (table/column resolution)
3. SeqScan + Filter + Project executor path
4. INSERT + in-memory catalogue integration
5. TCP protocol request/response loop
6. JDBC over TCP
7. MCP tools and NL agent integration

### 5) Definition of Done per feature

- Unit tests in `jlite-core` for parser/analyser/executor behavior.
- At least one end-to-end test through a real adapter (`cli`, `server`, or `jdbc`).
- Error path covered (invalid SQL / unknown table / type mismatch).
- README updated if behavior or protocol changed.

---

## Feature Status

| Component | Status |
|---|---|
| Lexer | ⬜ TODO |
| Parser (SELECT, WHERE) | ⬜ TODO |
| AST nodes | ⬜ TODO |
| Semantic analyser | ⬜ TODO |
| Heap file storage | ⬜ TODO |
| Buffer pool (LRU) | ⬜ TODO |
| Catalogue (in-memory) | ⬜ TODO |
| Volcano executor | ⬜ TODO |
| CREATE / DROP TABLE | ⬜ TODO |
| INSERT / UPDATE / DELETE | ⬜ TODO |
| SELECT + WHERE | ⬜ TODO |
| JOIN (NL, hash, merge) | ⬜ TODO |
| GROUP BY / HAVING | ⬜ TODO |
| ORDER BY / LIMIT | ⬜ TODO |
| B-Tree index | ⬜ TODO |
| Hash index | ⬜ TODO |
| WAL + crash recovery | ⬜ TODO |
| Transactions (ACID) | ⬜ TODO |
| MVCC | ⬜ TODO |
| Lock manager | ⬜ TODO |
| TCP Server | ⬜ TODO |
| Virtual thread connection pool | ⬜ TODO |
| CLI Shell / REPL | ⬜ TODO |
| JDBC Driver | ⬜ TODO |
| MCP Server | ⬜ TODO |
| NL Agent | ⬜ TODO |
| Execution plan visualiser | ⬜ TODO |

---

## Getting Started

**Prerequisites:** Java 21+ (virtual threads require Project Loom, GA in Java 21).

```bash
git clone https://github.com/your-handle/jlite.git
cd jlite
mvn -pl jlite-cli exec:java -Dexec.mainClass=jlite.cli.Repl
```

The CLI starts a REPL session:

```
JLite> CREATE TABLE users (id INT, name TEXT, age INT);
JLite> INSERT INTO users VALUES (1, 'Alice', 30);
JLite> SELECT name, age FROM users WHERE age > 25;
```

To start the TCP server:

```bash
mvn -pl jlite-server exec:java -Dexec.mainClass=jlite.server.TcpServer -Dexec.args="--port 5432"
```

---

## SQL Support

JLite targets a practical subset of ANSI SQL.

### DDL

```sql
CREATE TABLE <name> (<col> <type> [NOT NULL] [PRIMARY KEY], ...);
DROP TABLE <name>;
ALTER TABLE <name> ADD COLUMN <col> <type>;
ALTER TABLE <name> DROP COLUMN <col>;
CREATE INDEX <name> ON <table>(<col>);
DROP INDEX <name>;
```

### DML

```sql
INSERT INTO <table> [(<cols>)] VALUES (...), (...);
UPDATE <table> SET <col>=<expr> [WHERE <condition>];
DELETE FROM <table> [WHERE <condition>];
```

### DQL

```sql
SELECT [DISTINCT] <cols> | *
FROM <table> [AS <alias>]
  [JOIN <table> ON <condition>]
  [WHERE <condition>]
  [GROUP BY <cols>]
  [HAVING <condition>]
  [ORDER BY <col> [ASC|DESC]]
  [LIMIT <n> [OFFSET <m>]];
```

### Transactions

```sql
BEGIN;
COMMIT;
ROLLBACK;
SET ISOLATION LEVEL READ_COMMITTED | REPEATABLE_READ | SERIALIZABLE;
```

### Supported types

`INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `TEXT`, `VARCHAR(n)`, `BOOLEAN`, `DATE`, `TIMESTAMP`

---

## Virtual Threads & Connection Pool

JLite uses Java 21 virtual threads (Project Loom) throughout.

Each inbound TCP connection is dispatched to a virtual thread — no thread pool exhaustion under high concurrency. The connection pool is also managed via virtual threads: each idle connection waits on a virtual thread that parks cheaply rather than blocking a carrier thread.

Key design decisions:

- `Thread.ofVirtual().start(handler)` per accepted socket — no explicit executor needed for basic use.
- The bounded connection pool uses a `Semaphore` to cap active connections; waiting acquires park the virtual thread at zero OS cost.
- Pinning to carrier threads is avoided: no `synchronized` blocks in hot paths — `ReentrantLock` or `java.util.concurrent` primitives are used instead.
- Structured concurrency (`StructuredTaskScope`) is used for fan-out query phases (e.g. parallel index probes).

---

## TCP Server (Remote Access)

The TCP server exposes JLite over a simple binary wire protocol, allowing remote clients (CLI, JDBC driver, any TCP socket) to connect.

```
Client                          JLite TCP Server
  |  --- Handshake (version) -->  |
  |  <-- Handshake OK ----------  |
  |  --- AuthRequest ---------->  |
  |  <-- AuthOK / AuthFail -----  |
  |  --- QueryRequest(sql) ----->  |
  |  <-- ResultSet / Error -----  |
  |  --- Close ----------------->  |
```

Message framing uses a 4-byte length prefix followed by a JSON (later: binary) payload. This is intentionally simple — the goal is correctness first, not PostgreSQL wire-protocol compatibility.

---

## MCP Server

The MCP (Model Context Protocol) server exposes JLite as a set of AI-callable tools. Any MCP-compatible agent (Claude, Cursor, etc.) can connect and interact with the database through these tools.

### Tools exposed

| Tool | Description |
|---|---|
| `execute_query` | Execute a SQL string, return rows as JSON |
| `list_tables` | Return all table names in the catalogue |
| `describe_table` | Return schema (columns, types, indexes) for a table |
| `get_execution_plan` | Return the query plan for a SQL string (EXPLAIN) |
| `begin_transaction` | Start a transaction, return a TX token |
| `commit_transaction` | Commit by TX token |
| `rollback_transaction` | Rollback by TX token |

The MCP server runs over stdio (default) or SSE transport, making it usable both locally and via a remote endpoint.

---

## Natural Language Agent

The NL Agent is a thin layer that accepts an English question, builds a schema-aware prompt, calls an LLM (Claude by default), receives SQL back, validates it against the live catalogue, and executes it through the normal query pipeline.

```
"Show me all users who signed up last month"
        ↓
  SchemaContextBuilder   (injects live table/column names)
        ↓
  LLM call (Claude claude-sonnet-4-20250514)
        ↓
  SQL: SELECT * FROM users WHERE signup_date >= DATE_TRUNC('month', NOW() - INTERVAL '1 month')
        ↓
  SqlValidator           (parse check, table/col existence)
        ↓
  JLite query pipeline   → ResultSet
        ↓
  Formatted answer to user
```

The agent is intentionally stateless between turns. Conversation history (if needed) is managed by the calling application.

---

## Execution Plan Visualiser

> Future feature — tracked in TODO below.

The visualiser will render the physical query plan as an interactive tree. Each node shows operator type, estimated cost, actual row count (post-execution), and whether an index was used. Hot paths (high-cost nodes) are highlighted. The goal is to make the cost-based optimiser's decisions legible.

---

## TODO / Roadmap

### Lexer
- [ ] Tokenise all SQL keywords: `SELECT`, `FROM`, `WHERE`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `JOIN`, `ON`, `GROUP BY`, `ORDER BY`, `HAVING`, `LIMIT`, `OFFSET`, `DISTINCT`, `AS`, `INDEX`
- [ ] Recognise all operators: `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `LIKE`, `IS NULL`, `IS NOT NULL`
- [ ] Handle string literals (single-quoted), numeric literals (int, float), identifiers, comments (`--`, `/* */`)
- [ ] Emit position information (line, column) for error messages
- [ ] Support Unicode identifiers

### Parser
- [ ] Recursive-descent parser producing a typed AST
- [ ] SELECT with all clauses: WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, DISTINCT
- [ ] JOINs: INNER, LEFT OUTER, RIGHT OUTER, CROSS
- [ ] Subqueries in WHERE and FROM
- [ ] INSERT (single row and multi-row VALUES)
- [ ] UPDATE with arbitrary SET expressions
- [ ] DELETE with WHERE
- [ ] CREATE TABLE with column constraints (NOT NULL, PRIMARY KEY, UNIQUE, DEFAULT, FOREIGN KEY)
- [ ] DROP TABLE, ALTER TABLE (ADD COLUMN, DROP COLUMN, RENAME COLUMN)
- [ ] CREATE INDEX, DROP INDEX
- [ ] Transaction statements: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
- [ ] Expression parsing: arithmetic, boolean, function calls, CASE/WHEN, CAST

### AST
- [ ] Define sealed interface / record hierarchy for all statement and expression types
- [ ] Visitor interface for AST traversal (used by analyser, planner, visualiser)
- [ ] AST pretty-printer for debugging

### Semantic Analyser
- [ ] Resolve table and column references against the catalogue
- [ ] Type-check expressions (no comparing TEXT to INT without explicit CAST)
- [ ] Validate aggregate usage (no bare non-aggregated columns outside GROUP BY)
- [ ] Expand `SELECT *` to explicit column list
- [ ] Alias resolution and scope handling for subqueries

### Storage Engine
- [ ] Fixed-size page abstraction (default 4 KB)
- [ ] Heap file: linked list of pages, free-space tracking per page
- [ ] Slotted page layout: slot array at page header, tuples growing from the end
- [ ] Tuple serialisation/deserialisation for all supported types
- [ ] Page manager: open, read, write, sync pages from disk
- [ ] Buffer pool: fixed-size LRU page cache with dirty-page tracking
- [ ] Buffer pool eviction: LRU with clock-sweep as alternative
- [ ] WAL (Write-Ahead Log): append-only log file, REDO records for every write
- [ ] WAL flushing policy: configurable (sync-on-commit vs group commit)
- [ ] Crash recovery: ARIES-style redo pass on startup
- [ ] Checkpoint mechanism to bound recovery time
- [ ] File-per-table layout on disk with a root directory file for the catalogue

### Catalogue
- [ ] In-memory catalogue: tables, columns, types, constraints, indexes
- [ ] Catalogue persistence: serialise to/from a catalogue file on disk
- [ ] System tables: `jlite_tables`, `jlite_columns`, `jlite_indexes`
- [ ] Schema versioning and migration support (future)

### Index Manager
- [ ] B-Tree index: insert, delete, point lookup, range scan
- [ ] B-Tree page splits and merges
- [ ] Hash index: static hash, bucket overflow chains
- [ ] Index-only scan (covering index) where possible
- [ ] Multi-column composite indexes
- [ ] Index statistics (cardinality, key distribution) used by optimiser

### Query Planner
- [ ] Convert AST → logical plan (LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate, LogicalSort, LogicalLimit)
- [ ] Convert logical plan → physical plan (choosing operators based on index availability, row estimates)
- [ ] EXPLAIN output: return physical plan as text tree

### Query Optimiser
- [ ] Predicate pushdown: move filters as close to the scan as possible
- [ ] Projection pushdown: eliminate unused columns early
- [ ] Index selection: choose index scan over heap scan when selectivity is high
- [ ] Join order optimisation: dynamic programming over join order for ≤ N tables (configurable, default 8)
- [ ] Join algorithm selection: nested-loop (small tables), hash join (large tables, equality), merge join (pre-sorted inputs)
- [ ] Constant folding and trivially-true/false predicate elimination
- [ ] Column statistics collection (`ANALYZE` command) feeding cost estimates
- [ ] Subquery unnesting / decorrelation

### Execution Engine (Volcano / Iterator Model)
- [ ] `Operator` interface: `open()`, `next(): Tuple`, `close()`
- [ ] SeqScan: full table scan
- [ ] IndexScan: B-Tree or hash index probe
- [ ] Filter: evaluate predicate per tuple
- [ ] Project: evaluate expression list, emit projected tuple
- [ ] NestedLoopJoin, HashJoin, MergeJoin
- [ ] HashAggregate (GROUP BY + aggregate functions: COUNT, SUM, AVG, MIN, MAX)
- [ ] Sort (external sort for data exceeding buffer pool)
- [ ] Limit / Offset
- [ ] Distinct (hash-based deduplication)
- [ ] Insert, Update, Delete operators
- [ ] Expression evaluator: arithmetic, boolean, comparison, CASE, CAST, built-in functions

### Transaction Manager
- [ ] BEGIN / COMMIT / ROLLBACK
- [ ] SAVEPOINT / ROLLBACK TO SAVEPOINT
- [ ] Isolation levels: Read Committed, Repeatable Read, Serializable
- [ ] MVCC: version chain per tuple, visibility check based on transaction snapshot
- [ ] Vacuum / dead-tuple cleanup
- [ ] 2PL (two-phase locking) as an alternative to MVCC (configurable)

### Lock Manager
- [ ] Row-level shared/exclusive locks
- [ ] Table-level locks (schema change protection)
- [ ] Intention locks (IS, IX, SIX) for hierarchical locking
- [ ] Deadlock detection: wait-for graph cycle detection, victim selection
- [ ] Lock timeout configuration

### Virtual Threads & Connection Pool
- [ ] One virtual thread per accepted TCP connection (no explicit pool)
- [ ] Bounded connection pool using `Semaphore` + virtual threads for waiters
- [ ] Replace all `synchronized` hot-path blocks with `ReentrantLock` to avoid virtual-thread pinning
- [ ] Structured concurrency (`StructuredTaskScope`) for parallel query phases
- [ ] Connection lifecycle: idle timeout, keepalive ping, graceful drain on shutdown
- [ ] Connection pool metrics: active count, wait queue depth, acquisition latency histogram

### TCP Server
- [ ] ServerSocket accept loop dispatching to virtual threads
- [ ] Length-prefixed binary frame: `[4-byte length][payload bytes]`
- [ ] Wire protocol messages: Handshake, AuthRequest, AuthOK, QueryRequest, ResultSetResponse, ErrorResponse, CloseRequest
- [ ] TLS support via `SSLServerSocket`
- [ ] Graceful shutdown: drain in-flight requests, close idle connections
- [ ] Server configuration: port, max connections, bind address, auth mode

### CLI Shell
- [ ] REPL with readline-style history and arrow-key editing (JLine3)
- [ ] `\tables` — list all tables
- [ ] `\describe <table>` — show schema
- [ ] `\explain <sql>` — print query plan
- [ ] `\timing` — toggle query timing output
- [ ] `\connect host:port` — connect to remote TCP server
- [ ] Syntax highlighting for SQL keywords
- [ ] Multi-line statement entry (continue on `;`)
- [ ] `\import <file.sql>` — execute a SQL script file

### JDBC Driver
- [ ] Implement `java.sql.Driver`, `Connection`, `Statement`, `PreparedStatement`, `ResultSet`
- [ ] URL format: `jdbc:jlite://host:port/dbname` or `jdbc:jlite:file:/path/to/db`
- [ ] `PreparedStatement` with `?` parameter binding
- [ ] `ResultSetMetaData` with column names and types
- [ ] Batch inserts via `addBatch` / `executeBatch`
- [ ] Transaction control via `Connection.setAutoCommit(false)`
- [ ] Register with `ServiceLoader` for automatic JDBC driver discovery

### MCP Server
- [ ] Implement MCP spec: stdio and SSE transports
- [ ] Tool: `execute_query(sql: string) → rows[]`
- [ ] Tool: `list_tables() → string[]`
- [ ] Tool: `describe_table(table: string) → schema`
- [ ] Tool: `get_execution_plan(sql: string) → plan_tree`
- [ ] Tool: `begin_transaction() → tx_token`
- [ ] Tool: `commit_transaction(tx_token: string)`
- [ ] Tool: `rollback_transaction(tx_token: string)`
- [ ] Authentication token for MCP server access
- [ ] Resource: expose each table as a browsable MCP resource

### Natural Language Agent
- [ ] `SchemaContextBuilder`: introspect live catalogue, render as compact schema prompt
- [ ] Prompt template: system prompt with schema context + user question → SQL
- [ ] LLM call via Anthropic Java SDK (Claude claude-sonnet-4-20250514)
- [ ] `SqlValidator`: parse the returned SQL through JLite's own parser, catch syntax errors before execution
- [ ] Retry loop: on validation failure, feed the error back to the LLM for self-correction (max 3 attempts)
- [ ] Multi-turn conversation support: maintain message history for follow-up questions
- [ ] Result formatting: render row data as a readable table or prose summary
- [ ] Safety guardrails: refuse destructive statements (DROP, DELETE without WHERE) unless explicitly confirmed

### Execution Plan Visualiser
- [ ] `EXPLAIN` command returns a serialisable plan tree (JSON)
- [ ] Web-based interactive tree renderer (React or plain HTML/SVG)
- [ ] Each node shows: operator type, estimated rows, actual rows (post-run), estimated cost, actual time
- [ ] Hot-path highlighting: nodes exceeding X% of total cost are marked in amber/red
- [ ] Index usage annotation: green badge when an index scan is used
- [ ] Diff view: compare plan before/after `ANALYZE` or query rewrite
- [ ] Embed visualiser in CLI via `\visualise <sql>` (opens browser tab)

### Testing & Quality
- [ ] Unit tests for every lexer token type
- [ ] Parser round-trip tests: parse → unparse → re-parse, assert AST equality
- [ ] Storage tests: write pages, simulate crash, verify recovery
- [ ] Concurrency tests: interleaved transactions under multiple virtual threads
- [ ] TPC-H benchmark subset for performance regression testing
- [ ] Fuzzing the parser with random token streams (no crashes allowed)

---

## Contributing

Pick any TODO above, open an issue to claim it, then send a PR. The project is deliberately modular — most components can be developed and tested in isolation before wiring them together.

### Code style

- Java 21, preview features enabled where useful
- Google Java Format (enforced via Spotless Maven plugin)
- All public API documented with Javadoc
- No external runtime dependencies in `jlite-core` — it must remain a zero-dependency library

### Build

```bash
mvn clean install
mvn -pl jlite-core test
mvn -pl jlite-server exec:java
```

---

## About

JLite is a learning project. The goal is not to replace PostgreSQL — it is to understand, from first principles, why PostgreSQL makes the choices it does.

Inspired by [SharpLite](https://github.com/saifadin1/SharpLite), [CMU 15-445](https://15445.courses.cs.cmu.edu/), and [Architecture of a Database System](http://db.cs.berkeley.edu/papers/fntdb07-architecture.pdf).
