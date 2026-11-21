# Implementation Plan: MySQL Wire Protocol to Trino REST API Gateway (`trino-mysql-proxy`)

## 1. Executive Summary

This document outlines the architecture and phased implementation plan for a new standalone Trino service module, `service/trino-mysql-proxy`, that accepts client connections over the **MySQL binary wire protocol (v4.1 / HandshakeV10)** and translates them into **Trino Client REST API (`/v1/statement`)** calls.

The design is modeled directly on **Apache Doris Frontend's (FE) MySQL network protocol layer**, adapted to Trino's Airlift/Guice service conventions (`service/trino-proxy`) and backed by Trino's existing `client/trino-client` REST client library.

---

## 2. Reference Analysis: How Apache Doris Receives MySQL Protocol Requests

In Apache Doris (`apache/doris`), the MySQL wire protocol server is embedded inside the Java Frontend (`fe/fe-core`) and listens on `Config.query_port` (default `9030`).

### 2.1 End-to-End Request Lifecycle in Apache Doris

```text
MySQL Client (mysql CLI / JDBC / PyMySQL)
   │  TCP (default port 9030)
   ▼
DorisFE.start() [DorisFE.java:308-312]
   └─► QeService.start() [QeService.java:45-67]
         └─► FeServerStarterFactory.createMysqlServerStarter()
               └─► OssFeServerStarterProvider.OssMysqlServerStarter [OssFeServerStarterProvider.java:117-138]
                     └─► MysqlServer.start() [MysqlServer.java:58-92]
                           │  JBoss XNIO: xnioWorker.createStreamConnectionServer(port, acceptListener)
                           ▼
                     AcceptListener.handleEvent() [AcceptListener.java:54-87]
                           │  channel.accept() -> StreamConnection
                           │  ConnectContext.forMysql(connection)
                           │  channel.getWorker().execute(() -> handleConnection(...))
                           ▼
                     AcceptListener.handleConnection() [AcceptListener.java:90-160]
                           ├─► (Optional) ProxyProtocolHandler.handle()
                           ├─► MysqlProto.negotiate(context) [MysqlProto.java:101-259]
                           │     1. Send MysqlHandshakePacket (protocol=10, 20-byte scramble, capabilities)
                           │     2. Read client response via MysqlChannel.fetchOnePacket()
                           │     3. If CLIENT_SSL: parse MysqlSslPacket -> MysqlSslContext.sslExchange()
                           │        -> switch MysqlChannel to TLS mode -> read encrypted MysqlAuthPacket
                           │     4. Parse MysqlAuthPacket (capabilities, user, authResponse, db, attrs)
                           │     5. Authenticate via AuthenticatorManager
                           │     6. Initialize catalog & database; send OK / ERR packet
                           └─► new MysqlConnectProcessor(context)
                               MysqlProtocolAdapter.of(context).startAcceptQuery(context, processor)
                                 └─► MysqlChannel.startAcceptQuery() [MysqlChannel.java:629-632]
                                       Attaches ReadListener to XNIO SourceChannel & calls resumeReads()
                                       ▼
                     ReadListener.handleEvent() [ReadListener.java:48-85]
                           │  1. Runs on XnioIoThread; calls protocol.suspendAcceptQuery() so only one
                           │     command runs at a time per connection
                           │  2. Submits task to worker thread pool:
                           │       connectProcessor.processOnce()
                           │       if (!ctx.isKilled()) protocol.resumeAcceptQuery()
                           ▼
                     MysqlConnectProcessor.processOnce() [MysqlConnectProcessor.java:588-628]
                           │  1. Reset QueryState & channel sequenceId = 0
                           │  2. packetBuf = channel.fetchOnePacket() [MysqlChannel.java:340-371]
                           │     - Reads 4-byte MySQL header (3-byte LE length + 1-byte sequenceId)
                           │     - Validates sequenceId, reassembles >= 16 MiB (0xFFFFFF) split packets
                           │     - Enforces max_allowed_packet and handles TLS unwrap when SSL is on
                           │  3. dispatch() [MysqlConnectProcessor.java:383-459]
                           │     - Reads 1st byte -> MysqlCommand enum
                           │     - Routes: COM_QUERY, COM_INIT_DB, COM_QUIT, COM_PING, COM_FIELD_LIST,
                           │       COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_FETCH,
                           │       COM_STMT_SEND_LONG_DATA, COM_STMT_CLOSE, COM_STMT_RESET,
                           │       COM_CHANGE_USER, COM_SET_OPTION, COM_RESET_CONNECTION, COM_STATISTICS
                           │  4. finalizeCommand() -> MysqlProtocolAdapter.finishCommand()
                           │     - Sends terminal OK / EOF / ResultSetEnd (0xFE) / ERR packet & flushes
                           ▼
                     MysqlResultSender [MysqlResultSender.java] & MysqlSerializer [MysqlSerializer.java]
                           - Column count -> ColumnDefinition41 packets -> EOF (if !CLIENT_DEPRECATE_EOF)
                           - Row packets (text length-encoded strings OR binary null-bitmap + typed values)
                           - Final OK/EOF/ERR packet via MysqlChannel.sendOnePacket() / sendAndFlush()
```

### 2.2 Key Doris Source Files Mapped to Responsibilities

| Responsibility | Apache Doris File (`fe/fe-core/src/main/java/org/apache/doris/`) | Key Mechanism |
| --- | --- | --- |
| Service bootstrap | `DorisFE.java`, `qe/QeService.java`, `tls/server/OssFeServerStarterProvider.java` | Starts `MysqlServer` on `Config.query_port` (9030) |
| Non-blocking TCP server | `mysql/MysqlServer.java` | JBoss XNIO `XnioWorker` + `AcceptingChannel<StreamConnection>` + worker `ExecutorService` |
| Connection accept & lifecycle | `mysql/AcceptListener.java` | Accepts socket, creates `ConnectContext`, runs handshake on worker thread, registers `ReadListener` |
| Event-driven request trigger | `mysql/ReadListener.java` | Suspends reads on IO thread, executes `processOnce()` on worker pool, resumes reads on completion |
| Packet framing & TLS | `mysql/MysqlChannel.java` | 4-byte MySQL packet header, sequence ID tracking, >16 MiB packet reassembly/splitting, `SSLEngine` wrap/unwrap, 2 MiB send buffer |
| Handshake & auth flow | `mysql/MysqlProto.java`, `mysql/MysqlHandshakePacket.java`, `mysql/MysqlAuthPacket.java`, `mysql/MysqlSslPacket.java` | HandshakeV10, capability intersection, SSL upgrade, auth plugin negotiation |
| Command routing | `qe/MysqlConnectProcessor.java`, `mysql/MysqlCommand.java` | 1-byte command byte switch (`COM_QUERY`, `COM_INIT_DB`, prepared statements, etc.) |
| Wire encoding & result streaming | `mysql/MysqlSerializer.java`, `mysql/protocol/MysqlResultSender.java`, `mysql/protocol/MysqlProtocolAdapter.java`, `mysql/MysqlOkPacket.java`, `mysql/MysqlErrPacket.java`, `mysql/MysqlEofPacket.java`, `mysql/MysqlResultSetEndPacket.java` | Little-endian ints, length-encoded ints/strings, `ColumnDefinition41`, text & binary rows, `CLIENT_DEPRECATE_EOF` handling |

---

## 3. Target Architecture in Trino

### 3.1 Placement & Runtime Model

Create a new standalone Maven module under `service/`:

- **Module path**: `service/trino-mysql-proxy` (registered in root `pom.xml` alongside `service/trino-proxy` and `service/trino-verifier`).
- **Bootstrap framework**: Airlift (`Bootstrap`, `NodeModule`, `JmxModule`, `LogJmxModule`, `TracingModule`, `Configuration`), matching `service/trino-proxy/src/main/java/io/trino/proxy/TrinoProxy.java`.
- **Network transport**: **Netty** (`io.netty:netty-transport`, `io.netty:netty-handler`, `io.netty:netty-codec`) instead of JBoss XNIO. Netty is already version-managed in Trino's root `pom.xml` (`io.netty:netty-bom`), provides native `ByteToMessageDecoder` / `MessageToByteEncoder` framing pipelines, clean `SslHandler` mid-connection TLS upgrades, and channel writability backpressure.
- **Trino REST client**: Reuse Trino's own `client/trino-client` (`StatementClientFactory`, `StatementClient`, `ClientSession`, `ProtocolHeaders`, `QueryResults`, `OkHttpClient`). This avoids re-implementing Trino's HTTP protocol (`/v1/statement`, `/v1/statement/queued/...`, `/v1/statement/executing/...`, retry logic, response header parsing, and `FixJsonDataUtils`).

### 3.2 High-Level Architecture Diagram

```text
┌────────────────────────────────────────────────────────┐
│   MySQL Clients (mysql CLI, Connector/J, PyMySQL, BI)  │
└──────────────────────────┬─────────────────────────────┘
                           │ TCP / Optional TLS (default port 3306 or 9030)
                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       service/trino-mysql-proxy                         │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Netty I/O Layer (Boss/Worker EventLoopGroup)                      │  │
│  │  • MysqlNettyServer          • SslHandler (dynamic TLS upgrade)   │  │
│  │  • MysqlPacketDecoder        • MysqlPacketEncoder (>16 MiB split) │  │
│  │  • MysqlConnectionHandler (suspends auto-read per active command) │  │
│  └──────────────────────────────┬────────────────────────────────────┘  │
│                                 │ Dispatched to query executor pool     │
│                                 ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Protocol & Session Layer                                          │  │
│  │  • MysqlHandshakeHandler (HandshakeV10, Auth, DB/Catalog init)    │  │
│  │  • MysqlSession (connectionId, ClientSession, active query,       │  │
│  │                  prepared statements, negotiated capabilities)    │  │
│  │  • MysqlSessionRegistry (tracks active sessions for KILL QUERY)   │  │
│  └──────────────────────────────┬────────────────────────────────────┘  │
│                                 ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Command Translation & Compatibility Layer                         │  │
│  │  • MysqlCommandDispatcher (COM_QUERY, COM_INIT_DB, COM_PING,      │  │
│  │      COM_FIELD_LIST, COM_STMT_*, COM_QUIT, COM_RESET_CONNECTION)  │  │
│  │  • MysqlSystemVariableHandler (@@version, @@max_allowed_packet,   │  │
│  │      SET NAMES / SET sql_mode / SHOW VARIABLES compatibility)     │  │
│  │  • MysqlPreparedStatementManager (PREPARE / EXECUTE / DEALLOCATE) │  │
│  └──────────────────────────────┬────────────────────────────────────┘  │
│                                 ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │ Trino REST Bridge & Wire Encoder                                  │  │
│  │  • TrinoStatementExecutor (wraps io.trino.client.StatementClient) │  │
│  │  • TrinoSessionSynchronizer (applies X-Trino-Set-* headers)       │  │
│  │  • TrinoToMysqlTypeMapper (ClientTypeSignature -> FIELD_TYPE_*)   │  │
│  │  • MysqlTextResultEncoder / MysqlBinaryResultEncoder              │  │
│  │  • TrinoErrorMapper (QueryError -> MySQL ERR_Packet + SQLSTATE)   │  │
│  └──────────────────────────────┬────────────────────────────────────┘  │
└─────────────────────────────────┼───────────────────────────────────────┘
                                  │ HTTP/HTTPS (OkHttp via trino-client)
                                  │ POST /v1/statement
                                  │ GET  /v1/statement/{queued|executing}/...
                                  │ DELETE /v1/statement/{queued|executing}/...
                                  ▼
┌────────────────────────────────────────────────────────┐
│                   Trino Coordinator                    │
│  (QueuedStatementResource / ExecutingStatementResource)│
└────────────────────────────────────────────────────────┘
```

### 3.3 Component Mapping: Apache Doris vs. `trino-mysql-proxy`

| Apache Doris Component | Proposed Trino Equivalent (`io.trino.mysqlproxy.*`) | Notes |
| --- | --- | --- |
| `QeService` + `MysqlServer` | `TrinoMysqlProxy` + `MysqlProxyModule` + `MysqlNettyServer` | Airlift lifecycle (`@PostConstruct` / `@PreDestroy`) managing Netty boss/worker groups and query executor pool |
| `AcceptListener` | `MysqlNettyServer` `ChannelInitializer` + `MysqlConnectionHandler.channelActive()` | Allocates `connectionId`, creates `MysqlSession`, sends `HandshakeV10` |
| `ReadListener` | `MysqlConnectionHandler.channelRead0()` with `ChannelConfig.setAutoRead(false/true)` | Mirrors Doris's suspend/resume pattern so at most one command executes per connection at a time without blocking Netty I/O threads |
| `MysqlChannel` | `MysqlPacketDecoder` + `MysqlPacketEncoder` + `MysqlWireWriter` | Handles 4-byte framing, sequence ID verification/reset, >16 MiB packet reassembly/splitting, `max_allowed_packet`, TLS `SslHandler` insertion |
| `MysqlProto` | `MysqlHandshakeHandler` | HandshakeV10 -> optional TLS upgrade -> `HandshakeResponse41` -> auth plugin -> catalog/schema init |
| `ConnectContext` | `MysqlSession` | Holds negotiated `MysqlCapability`, `ClientSession` (user, catalog, schema, properties, transactionId, preparedStatements), active `StatementClient` ref |
| `MysqlConnectProcessor` | `MysqlCommandDispatcher` | Dispatches MySQL `COM_*` commands to Trino REST calls or local compatibility handlers |
| `MysqlProtocolAdapter` + `MysqlResultSender` | `TrinoStatementExecutor` + `MysqlResultWriter` (`MysqlTextResultEncoder` / `MysqlBinaryResultEncoder`) | Streams Trino `QueryResults` pages directly into MySQL `ColumnDefinition41` + row packets + `EOF`/`ResultSetOK` |
| `MysqlSerializer` | `MysqlPacketBuffer` (backed by Netty `ByteBuf`) | Zero-copy little-endian integers, length-encoded ints (`writeLenEncInt`), length-encoded strings/bytes, NUL-terminated strings |

---

## 4. Proposed Module & Package Layout

```text
service/trino-mysql-proxy/
├── pom.xml
└── src/
    ├── main/java/io/trino/mysqlproxy/
    │   ├── TrinoMysqlProxy.java                  # Entry point (Airlift Bootstrap)
    │   ├── MysqlProxyModule.java                 # Guice bindings & config
    │   ├── MysqlProxyConfig.java                 # Listen host/port, Trino URI, TLS, auth mode, timeouts, thread pool sizes
    │   ├── server/
    │   │   ├── MysqlNettyServer.java             # Netty ServerBootstrap lifecycle
    │   │   ├── MysqlPacketDecoder.java           # 4-byte header + >16 MiB reassembly + sequence check
    │   │   ├── MysqlPacketEncoder.java           # 4-byte header + >16 MiB splitting + sequence increment
    │   │   ├── MysqlConnectionHandler.java       # Per-connection state machine (HANDSHAKE -> COMMAND -> CLOSED)
    │   │   └── MysqlSessionRegistry.java         # ConcurrentMap<Integer, MysqlSession> for KILL & metrics
    │   ├── protocol/
    │   │   ├── MysqlCapability.java              # Capability flag constants & negotiation helper
    │   │   ├── MysqlCommand.java                 # COM_* enum (0x00..0x1F)
    │   │   ├── MysqlServerStatusFlag.java        # SERVER_STATUS_* bitmask constants
    │   │   ├── MysqlPacketBuffer.java            # Little-endian & lenenc read/write helpers over ByteBuf
    │   │   ├── MysqlFieldType.java               # MySQL FIELD_TYPE_* codes & charset/flag constants
    │   │   └── packet/
    │   │       ├── MysqlPacket.java
    │   │       ├── HandshakeV10Packet.java
    │   │       ├── SslRequestPacket.java
    │   │       ├── HandshakeResponse41Packet.java
    │   │       ├── AuthSwitchRequestPacket.java
    │   │       ├── OkPacket.java
    │   │       ├── ErrPacket.java
    │   │       ├── EofPacket.java
    │   │       ├── ResultSetEndPacket.java       # 0xFE OK packet when CLIENT_DEPRECATE_EOF is set
    │   │       ├── ColumnDefinition41Packet.java
    │   │       └── StmtPrepareOkPacket.java
    │   ├── session/
    │   │   ├── MysqlSession.java                 # Mutable per-connection state + immutable Trino ClientSession
    │   │   ├── MysqlAuthenticator.java           # Auth plugin handling & Trino credential binding
    │   │   ├── TrinoClientFactory.java           # Builds OkHttpClient per config/user credentials
    │   │   └── TrinoSessionSynchronizer.java     # Applies StatementClient X-Trino-Set-* headers to ClientSession
    │   ├── execution/
    │   │   ├── MysqlCommandDispatcher.java       # Routes COM_* opcodes
    │   │   ├── TrinoStatementExecutor.java       # Executes SQL via StatementClient & streams pages
    │   │   ├── MysqlSystemVariableHandler.java   # Intercepts MySQL driver bootstrap queries (@@version, SET, etc.)
    │   │   ├── MysqlPreparedStatementManager.java# COM_STMT_PREPARE/EXECUTE/CLOSE/RESET -> Trino PREPARE/EXECUTE
    │   │   ├── TrinoToMysqlTypeMapper.java       # ClientTypeSignature -> MysqlFieldType / length / decimals / flags
    │   │   ├── MysqlTextResultEncoder.java       # Encodes QueryData rows in MySQL Text Resultset format
    │   │   ├── MysqlBinaryResultEncoder.java     # Encodes QueryData rows in MySQL Binary Resultset format
    │   │   └── TrinoErrorMapper.java             # QueryError -> MySQL error code + 5-char SQLSTATE
    │   └── metrics/
    │       └── MysqlProxyStats.java              # JMX counters (active connections, queries, errors, bytes)
    └── test/java/io/trino/mysqlproxy/
        ├── protocol/                             # Wire packet codec & framing unit tests
        ├── execution/                            # Type mapping, error mapping, variable interception tests
        ├── mock/                                 # OkHttp MockWebServer + Connector/J tests
        └── integration/                          # TestingTrinoServer + Connector/J end-to-end tests
```

---

## 5. Detailed Protocol-to-REST Translation Design

### 5.1 Configuration (`MysqlProxyConfig`)

| Config Property | Default | Description |
| --- | --- | --- |
| `mysql-proxy.listen-host` | `0.0.0.0` | Bind address for the MySQL TCP server |
| `mysql-proxy.listen-port` | `3306` | TCP port for MySQL wire protocol clients |
| `mysql-proxy.trino-uri` | *(required)* | Target Trino coordinator URI (e.g., `https://trino.example.com:8443`) |
| `mysql-proxy.default-catalog` | *(optional)* | Default Trino catalog if client does not specify a DB |
| `mysql-proxy.default-schema` | *(optional)* | Default Trino schema if client does not specify a DB |
| `mysql-proxy.server-version` | `8.0.32-trino` | Version banner in `HandshakeV10` (must look like MySQL 8.0.x for JDBC/ORM compatibility) |
| `mysql-proxy.auth-mode` | `TRUSTED_USER` | `TRUSTED_USER` (forwards MySQL username via `X-Trino-User`), `BASIC_CLEAR_PASSWORD` (uses `mysql_clear_password` over TLS to send HTTP Basic Auth to Trino), or `STATIC_TOKEN` |
| `mysql-proxy.tls.enabled` | `false` | Enables `CLIENT_SSL` advertisement and mid-handshake TLS upgrade |
| `mysql-proxy.tls.keystore-path` / `keystore-password` | *(optional)* | Server certificate/key for MySQL TLS |
| `mysql-proxy.max-allowed-packet` | `64MB` | Upper bound on incoming logical MySQL packet payload size |
| `mysql-proxy.max-connections` | `1024` | Connection pool cap (mirrors Doris `ConnectPoolMgr`) |
| `mysql-proxy.io-threads` | `Runtime.availableProcessors()` | Netty worker event-loop threads |
| `mysql-proxy.query-threads` | `256` | Executor pool threads for blocking `StatementClient` REST polling |
| `mysql-proxy.idle-timeout` | `8h` | Idle connection cleanup timeout |

### 5.2 Handshake, TLS Upgrade, and Authentication

1. **Initial Handshake (`HandshakeV10Packet`)**:
   - On `channelActive`, generate a unique `connectionId` and 20-byte random scramble, then send `HandshakeV10` with `sequenceId = 0`.
   - Advertised capabilities:
     `CLIENT_LONG_PASSWORD | CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_TRANSACTIONS | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_ATTRS | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA | CLIENT_DEPRECATE_EOF` (+ `CLIENT_SSL` when TLS is configured).
2. **Optional TLS Upgrade (`SslRequestPacket`)**:
   - Inspect the first 4 bytes (client capability flags) of the client's first packet (`sequenceId = 1`).
   - If `CLIENT_SSL` is set:
     - If TLS is disabled on the proxy, send `ERR_Packet` and close.
     - Otherwise, dynamically insert Netty's `SslHandler` at the head of the pipeline and await the next packet (`HandshakeResponse41`, `sequenceId = 2`) over TLS.
3. **Authentication Bridge to Trino**:
   - **Challenge**: MySQL's default `mysql_native_password` and `caching_sha2_password` transmit salted password hashes, whereas Trino's REST API password authenticators expect HTTP `Authorization: Basic base64(user:plaintext)` (or Bearer JWT).
   - **Supported modes**:
     - `TRUSTED_USER` (default for internal/unauthenticated Trino): accept `mysql_native_password` with empty/any password and set `X-Trino-User: <mysqlUser>` on `ClientSession`.
     - `BASIC_CLEAR_PASSWORD` (for password-authenticated Trino clusters): advertise `mysql_clear_password` (or emit `AuthSwitchRequest` to `mysql_clear_password`), **enforce TLS** so plaintext passwords never cross unencrypted networks, extract the NUL-terminated plaintext password, and attach an OkHttp Basic Auth interceptor to the session's `OkHttpClient`. Optionally validate credentials immediately via a lightweight `GET /v1/info` or `SELECT 1` before returning `OK_Packet`.
4. **Database -> Catalog / Schema Mapping**:
   - MySQL has a 1-level database hierarchy (`db`), whereas Trino has a 2-level hierarchy (`catalog.schema`).
   - Convention for `HandshakeResponse41.database` and `COM_INIT_DB`:
     - `"catalog.schema"` -> `catalog = "catalog"`, `schema = "schema"`.
     - `"catalog"` -> if `default-catalog` is unset, treat as `catalog` (schema unset); if `default-catalog` is set and the identifier contains no `.`, treat as `schema` under the current/default catalog (configurable via `mysql-proxy.single-db-name-target=CATALOG|SCHEMA`).
     - Validate by executing `USE <target>` against Trino or updating `ClientSession` directly (validating via `USE` returns authentic Trino errors for unknown catalog/schema).

### 5.3 MySQL Command-to-Trino REST Mapping

| MySQL Command | Opcode | Trino REST Translation | Response Packets |
| --- | --- | --- | --- |
| `COM_PING` | `0x0E` | Optional `GET /v1/info` (or immediate local no-op) | `OK_Packet` (or `ERR_Packet` if coordinator unreachable) |
| `COM_QUIT` | `0x01` | Cancel any active `StatementClient` (`DELETE /v1/statement/...`), close TCP connection | None (close socket) |
| `COM_INIT_DB` | `0x02` | Parse `catalog.schema` / `schema`; execute `USE ...` via `POST /v1/statement` and sync `X-Trino-Set-Catalog` / `X-Trino-Set-Schema` | `OK_Packet` or `ERR_Packet` |
| `COM_QUERY` | `0x03` | 1. Check `MysqlSystemVariableHandler` (driver bootstrap/compatibility queries)<br>2. Handle `KILL [QUERY\|CONNECTION] <id>` via `MysqlSessionRegistry`<br>3. Otherwise execute SQL via `StatementClientFactory.newStatementClient(httpClient, session.clientSession(), sql)` (`POST /v1/statement` + `GET nextUri` loop) | Either **ResultSet** (if `columns != null`) or **`OK_Packet`** (with `affectedRows = updateCount` for DML/DDL), or **`ERR_Packet`** |
| `COM_FIELD_LIST` | `0x04` | Execute `DESCRIBE <table>` via Trino REST, translate rows to `ColumnDefinition41Packet`s | `ColumnDefinition41` × N + `EOF`/`ResultSetEndPacket` |
| `COM_STMT_PREPARE` | `0x16` | Register synthetic name `trino_ps_<id>`; execute `PREPARE trino_ps_<id> FROM <sql>` (sync `X-Trino-Added-Prepare`), then `DESCRIBE INPUT trino_ps_<id>` and `DESCRIBE OUTPUT trino_ps_<id>` to determine parameter & output column metadata | `StmtPrepareOkPacket` + param `ColumnDefinition41`s + column `ColumnDefinition41`s (respecting `CLIENT_DEPRECATE_EOF`) |
| `COM_STMT_EXECUTE` | `0x17` | Decode null bitmap, param types, and binary parameter values; format Trino SQL literals (mirroring `TrinoPreparedStatement`); execute `EXECUTE trino_ps_<id> USING ...` via Trino REST with `X-Trino-Prepared-Statement` header | ResultSet encoded with **`MysqlBinaryResultEncoder`** (or `OK_Packet`) |
| `COM_STMT_CLOSE` | `0x19` | Remove from `ClientSession.preparedStatements` (and optionally `DEALLOCATE PREPARE`) | None (per MySQL spec) |
| `COM_STMT_RESET` | `0x1A` | Clear buffered long-data parameters for `stmtId` | `OK_Packet` |
| `COM_STMT_SEND_LONG_DATA` | `0x18` | Append bytes to parameter buffer in `MysqlPreparedStatementManager` | None (per MySQL spec) |
| `COM_RESET_CONNECTION` | `0x1F` | Reset `ClientSession` properties, roles, prepared statements, transactionId (cancel active tx if any) | `OK_Packet` |
| `COM_CHANGE_USER` | `0x11` | Re-run `MysqlAuthenticator` and reset `ClientSession` | `OK_Packet` or `ERR_Packet` |
| `COM_SET_OPTION` | `0x1B` | Record `MYSQL_OPTION_MULTI_STATEMENTS_ON/OFF` (or accept as no-op) | `OK_Packet` |
| `COM_STATISTICS` | `0x09` | Return proxy uptime/query stats string | EOF-terminated string packet |
| Other unsupported commands | — | Return `ER_UNKNOWN_COM_ERROR (1047)` | `ERR_Packet` |

### 5.4 Streaming Execution Loop (`TrinoStatementExecutor`)

To avoid buffering entire Trino result sets in memory, `TrinoStatementExecutor` streams results incrementally as `StatementClient` polls `nextUri`:

1. Create `StatementClient client = StatementClientFactory.newStatementClient(httpClient, session.clientSession(), sql)` and store it in `session.setActiveStatement(client)` so concurrent `KILL QUERY` or TCP disconnect can call `client.close()`.
2. Maintain `boolean headerSent = false;` and `List<Column> columns = null;`.
3. Loop while `client.isRunning()`:
   - Check `client.currentStatusInfo().getColumns()`: when non-null and `!headerSent`:
     - Write column count packet (`writeLenEncInt(columns.size())`).
     - Write one `ColumnDefinition41Packet` per `Column` using `TrinoToMysqlTypeMapper`.
     - If `!session.capabilities().isDeprecatedEof()`, write intermediate `EofPacket`.
     - Set `headerSent = true`.
   - Read `QueryData data = client.currentData()`. If `data != null && data.getData() != null`:
     - For each `List<Object> row : data.getData()`:
       - Encode row using `MysqlTextResultEncoder` (for `COM_QUERY`) or `MysqlBinaryResultEncoder` (for `COM_STMT_EXECUTE`).
       - Write packet to Netty channel; flush periodically (e.g., every page or when buffer exceeds 64 KiB) and honor backpressure (`if (!ctx.channel().isWritable())` flush & await writability with write-timeout check, preventing OOM on slow clients).
   - Call `client.advance()` to fetch the next REST page (`GET nextUri`).
4. After loop finishes:
   - Synchronize Trino response headers to `session.clientSession()` via `TrinoSessionSynchronizer`:
     - `getSetCatalog()`, `getSetSchema()`, `getSetPath()`, `getSetSessionProperties()`, `getResetSessionProperties()`, `getSetRoles()`, `getAddedPreparedStatements()`, `getDeallocatedPreparedStatements()`, `getStartedTransactionId()`, `isClearTransactionId()`, `getSetAuthorizationUser()`, `isResetAuthorizationUser()`.
   - Inspect `QueryStatusInfo finalStatus = client.finalStatusInfo()`:
     - If `client.isClientAborted()` or `client.isClientError()` or `finalStatus.getError() != null`:
       - Map error via `TrinoErrorMapper` and send `ErrPacket` (note: if `headerSent` is true, MySQL clients accept an `ERR_Packet` mid-resultset in place of the terminating EOF/OK packet).
     - Else if `headerSent`:
       - Send `ResultSetEndPacket` (`0xFE` OK packet) if `CLIENT_DEPRECATE_EOF` is negotiated, otherwise `EofPacket`.
     - Else (DML / DDL / `USE` / `SET SESSION` with no result columns):
       - Send `OkPacket` with `affectedRows = Optional.ofNullable(finalStatus.getUpdateCount()).orElse(0L)` and `warnings = finalStatus.getWarnings().size()`.
5. Always flush, clear `session.clearActiveStatement()`, close `client`, and re-enable `ctx.channel().config().setAutoRead(true)` on the Netty event loop.

### 5.5 Trino-to-MySQL Data Type Mapping (`TrinoToMysqlTypeMapper`)

`trino-client` normalizes JSON row values via `FixJsonDataUtils` into Java objects (`Boolean`, `Byte`, `Short`, `Integer`, `Long`, `Float`, `Double`, `String`, `byte[]`, `List`, `Map`, `Row`). Each `ClientTypeSignature` maps to a MySQL wire column definition as follows:

| Trino `ClientStandardTypes` | MySQL `FIELD_TYPE_*` | Code | Text Encoding | Binary Encoding (`COM_STMT_EXECUTE`) |
| --- | --- | --- | --- | --- |
| `boolean` | `MYSQL_TYPE_TINY` | `0x01` | `"1"` / `"0"` | 1-byte int (`1` / `0`) |
| `tinyint` | `MYSQL_TYPE_TINY` | `0x01` | Decimal string | 1-byte signed int |
| `smallint` | `MYSQL_TYPE_SHORT` | `0x02` | Decimal string | 2-byte LE signed int |
| `integer` | `MYSQL_TYPE_LONG` | `0x03` | Decimal string | 4-byte LE signed int |
| `bigint` | `MYSQL_TYPE_LONGLONG` | `0x08` | Decimal string | 8-byte LE signed int |
| `real` | `MYSQL_TYPE_FLOAT` | `0x04` | Float string | 4-byte IEEE 754 LE float |
| `double` | `MYSQL_TYPE_DOUBLE` | `0x05` | Double string | 8-byte IEEE 754 LE double |
| `decimal(p, s)` | `MYSQL_TYPE_NEWDECIMAL` | `0xF6` | Exact decimal string (`decimals = s`) | Length-encoded decimal string |
| `date` | `MYSQL_TYPE_DATE` | `0x0A` | `YYYY-MM-DD` | Binary date struct (4 bytes: year, month, day) |
| `time` / `time(p)` | `MYSQL_TYPE_TIME` | `0x0B` | `HH:MM:SS[.ffffff]` | Binary time struct (8 or 12 bytes) |
| `timestamp` / `timestamp(p)` | `MYSQL_TYPE_DATETIME` | `0x0C` | `YYYY-MM-DD HH:MM:SS[.ffffff]` | Binary datetime struct (7 or 11 bytes) |
| `timestamp with time zone`, `time with time zone` | `MYSQL_TYPE_VAR_STRING` | `0xFD` | Trino formatted string (preserves zone/offset) | Length-encoded UTF-8 string |
| `char(n)` | `MYSQL_TYPE_STRING` | `0xFE` | UTF-8 string | Length-encoded UTF-8 string |
| `varchar` / `varchar(n)` | `MYSQL_TYPE_VAR_STRING` | `0xFD` | UTF-8 string | Length-encoded UTF-8 string |
| `varbinary` | `MYSQL_TYPE_BLOB` | `0xFC` | Raw bytes (`charset = 63` binary) | Length-encoded raw bytes |
| `json` | `MYSQL_TYPE_JSON` (or `VAR_STRING`) | `0xF5` / `0xFD` | UTF-8 JSON string | Length-encoded UTF-8 JSON string |
| `uuid`, `ipaddress`, `interval *`, `array`, `map`, `row`, other | `MYSQL_TYPE_VAR_STRING` | `0xFD` | String / JSON serialization | Length-encoded UTF-8 string |

### 5.6 MySQL Client & BI Tool Compatibility Layer (`MysqlSystemVariableHandler`)

Standard MySQL clients (`mysql` CLI, MySQL Connector/J 8/9, PyMySQL, Go `go-sql-driver/mysql`, DBeaver, Tableau) automatically emit metadata/session probe queries immediately after connecting, before any user SQL runs. If forwarded verbatim to Trino, these fail with syntax errors.

The compatibility interceptor handles a focused, well-tested set of probe patterns locally (returning synthetic MySQL result sets or `OK_Packet`):

- `SELECT @@version_comment LIMIT 1` -> returns configured version comment (e.g., `Trino MySQL Protocol Proxy`).
- `SELECT @@version` / `SELECT VERSION()` -> returns configured server version banner.
- Connector/J bootstrap multi-variable `SELECT @@session.auto_increment_increment AS ..., @@character_set_client AS ..., @@max_allowed_packet AS ..., @@sql_mode AS ..., @@time_zone AS ..., @@transaction_isolation AS ...` -> returns sensible defaults (UTF-8mb4, `max_allowed_packet`, current session time zone, `READ-COMMITTED`).
- `SET NAMES ...`, `SET character_set_results = ...`, `SET autocommit = ...`, `SET sql_mode = ...`, `SET net_write_timeout = ...`, `SET SESSION transaction ...` -> return `OK_Packet` (and map `SET time_zone = '...'` directly to `ClientSession.timeZone`).
- `SHOW VARIABLES [LIKE '...']` / `SHOW COLLATION` -> return minimal curated table required by JDBC/ORM drivers.
- `SELECT DATABASE()` -> return current `catalog.schema` (or `schema`) from `ClientSession`.
- `KILL [QUERY | CONNECTION] <connectionId>` -> look up target `MysqlSession` in `MysqlSessionRegistry` and invoke `activeStatement.close()` (and close channel if `KILL CONNECTION`).

A config flag (`mysql-proxy.compat-interceptor.enabled=true`) allows operators to disable or customize interception if desired.

### 5.7 Error Mapping (`TrinoErrorMapper`)

Translate Trino `QueryError` (`errorCode`, `errorName`, `errorType`, `message`) into MySQL `ERR_Packet` (`0xFF`, 2-byte error code, `#`, 5-char SQLSTATE, message):

| Trino Condition / `ErrorType` | MySQL Error Code | SQLSTATE |
| --- | --- | --- |
| `SYNTAX_ERROR` | `1064` (`ER_PARSE_ERROR`) | `42000` |
| `CATALOG_NOT_FOUND` / `SCHEMA_NOT_FOUND` | `1049` (`ER_BAD_DB_ERROR`) | `42000` |
| `TABLE_NOT_FOUND` | `1146` (`ER_NO_SUCH_TABLE`) | `42S02` |
| `COLUMN_NOT_FOUND` | `1054` (`ER_BAD_FIELD_ERROR`) | `42S22` |
| `PERMISSION_DENIED` | `1142` (`ER_TABLEACCESS_DENIED_ERROR`) | `42000` |
| HTTP 401 Unauthorized | `1045` (`ER_ACCESS_DENIED_ERROR`) | `28000` |
| `USER_CANCELED` | `1317` (`ER_QUERY_INTERRUPTED`) | `70100` |
| `INSUFFICIENT_RESOURCES` | `1041` (`ER_OUT_OF_RESOURCES`) | `HY000` |
| Other `USER_ERROR` / `INTERNAL_ERROR` / `EXTERNAL` | `1105` (`ER_UNKNOWN_ERROR`) | `HY000` |

---

## 6. Phased Implementation Roadmap

### Phase 1 — Module Scaffolding, Netty Wire Framing & Handshake
- Add `service/trino-mysql-proxy` module and register it in root `pom.xml`.
- Implement `TrinoMysqlProxy`, `MysqlProxyModule`, `MysqlProxyConfig`.
- Implement `MysqlPacketBuffer`, `MysqlPacketDecoder` (4-byte header, sequence ID checks, >16 MiB packet reassembly, `max_allowed_packet` enforcement), and `MysqlPacketEncoder` (>16 MiB packet splitting).
- Implement `HandshakeV10Packet`, `HandshakeResponse41Packet`, `OkPacket`, `ErrPacket`, `EofPacket`, `ResultSetEndPacket`.
- Implement `MysqlNettyServer`, `MysqlConnectionHandler` (with auto-read suspend/resume), `MysqlSession`, `MysqlSessionRegistry`.
- Handle `COM_PING` and `COM_QUIT`.
- **Exit Criteria**: Standard `mysql` CLI and MySQL Connector/J can connect, complete handshake, ping, and cleanly disconnect.

### Phase 2 — Trino REST Execution (`COM_QUERY`, `COM_INIT_DB`) & Text Result Streaming
- Implement `TrinoClientFactory`, `TrinoStatementExecutor`, and `TrinoSessionSynchronizer` on top of `io.trino.client.StatementClientFactory`.
- Implement `ColumnDefinition41Packet`, `TrinoToMysqlTypeMapper`, `MysqlTextResultEncoder`, and `TrinoErrorMapper`.
- Implement `COM_QUERY` streaming (columns -> rows -> `EOF`/`ResultSetEndPacket`, or `OK_Packet` for DML/DDL) and `COM_INIT_DB`.
- Implement client disconnect cancellation (`channelInactive` -> `activeStatement.close()`).
- **Exit Criteria**: `SELECT`, `SHOW CATALOGS`, `SHOW SCHEMAS`, `SHOW TABLES`, `USE`, `INSERT`, `CREATE TABLE`, and `SET SESSION` work end-to-end against `TestingTrinoServer` from `mysql` CLI and Connector/J.

### Phase 3 — Driver Compatibility, TLS Upgrade & Password Auth
- Implement `MysqlSystemVariableHandler` for Connector/J 8/9, PyMySQL, `mysql` CLI, and BI tool initialization queries.
- Implement `SslRequestPacket` and dynamic Netty `SslHandler` insertion (`mysql-proxy.tls.enabled=true`).
- Implement `mysql_clear_password` (`AuthSwitchRequestPacket`) over TLS forwarding credentials as HTTP Basic Auth to Trino.
- Implement `COM_FIELD_LIST`, `COM_RESET_CONNECTION`, `COM_CHANGE_USER`, `COM_SET_OPTION`, `COM_STATISTICS`, and `KILL QUERY` / `KILL CONNECTION`.
- **Exit Criteria**: MySQL Connector/J 8.x/9.x, PyMySQL, and `mysql --ssl-mode=REQUIRED` connect cleanly with default driver settings and execute queries without workarounds.

### Phase 4 — Server-Side Prepared Statements & Binary Resultset Protocol
- Implement `MysqlPreparedStatementManager` for `COM_STMT_PREPARE`, `COM_STMT_EXECUTE`, `COM_STMT_CLOSE`, `COM_STMT_RESET`, and `COM_STMT_SEND_LONG_DATA`.
- Map `COM_STMT_PREPARE` to Trino `PREPARE` + `DESCRIBE INPUT` + `DESCRIBE OUTPUT`, sending `StmtPrepareOkPacket` + metadata definitions.
- Implement MySQL binary parameter decoding (null bitmap, unsigned flags, temporal/numeric/string/blob wire types) and literal formatting for Trino `EXECUTE ... USING ...`.
- Implement `MysqlBinaryResultEncoder` (null bitmap with 2-bit offset + binary typed column values).
- **Exit Criteria**: JDBC `PreparedStatement` with `useServerPrepStmts=true` works for parameterized `SELECT` queries across all common scalar types.

### Phase 5 — Observability, Hardening & Packaging
- Add JMX metrics (`MysqlProxyStats`: active/total connections, active/failed queries, rows/bytes streamed, auth failures) and OpenTelemetry tracing spans linked to `X-Trino-Trace-Token`.
- Add idle connection eviction (`IdleStateHandler`), connection limits, and write-timeout backpressure enforcement.
- Add executable packaging/assembly (following `service/trino-proxy`) and operator documentation under `docs/src/`.
- **Exit Criteria**: Load/soak tests verify bounded memory under large streaming result sets (e.g., millions of rows) and clean cancellation under client disconnects.

---

## 7. Testing & Verification Strategy

1. **Protocol Unit Tests** (`protocol/`):
   - Round-trip serialization/deserialization of every MySQL packet type (`HandshakeV10`, `HandshakeResponse41`, `ColumnDefinition41`, `OkPacket`, `ErrPacket`, `EofPacket`, `ResultSetEndPacket`, `StmtPrepareOkPacket`).
   - Sequence ID wrapping (`255 -> 0`), >16 MiB (`0xFFFFFF`) multi-packet splitting and reassembly, and `max_allowed_packet` rejection.
   - `CLIENT_DEPRECATE_EOF` enabled vs. disabled framing verification.
   - `TrinoToMysqlTypeMapper`, `MysqlTextResultEncoder`, `MysqlBinaryResultEncoder`, and `TrinoErrorMapper` coverage for every `ClientStandardTypes` value including `NULL`, edge timestamps, decimals, binary blobs, arrays, maps, and rows.
2. **Mock Trino Coordinator Tests** (`mock/`):
   - Use OkHttp `MockWebServer` (already used in `trino-client` tests) to simulate multi-page `/v1/statement` responses, `X-Trino-Set-*` headers, HTTP 401/503 retries, mid-stream `QueryError`, and `DELETE` cancellation verification when a MySQL client disconnects.
3. **End-to-End Integration Tests** (`integration/`):
   - Boot an in-process `TestingTrinoServer` with `tpch` and `memory` connectors (matching `service/trino-proxy/pom.xml` test dependencies) behind `MysqlNettyServer`.
   - Run real **MySQL Connector/J** tests (both `useServerPrepStmts=false` and `useServerPrepStmts=true`, with and without TLS) executing TPC-H queries, DDL/DML on `memory`, session property changes, transactions, large streaming result sets, and `KILL QUERY`.

---

## 8. Risks, Trade-offs & Open Questions

1. **Authentication Protocol Mismatch**:
   - MySQL's `mysql_native_password` and `caching_sha2_password` cannot expose the plaintext password to an intermediary, so forwarding passwords to Trino HTTP Basic Auth requires `mysql_clear_password` (which must require TLS and may require `--enable-cleartext-plugin` on older `mysql` CLI builds).
   - *Mitigation*: Support both `TRUSTED_USER` (for trusted networks / gateway deployments) and `BASIC_CLEAR_PASSWORD` over TLS, and consider optional JWT/OAuth token pass-through in the password field.
2. **SQL Dialect Differences**:
   - Trino speaks ANSI SQL, whereas MySQL tools sometimes emit MySQL-specific syntax (backtick `` `ident` `` quoting, `LIMIT offset, count`, `SHOW DATABASES`, `SELECT @@...`).
   - *Decision*: Keep SQL rewriting minimal by design — intercept only well-known driver bootstrap/metadata probes (`MysqlSystemVariableHandler`), and document that user queries use Trino SQL (optional backtick-to-double-quote rewriting can be evaluated later as an opt-in config).
3. **Mid-Stream Failures**:
   - Because results stream to the MySQL client as Trino pages arrive, a Trino execution failure occurring *after* `ColumnDefinition41` packets have been written cannot send a clean top-level error; it sends an `ERR_Packet` instead of the terminating `EOF`/`ResultSetEndPacket`, which modern MySQL drivers surface as a `SQLException` during `ResultSet.next()`.
4. **Prepared Statement Latency**:
   - Mapping `COM_STMT_PREPARE` to three Trino REST round-trips (`PREPARE`, `DESCRIBE INPUT`, `DESCRIBE OUTPUT`) adds setup latency per prepared statement.
   - *Mitigation*: Cache prepared statement metadata per session and allow clients using `useServerPrepStmts=false` (Connector/J default) to bypass `COM_STMT_PREPARE` completely.
