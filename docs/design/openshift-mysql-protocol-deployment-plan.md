# OpenShift Build & Deployment Plan: Trino `mysql-protocol-proxy-plan`

## Progress / Executed Tasks

### Completed
- [x] Pushed `oraclesplit_clean` (`51da6c1c0c35deafb3376af4e2e14c34c48d70c4`) to `https://gitlab.telkom.co.id/dit-epd/big-data/trino.git`
- [x] Pushed `mysql-protocol-proxy-plan` (`5ed4a2d6a3b4dfa27840b75ff8aeea8a705dc698`) to `https://gitlab.telkom.co.id/dit-epd/big-data/trino.git`
- [x] Examined OpenShift deployments `trino-chart-coordinator` and `trino-chart-worker` in namespace `epd-financeapps` via `oc48`
- [x] Switched local git branch to `mysql-protocol-proxy-plan` tracking `yudhiremote/mysql-protocol-proxy-plan`
- [x] Created this deployment plan document (`docs/design/openshift-mysql-protocol-deployment-plan.md`)
- [x] Step 2: Backed up existing OpenShift configurations (`deployment`, `configmap`, `service`, `pvc`) to `openshift-backups/2026-11-22/`
- [x] Step 3: Authored `core/trino-server/src/main/provisio/trino-openshift.xml` and `Dockerfile.openshift`, committed, and pushed to GitLab (`mysql-protocol-proxy-plan`)
- [x] Step 4: Created Git source secret (`trino-gitlab-secret`), `ImageStream` (`trino`), and `BuildConfig` (`trino-mysql-proxy`) in `epd-financeapps`, and completed OpenShift Docker build (`trino-mysql-proxy-3` → `image-registry.openshift-image-registry.svc:5000/epd-financeapps/trino:455-mysql-proxy`)
- [x] Step 5: Enabled embedded MySQL wire protocol (`mysql-protocol.enabled=true`, port `3306`) in `configmap/trino-chart-coordinator`, exposed port `3306/TCP` on `service/trino-chart`, and created `NodePort` service `service/trino-chart-mysql-np` (`3306:31485/TCP`, ClusterIP `172.19.205.214`)
- [x] Step 6: Unmounted legacy `442` plugin PVCs (`mysqlplugin2`, `oraplugin2`), configured `imagePullSecrets` (`deployer-dockercfg-gvhnw`, `default-dockercfg-j88t9`) and `rollingUpdate: {maxSurge: 0, maxUnavailable: 1}`, and switched `trino-chart-coordinator` and `trino-chart-worker` to `image-registry.openshift-image-registry.svc:5000/epd-financeapps/trino:455-mysql-proxy`
- [x] Step 7: Verified rollout (`1/1` coordinator + `3/3` workers `active` in `system.runtime.nodes`), all 25 catalogs (`24` configured + `system`), and MySQL wire protocol listener on port `3306` (`8.0.32-trino` handshake)
- [x] Step 8: Implemented `MysqlSqlPreprocessor` (backquoted identifiers `` `...` `` -> `"..."`, `#` comments -> `--`, `LIMIT offset, count` -> `OFFSET offset LIMIT count`) and Sequel Pro / MySQL GUI metadata query compatibility (`SHOW DATABASES`, `SHOW [FULL] TABLES`, `SHOW TABLE STATUS`, `SHOW [FULL] COLUMNS/FIELDS`, `SHOW CREATE TABLE`, `SHOW INDEX/KEYS`, `SHOW ENGINES/TRIGGERS/PROCEDURE STATUS/FUNCTION STATUS`) in `lib/trino-mysql-protocol`
- [x] Step 9: Ran unit tests for `lib/trino-mysql-protocol`, committed (`0210760ae6b`), and pushed to `mysql-protocol-proxy-plan` on GitLab and GitHub
- [x] Step 10: Built updated OpenShift image (`trino-mysql-proxy-4` → `sha256:ac2bbb6ad28e8e46390f6051c14246669a849d772fe906230ffe9a91082faf1f`) and rolled out to `trino-chart-coordinator` (`1/1`) and `trino-chart-worker` (`3/3`)

### In Progress / Pending
- *(None — all planned steps completed)*

---

## Context & Decisions
- **Build Method (Option B)**: In-cluster multi-stage Git + `Dockerfile` build in OpenShift namespace `epd-financeapps`, pulling `mysql-protocol-proxy-plan` from `https://gitlab.telkom.co.id/dit-epd/big-data/trino.git` and pushing to OpenShift `ImageStreamTag` `trino:455-mysql-proxy` (`image-registry.openshift-image-registry.svc:5000/epd-financeapps/trino:455-mysql-proxy`).
- **Legacy `442` Plugin PVCs (`mysqlplugin2`, `oraplugin2`)**: Back up live OpenShift configurations before unmounting them from `trino-chart-coordinator` and `trino-chart-worker`. `oraclesplit_clean` is already merged into `mysql-protocol-proxy-plan`; `mysqlsplit` merging is postponed.
- **MySQL Wire Protocol**: Enable `mysql-protocol.enabled=true` (`mysql-protocol.listen-port=3306`) on `trino-chart-coordinator` and expose port `3306/TCP` on `deployment/trino-chart-coordinator` and `service/trino-chart`.

---

## Ordered Execution Steps

### Step 1: Switch Local Branch & Create Plan File
1. `git checkout -B mysql-protocol-proxy-plan yudhiremote/mysql-protocol-proxy-plan`
2. Write `docs/design/openshift-mysql-protocol-deployment-plan.md`.

### Step 2: Back Up Existing OpenShift Configurations
Export live YAML manifests from namespace `epd-financeapps` into `openshift-backups/2026-11-22/` (untracked by Git):
- `deployment/trino-chart-coordinator`
- `deployment/trino-chart-worker`
- `configmap/trino-chart-coordinator`
- `configmap/trino-chart-worker`
- `service/trino-chart`
- `pvc/mysqlplugin2`, `pvc/oraplugin2`

### Step 3: Author `Dockerfile.openshift` & Slim Provisio Descriptor
- Create `core/trino-server/src/main/provisio/trino-openshift.xml` containing only the active connectors/plugins used in `epd-financeapps`:
  - Connectors: `oracle`, `mysql`, `postgresql`, `iceberg`, `hive` (with `trino-hdfs`), `tpcds`, `tpch`
  - Core/System plugins: `exchange-filesystem`, `jmx`, `memory`, `password-authenticators`, `resource-group-managers`, `session-property-managers`
- Create `Dockerfile.openshift` using `harbor.telkom.co.id/dockerhub-proxy/trinodb/trino:454` as both builder base (provides JDK 22 + prebuilt `trino-web-ui-454.jar` static assets + `libjvmkill.so`) and runtime base.
- Commit and push `Dockerfile.openshift`, `core/trino-server/src/main/provisio/trino-openshift.xml`, and this plan document to `mysql-protocol-proxy-plan` on GitLab.

### Step 4: Configure & Run OpenShift `Dockerfile` Build
- Create/update `secret/trino-gitlab-secret` (`kubernetes.io/basic-auth`).
- Create `ImageStream` `trino` and `BuildConfig` `trino-mysql-proxy` (`strategy.type: Docker`, `dockerfilePath: Dockerfile.openshift`, `output.to: ImageStreamTag trino:455-mysql-proxy`, limits `cpu: "4"`, `memory: 8Gi`).
- Run `oc48 start-build trino-mysql-proxy --follow` and verify `Complete` status.

### Step 5: Enable Embedded MySQL Wire Protocol
- Append `mysql-protocol.enabled=true` and `mysql-protocol.listen-port=3306` to `config.properties` in `configmap/trino-chart-coordinator`.
- Add port `3306/TCP` (`name: mysql`) to `service/trino-chart`.

### Step 6: Unmount Legacy `442` Plugin PVCs & Update Deployments
- Remove `mysqlplugin2` and `oraplugin2` `volumeMounts` and `volumes` from `deployment/trino-chart-coordinator` and `deployment/trino-chart-worker`.
- Add containerPort `3306/TCP` (`name: mysql`) to `deployment/trino-chart-coordinator`.
- Set image on both deployments to `image-registry.openshift-image-registry.svc:5000/epd-financeapps/trino:455-mysql-proxy`.

### Step 7: Verify Rollout & Update Progress
- Verify `oc48 rollout status` for `trino-chart-coordinator` and `trino-chart-worker`.
- Verify `/v1/info` reports `455-SNAPSHOT`, `system.runtime.nodes` shows 1 coordinator + 3 workers `active`, `SHOW CATALOGS` lists all 24 catalogs, and port `3306` responds with the `8.0.32-trino` MySQL handshake packet.
- Update the Progress section above.

### Step 8: Implement SQL Preprocessor & Sequel Pro GUI Metadata Compatibility
- **`MysqlSqlPreprocessor`**:
  - Single-pass lexical state machine that safely converts MySQL backquoted identifiers `` `ident` `` (with `` `` `` unescaping and `"` -> `""` escaping) into ANSI double-quoted identifiers `"ident"`, while preserving single-quoted strings (`'...'`), double-quoted identifiers (`"..."`), `--` line comments, and `/* ... */` block comments.
  - Converts MySQL `# ...` line comments (outside strings/identifiers) into ANSI `-- ...` line comments.
  - Rewrites MySQL two-argument pagination `LIMIT <offset>, <count>` into Trino ANSI `OFFSET <offset> LIMIT <count>`.
- **Sequel Pro & MySQL GUI Metadata Compatibility (`MysqlMetadataQueryHandler` / `MysqlSystemVariableHandler` / `MysqlCommandDispatcher`)**:
  - `SHOW DATABASES` / `SHOW SCHEMAS`: Lists schemas in the current/default catalog (or catalogs if no catalog is selected), returning column `Database`.
  - `SHOW [FULL] TABLES [FROM/IN db] [LIKE '...']`: Queries Trino `SHOW TABLES` and returns `Tables_in_<db>` (plus `Table_type = 'BASE TABLE'` when `FULL` is specified).
  - `SHOW TABLE STATUS [FROM/IN db] [LIKE '...' | WHERE ...]`: Queries Trino `SHOW TABLES` and returns standard MySQL `SHOW TABLE STATUS` columns (`Name`, `Engine`, `Version`, `Row_format`, `Rows`, `Avg_row_length`, `Data_length`, `Max_data_length`, `Index_length`, `Data_free`, `Auto_increment`, `Create_time`, `Update_time`, `Check_time`, `Collation`, `Checksum`, `Create_options`, `Comment`) so Sequel Pro's sidebar and table info view load cleanly.
  - `SHOW [FULL] COLUMNS/FIELDS FROM tbl [FROM/IN db]`: Executes Trino `DESCRIBE` and maps columns to MySQL's `Field`, `Type`, `Collation`, `Null`, `Key`, `Default`, `Extra`, `Privileges`, `Comment`.
  - `SHOW CREATE TABLE tbl`: Executes Trino `SHOW CREATE TABLE` and returns the 2-column MySQL result (`Table`, `Create Table`).
  - `SHOW INDEX/INDEXES/KEYS FROM tbl`, `SHOW TRIGGERS`, `SHOW ENGINES`, `SHOW STATUS`, `SHOW PROCEDURE STATUS`, `SHOW FUNCTION STATUS`, `SHOW WARNINGS`, `SHOW ERRORS`: Returns valid MySQL column headers (and empty or synthetic rows) so Sequel Pro's structure/triggers/indexes tabs do not error out.
  - `COM_FIELD_LIST`: Translates legacy MySQL `COM_FIELD_LIST` (`0x04`) into Trino `DESCRIBE` column definitions.

### Step 9: Unit Tests, Git Commit & Push
- Add comprehensive unit tests for `MysqlSqlPreprocessor` and Sequel Pro metadata query handling in `lib/trino-mysql-protocol`.
- Run `mvn test -pl lib/trino-mysql-protocol`.
- Commit and push to `mysql-protocol-proxy-plan` on `yudhiremote` (`https://gitlab.telkom.co.id/dit-epd/big-data/trino.git`).

### Step 10: OpenShift Build & Rollout
- Run `oc48 start-build trino-mysql-proxy --follow` in namespace `epd-financeapps`.
- Roll out the updated image to `deployment/trino-chart-coordinator` and `deployment/trino-chart-worker` and verify with MySQL backquoted queries and Sequel Pro metadata commands.

---

## Rollback Plan
If needed, restore the backed-up manifests from `openshift-backups/2026-11-22/`:
- `oc48 apply -f openshift-backups/2026-11-22/configmap-trino-chart-coordinator.yaml`
- `oc48 apply -f openshift-backups/2026-11-22/service-trino-chart.yaml`
- `oc48 apply -f openshift-backups/2026-11-22/deployment-trino-chart-coordinator.yaml`
- `oc48 apply -f openshift-backups/2026-11-22/deployment-trino-chart-worker.yaml`
