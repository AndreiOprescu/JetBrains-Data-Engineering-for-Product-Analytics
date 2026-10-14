# Assumptions and additional metadata

## Assumptions

### About the raw data
1. **The raw events are Trino `QueryCompletedEvent`s**, landed by an event
   listener sink (for example Kafka event listener → Kafka → Iceberg sink, or
   HTTP event listener → collector) in `raw_trino.query_completed`. The schema
   is in `sql/ddl/01_raw.sql`. If our sink uses different column names, the two
   fact transformations are the only place that changes.
2. **Delivery is at-least-once**: the same `query_id` can arrive more than
   once. The copy with the latest `ingested_at` wins.
3. **Events can be late** (sink outage, replay). `ingested_at` is stamped by
   the sink when the event is written. It's monotonic enough for watermarking,
   with a 15-minute safety lag.
4. **Timestamps are UTC**, and the reporting day is the UTC date of `end_time`.
5. `ioMetadata.inputs` lists the **base tables actually read**, with views
   expanded. Each input carries `physical_input_bytes`/`rows`, which Iceberg
   reports; JDBC connectors report rows but 0 bytes.
6. `cumulative_memory_byte_seconds` is the event's cumulative memory
   normalised to byte-seconds by the sink. Trino exposes cumulative memory as
   a memory × time integral; if the sink keeps the original unit, convert it
   in `20_fct_query.sql`.
7. Catalog, schema and table identifiers contain no dots, so `table_fqn` can be
   split back into its parts. Identifiers are treated case-insensitively.

### About users and teams
8. **The user is `context.user`** (the session user after impersonation), not
   the authenticated `principal`. With Superset/BI impersonation that's the
   real person. With a shared service login and no impersonation, the
   service account is the user, and its team is whoever owns that account.
9. **A user belongs to exactly one team on any day.** The membership history
   comes from the IdP. If the source has overlapping intervals, the most
   recently started one wins from its start date. If organisations really do
   need split membership, see the alternative below.
10. **Service accounts are mapped to the owning team** in the same membership
    table as people. Their account type comes from `meta_ref.user_account`;
    if it's missing, names starting with `svc` are treated as service accounts.
11. Users not in the membership table go to the team `__unassigned__`, and a
    data-quality warning lists them.
12. **Teams are attributed point-in-time**: a query belongs to the team the
    user was in on that day. "Current team" views are available via
    `dim_user.current_team_id`.

### About tables
13. **Only Iceberg tables in data-lake catalogs** (`meta_ref.catalog.is_data_lake`)
    count as "our tables". Every table Trino touches is still recorded, so
    federated sources (PostgreSQL) and `system.*` can be analysed or filtered.
14. **Ownership comes from the data catalog**, at table level or with a
    schema-level default (`table_name = '*'`). An exact table entry beats the
    schema default.
15. **Multi-table queries count as usage for each table they touch.** CPU and
    memory are split between tables in proportion to the bytes read from each.
    The full rules are in [design_decisions.md](design_decisions.md#4-multi-table-queries).

### About resources and cost
16. **Failed queries count**: they used CPU, memory and I/O. They are flagged
    (`is_success`) so they can be excluded or reported separately as waste.
17. Cost is an **internal showback estimate**: CPU-hours, memory GB-hours and
    TB scanned, each times a rate from a versioned price list. It's not an
    invoice. Storage cost isn't included, because it isn't in query events
    (see possible extensions).

## Additional metadata

The query events say *who ran what*. They don't say *who belongs to which
team*, *who owns which table*, or *what a CPU-hour costs*. That comes from
these reference tables (DDL in `sql/ddl/02_reference.sql`, example data in
`reference_data/`). Each is a full snapshot replaced on every sync
(`metadwh/reference_sync.py`).

| Table | Structure | Source of truth | Used for |
|---|---|---|---|
| `meta_ref.team` | `team_id`, `team_name`, `department`, `cost_center`, `is_active` | HR system / IdP groups | `dim_team`; cost-centre roll-ups |
| `meta_ref.user_team_membership` | `user_name`, `team_id`, `valid_from`, `valid_to` (exclusive, NULL = current) | IdP group-membership history (Okta/LDAP export; SCIM sync with a change log) | `dim_user_team_history` → point-in-time `fct_query.team_id`, `dim_user.current_team_id` |
| `meta_ref.user_account` | `user_name`, `account_type` (human/service), `display_name`, `is_active` | IdP | `dim_user`; human vs service usage split (Q1 breakdown) |
| `meta_ref.table_ownership` | `catalog_name`, `schema_name`, `table_name` (or `*`), `owner_team_id`, `data_domain` | Data catalog (DataHub/Glue tags), or a CODEOWNERS-style file in the repo that defines the tables | `dim_table.owner_team_id`; cross-team consumption; inventory of never-queried tables |
| `meta_ref.catalog` | `catalog_name`, `connector_type`, `is_data_lake` | Trino catalog configuration | Separates Iceberg data-lake tables from federated or system tables |
| `meta_ref.cost_rate` | `valid_from`, `valid_to`, `cpu_hour_usd`, `memory_gb_hour_usd`, `scanned_tb_usd` | FinOps (cluster cost ÷ capacity) | Cost views; versioned, so history uses the rates of its time |

**Important:** if the IdP only gives a *current* snapshot (no history), the
sync job should build the history itself. It compares each snapshot with the
previous one, closes changed rows (`valid_to = today`) and opens new ones.
That is a standard SCD2 merge, and it's why the table is shaped as intervals.

### Alternative if users can be in several teams at once
Add `allocation_pct` to `user_team_membership` and turn the user → team mapping
into a bridge table. `fct_query` would then need one row per query × team, or
a separate `fct_query_team_allocation`, with resources multiplied by
`allocation_pct`. That's more complex, and only worth it if split membership
is common. The single-team rule is the usual way showback is done.

## Open questions to confirm with the platform team
* How exactly does our sink name and unit the fields (especially memory and timestamps)?
* Are BI tools set up to impersonate end users? If not, BI usage is attributed to the BI service account's team.
* Should usage by the Meta DWH pipeline itself, and other platform service accounts, be excluded from team charge-back? (Today it's attributed to its owning team.)
* What retention is allowed for `query_text` in the raw layer?
