# Closes the TOCTOU window between the runner's `_has_running_run` check and the
# row insert via a partial unique index. Django's `UniqueConstraint(..., condition=...)`
# normally compiles to a non-concurrent `CREATE UNIQUE INDEX`, which locks the table
# while the index builds. We use `SeparateDatabaseAndState` so Django's model state
# still tracks the constraint while the actual DDL stays online via
# `CREATE UNIQUE INDEX CONCURRENTLY`. Same pattern as the rest of the codebase's
# safe non-blocking index migrations.
#
# Pre-index data cleanup: `CREATE UNIQUE INDEX CONCURRENTLY` fails if any duplicate
# (team_id, skill_name) WHERE status='running' rows already exist — and this PR is
# specifically fixing a race that can produce those duplicates. The runtime self-heal
# only kicks in after the migration succeeds, so without this step the deploy can be
# blocked by exactly the rows the rest of the change is designed to clean up. The
# data step keeps the newest RUNNING row per (team, skill) and reconciles the rest
# to FAILED with an explanatory summary. Idempotent — re-running on a clean table
# finds no duplicates and exits without writes.

from django.db import migrations, models


def _heal_duplicate_running_rows(apps, schema_editor):
    """Mark all-but-the-newest RUNNING row in each (team, skill_name) collision to FAILED.

    Runs before the CONCURRENTLY index so the unique build doesn't trip on
    pre-existing dupes. Safe to re-run.
    """
    from django.utils import timezone

    SignalScoutRun = apps.get_model("signals", "SignalScoutRun")
    # Historical model snapshots only expose managers declared in migrations. `0020` added
    # `all_teams = models.Manager()` (the unscoped sibling); the fail-closed `TeamScopedManager`
    # bound to `.objects` at runtime isn't serializable so isn't on the snapshot. Use `all_teams`
    # for cross-team migration reads — exactly the pattern called out in the scoping README.
    #
    # Pull every RUNNING row, group in Python by (team_id, skill_name). The cardinality
    # of stuck RUNNING rows is tiny (bounded by the worker pool + however long stale
    # rows have been accumulating); we deliberately do not stream.
    rows_by_key: dict[tuple[int, str], list] = {}
    for row in SignalScoutRun.all_teams.filter(status="running").only("id", "team_id", "skill_name", "started_at"):
        rows_by_key.setdefault((row.team_id, row.skill_name), []).append(row)

    now = timezone.now()
    for rows in rows_by_key.values():
        if len(rows) < 2:
            continue
        rows.sort(key=lambda r: r.started_at, reverse=True)
        to_heal = [r.id for r in rows[1:]]
        SignalScoutRun.all_teams.filter(pk__in=to_heal).update(
            status="failed",
            completed_at=now,
            summary=(
                "Run row auto-healed during partial-unique-index rollout: another RUNNING "
                "row for the same (team, skill_name) preceded the index creation. "
                "Worker / sandbox likely died without the cleanup path running."
            ),
        )


class Migration(migrations.Migration):
    # Required for CONCURRENTLY — Postgres rejects it inside a transaction. The
    # RunPython data step also runs outside a transaction; that's fine because the
    # per-key UPDATE is itself atomic and the operation is idempotent.
    atomic = False

    dependencies = [
        ("posthog", "1146_subscription_enabled"),
        ("signals", "0022_signalscoutconfig_runs_per_tick"),
    ]

    operations = [
        migrations.RunPython(
            _heal_duplicate_running_rows,
            reverse_code=migrations.RunPython.noop,
        ),
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddConstraint(
                    model_name="signalscoutrun",
                    constraint=models.UniqueConstraint(
                        condition=models.Q(("status", "running")),
                        fields=("team", "skill_name"),
                        name="signal_scout_run_one_running_per_team_skill",
                    ),
                ),
            ],
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
                        "signal_scout_run_one_running_per_team_skill "
                        'ON "signals_signalscoutrun" (team_id, skill_name) '
                        "WHERE status = 'running'"
                    ),
                    reverse_sql=("DROP INDEX CONCURRENTLY IF EXISTS signal_scout_run_one_running_per_team_skill"),
                ),
            ],
        ),
    ]
