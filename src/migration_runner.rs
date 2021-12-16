use std::collections::BTreeSet;
use std::env;

use anyhow::Context;
use chrono::{TimeZone, Utc};
use itertools::Itertools;
use log::{debug, warn};
use mysql::prelude::Queryable;

use crate::migration_state::MigrationState;

pub(crate) struct MigrationRunner {
    pool: mysql::Pool,
    tx_opts: mysql::TxOpts,
}

#[derive(Debug)]
pub struct ExecutedMigration {
    pub id: usize,
    pub executed_at: chrono::DateTime<Utc>,
}

#[derive(Debug)]
pub struct MigrationStep {
    pub prev_id: Option<usize>,
    pub id: usize,
    pub sql: String,
}

#[derive(Debug)]
pub struct MigrationPlan {
    steps: Vec<MigrationStep>,
    is_upgrade: bool,
}

impl MigrationPlan {
    pub fn steps(&self) -> &[MigrationStep] {
        self.steps.as_slice()
    }

    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

impl MigrationRunner {
    pub fn new() -> anyhow::Result<Self> {
        let url = env::var("DATABASE_URL").context("Must set $DATABASE_URL")?;
        Ok(MigrationRunner {
            pool: mysql::Pool::new(mysql::Opts::from_url(&url)?)?,
            tx_opts: mysql::TxOpts::default()
                .set_isolation_level(Some(mysql::IsolationLevel::RepeatableRead)),
        })
    }

    pub fn list_run_migrations(&self) -> anyhow::Result<Vec<ExecutedMigration>> {
        let mut tx = self.pool.start_transaction(self.tx_opts)?;
        if tx
            .query_iter("SHOW TABLE STATUS LIKE 'rmmm_migrations'")?
            .count()
            == 0
        {
            warn!(
                "rmmm_migrations table does not exist; assuming no migrations have been run at all"
            );
            return Ok(vec![]);
        }
        let rows = tx.query_map(
            "SELECT id, executed_at FROM rmmm_migrations",
            |(id, executed_at)| ExecutedMigration {
                id,
                executed_at: Utc.timestamp(executed_at, 0),
            },
        )?;
        Ok(rows)
    }

    pub fn plan(
        &self,
        state: &MigrationState,
        target_revision: usize,
    ) -> anyhow::Result<MigrationPlan> {
        let highest_id = state.highest_id();
        if target_revision > highest_id {
            anyhow::bail!("Invalid target revision {}", target_revision);
        }
        let run_ids = self
            .list_run_migrations()?
            .into_iter()
            .map(|m| m.id)
            .collect::<BTreeSet<usize>>();
        let is_upgrade = if let Some(highest_run_id) = run_ids.iter().max() {
            *highest_run_id <= target_revision
        } else if target_revision == 0 {
            false
        } else {
            true
        };
        let state_by_id = state.migrations_by_id();
        let to_run = if is_upgrade {
            state
                .all_ids()
                .difference(&run_ids)
                .cloned()
                .sorted()
                .collect::<Vec<usize>>()
        } else {
            run_ids
                .iter()
                .filter(|&&i| i > target_revision)
                .cloned()
                .collect::<Vec<usize>>()
        };
        let steps = if is_upgrade {
            to_run
                .into_iter()
                .map(|id| MigrationStep {
                    prev_id: if id == 1 { None } else { Some(id - 1) },
                    id: id,
                    sql: state_by_id.get(&id).unwrap().upgrade_text.clone(),
                })
                .collect::<Vec<_>>()
        } else {
            to_run
                .into_iter()
                .rev()
                .map(|id| {
                    let step = state_by_id.get(&id).unwrap();
                    if let Some(sql) = step.downgrade_text.as_ref() {
                        Ok(MigrationStep {
                            prev_id: if id == highest_id { None } else { Some(id + 1) },
                            id,
                            sql: sql.clone(),
                        })
                    } else {
                        anyhow::bail!("step {:?} is irreversible", id);
                    }
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        };
        Ok(MigrationPlan { steps, is_upgrade })
    }

    fn now(&self) -> u64 {
        std::time::UNIX_EPOCH.elapsed().unwrap().as_secs()
    }

    pub fn execute(&self, plan: MigrationPlan) -> anyhow::Result<()> {
        let mut tx = self.pool.start_transaction(self.tx_opts)?;
        if tx
            .query_iter("SHOW TABLE STATUS LIKE 'rmmm_migrations'")?
            .count()
            == 0
        {
            debug!("creating rmmm_migrations table");
            tx.query_drop("CREATE TABLE rmmm_migrations(id INT NOT NULL PRIMARY KEY, executed_at BIGINT NOT NULL)")?;
        }
        let insert_stmt = tx.prep("INSERT INTO rmmm_migrations(id, executed_at) VALUES(?, ?)")?;
        let delete_stmt = tx.prep("DELETE FROM rmmm_migrations WHERE id = ?")?;
        for step in plan.steps {
            for command in step.sql.split(";\n") {
                let command = command.replace('\n', " ").trim().to_owned();
                if command.is_empty() {
                    continue;
                }
                debug!("executing {:?}", command);
                tx.query_drop(command)?;
            }
            if plan.is_upgrade {
                tx.exec_drop(&insert_stmt, (step.id, self.now()))?;
            } else {
                tx.exec_drop(&delete_stmt, (step.id,))?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_tables(&self) -> anyhow::Result<Vec<String>> {
        let mut tx = self.pool.start_transaction(self.tx_opts)?;
        let db_name = tx
            .query_map("SELECT DATABASE()", |db_name: String| db_name)?
            .into_iter()
            .next()
            .unwrap();
        let stmt =
            tx.prep("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema=?")?;
        tx.exec_map(stmt, (db_name,), |table_name: String| table_name)
            .context("Could not list tables")
    }

    pub fn drop_table(&self, table_name: &str) -> anyhow::Result<()> {
        let mut tx = self.pool.start_transaction(self.tx_opts)?;
        assert!(!table_name.contains('`'));
        tx.query_drop(format!("DROP TABLE `{}`", table_name))?;
        Ok(())
    }

    pub fn dump_schema(&self) -> anyhow::Result<String> {
        let mut tables = self.list_tables()?;
        tables.sort();
        let mut tx = self.pool.start_transaction(self.tx_opts)?;
        let mut lines = Vec::with_capacity(tables.len());
        for table_name in &tables {
            assert!(!table_name.contains('`'));
            let schema = tx.query_map(
                format!("SHOW CREATE TABLE `{}`", table_name),
                |(_table_name, mut schema): (String, String)| {
                    schema.push(';');
                    schema
                },
            )?;
            lines.extend(schema);
        }
        if tables.contains(&"rmmm_migrations".to_owned()) {
            lines.extend(vec!["".to_string()]);
            lines.extend(tx.query_map(
                "SELECT id FROM rmmm_migrations ORDER BY id ASC",
                |id: u64| {
                    format!(
                        "INSERT INTO rmmm_migrations(id, executed_at) VALUES({}, NOW());",
                        id
                    )
                },
            )?);
        }
        Ok(lines.join("\n"))
    }
}
