#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;

use anyhow::Context;
use clap::Arg;
use derive_more::Display;
use itertools::Itertools;
use log::{debug, error, info};
use tabled::Tabled;

mod go_database_dsn;
mod migration_runner;
mod migration_state;

use crate::migration_runner::MigrationRunner;
use crate::migration_state::MigrationState;

fn initialize_logging(matches: &clap::ArgMatches) {
    let log_level = match (
        matches.is_present("quiet"),
        matches.occurrences_of("verbose"),
    ) {
        (true, _) => log::LevelFilter::Error,
        (false, 0) => env::var("RUST_LOG")
            .ok()
            .and_then(|v| v.parse::<log::LevelFilter>().ok())
            .unwrap_or(log::LevelFilter::Warn),
        (_, 1) => log::LevelFilter::Info,
        (_, 2) => log::LevelFilter::Debug,
        (_, _) => log::LevelFilter::Trace,
    };
    let colors = fern::colors::ColoredLevelConfig::new()
        .info(fern::colors::Color::Blue)
        .debug(fern::colors::Color::Cyan);
    fern::Dispatch::new()
        .level(log_level)
        .format(move |out, message, record| {
            out.finish(format_args!(
                "[{level_on}{time} {level:5} {target}{level_off}] {message}",
                level_on = format_args!("\x1B[{}m", colors.get_color(&record.level()).to_fg_str()),
                level_off = "\x1B[0m",
                time = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S"),
                level = record.level(),
                target = record.target(),
                message = message
            ))
        })
        .chain(std::io::stderr())
        .apply()
        .expect("Could not initialize logging");
}

#[derive(Debug, Display, PartialEq, Eq)]
enum MigrationStatus {
    Executed,
    NotExecuted,
}

#[derive(Tabled, Debug)]
struct MigrationStatusRow {
    id: u32,
    label: String,
    status: MigrationStatus,
    executed_at: String,
}

fn command_status(state: MigrationState, runner: MigrationRunner) -> anyhow::Result<()> {
    debug!("Starting command_status");
    let run_so_far = runner.list_run_migrations()?;
    let all_ids = state
        .all_ids()
        .union(&run_so_far.iter().map(|m| m.id).collect::<BTreeSet<u32>>())
        .cloned()
        .collect::<BTreeSet<u32>>();
    let migrations_by_id = state.migrations_by_id();
    let run_so_far_by_id = run_so_far
        .into_iter()
        .map(|m| (m.id, m))
        .collect::<BTreeMap<_, _>>();
    let data = all_ids
        .into_iter()
        .sorted()
        .map(|id| {
            let label = if let Some(l) = migrations_by_id.get(&id).and_then(|r| r.label.as_ref()) {
                l
            } else {
                "unknown"
            };
            let executed_at = run_so_far_by_id
                .get(&id)
                .map(|r| r.executed_at.map_or("".to_string(), |ea| ea.to_rfc3339()))
                .unwrap_or_else(|| "".to_string());
            MigrationStatusRow {
                id,
                executed_at,
                status: if run_so_far_by_id.contains_key(&id) {
                    MigrationStatus::Executed
                } else {
                    MigrationStatus::NotExecuted
                },
                label: label.to_string(),
            }
        })
        .collect::<Vec<_>>();
    let table = tabled::Table::new(&data).with(tabled::Style::modern().horizontal_off());
    println!("{table}");
    Ok(())
}

#[derive(Tabled, Debug)]
struct MigrationPlanRow {
    id: u32,
    sql_text: String,
}

fn command_apply_migrations(
    matches: &clap::ArgMatches,
    state: MigrationState,
    runner: MigrationRunner,
    is_upgrade: bool,
) -> anyhow::Result<()> {
    debug!("Starting command_upgrade");
    let target_revision = {
        let revision = matches.value_of("revision").unwrap();
        if revision == "latest" {
            state.highest_id()
        } else {
            revision
                .parse()
                .context("revision must be an integer or 'latest'")?
        }
    };
    let plan = runner.plan(&state, target_revision, is_upgrade)?;
    if plan.is_empty() {
        info!("Nothing to do!");
        return Ok(());
    }
    let plan_data = plan
        .steps()
        .iter()
        .map(|ps| MigrationPlanRow {
            id: ps.id,
            sql_text: ps.sql.clone(),
        })
        .collect::<Vec<_>>();
    let table = tabled::Table::new(&plan_data)
        .with(tabled::Style::modern().horizontal_off())
        .with(tabled::Modify::new(tabled::Column(1..=1)).with(tabled::Alignment::left()))
        ;
    println!("Migration plan:");
    println!("{table}");
    if matches.is_present("execute") {
        info!("executing plan with {} steps", plan.steps().len());
        runner.execute(plan)?;
        info!("done!");
        println!("New version: {target_revision}");
        if !matches.is_present("no-dump") {
            let schema = runner.dump_schema()?;
            state.write_schema(&schema)?;
        } else {
            println!("not writing schema file");
        }
    } else {
        error!("rerun with --execute to execute this plan");
    }
    Ok(())
}

fn command_reset(
    matches: &clap::ArgMatches,
    runner: &MigrationRunner,
    quiet: bool,
) -> anyhow::Result<()> {
    debug!("Starting command_reset");
    let tables = runner.list_tables()?;
    if !quiet {
        println!("Dropping the following tables:");
        for table in &tables {
            println!(" - {table}");
        }
    }
    if matches.is_present("execute") {
        for table in tables {
            runner.drop_table(&table)?;
        }
    } else {
        error!("rerun with --execute to execute this reset plan");
    }
    Ok(())
}

fn command_apply_snapshot(
    matches: &clap::ArgMatches,
    state: MigrationState,
    runner: &MigrationRunner,
    quiet: bool,
) -> anyhow::Result<()> {
    command_reset(matches, runner, quiet)?;
    let schema = state.read_schema()?;
    if matches.is_present("execute") {
        runner.apply_schema_snapshot(&schema)?;
        let run_so_far = runner.list_run_migrations()?;
        println!(
            "Migrations applied after snapshot application: {:?}",
            run_so_far.into_iter().map(|r| r.id).collect::<Vec<_>>()
        );
    } else {
        error!(
            "rerun with --execute to apply the {0}-byte snapshot from structure.sql",
            schema.len()
        );
    }
    Ok(())
}

fn cli() -> clap::Command<'static> {
    clap::Command::new(clap::crate_name!())
        .version(clap::crate_version!())
        .author(clap::crate_authors!())
        .about(clap::crate_description!())
        .arg(
            Arg::new("migration_path")
                .short('p')
                .long("migration-path")
                .takes_value(true)
                .default_value("db")
                .env("MIGRATION_PATH")
                .help("Directory in which state is stored"),
        )
        .arg(
            Arg::new("quiet")
                .short('q')
                .long("quiet")
                .help("Be less noisy when logging"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .multiple_occurrences(true)
                .help("Be more noisy when logging"),
        )
        .arg(
            Arg::new("database_url")
                .long("database-url")
                .env("DATABASE_URL")
                .takes_value(true)
                .forbid_empty_values(true)
                .value_hint(clap::ValueHint::Url)
                .value_name("URL")
                .help("mysql:// database URL"),
        )
        .arg(
            Arg::new("database_dsn")
                .long("database-dsn")
                .env("DATABASE_DSN")
                .takes_value(true)
                .forbid_empty_values(true)
                .value_name("DSN")
                .help("go-style database DSN"),
        )
        .group(
            clap::ArgGroup::default()
                .id("database_config")
                .args(&["database_url", "database_dsn"])
                .required(true),
        )
        .subcommand(clap::Command::new("status").about("Show the current status of migrations"))
        .subcommand(
            clap::Command::new("generate")
                .about("Generate a new migration")
                .arg(
                    Arg::new("label")
                        .required(true)
                        .help("Descriptive one-line label for the migration"),
                ),
        )
        .subcommand(
            clap::Command::new("upgrade")
                .about("Upgrade to the given revision")
                .arg(
                    Arg::new("revision")
                        .required(true)
                        .help("Revision to which to upgrade (or 'latest')"),
                )
                .arg(
                    Arg::new("execute")
                        .short('x')
                        .long("execute")
                        .help("Actually upgrade (otherwise will just print what would be done)"),
                )
                .arg(
                    Arg::new("no-dump")
                        .long("--no-write-schema")
                        .env("NO_WRITE_SCHEMA")
                        .help("Do not write updated db/structure.sql when done"),
                ),
        )
        .subcommand(
            clap::Command::new("apply-snapshot")
                .about("Apply a snapshot (structure.sql file). Does the equivalent of a reset first.")
                .arg(
                    Arg::new("execute")
                    .short('x')
                    .long("execute")
                    .help("Actually wipe and apply the snapshot (otherwise, will just print what would be done)")
                )
        )
        .subcommand(
            clap::Command::new("downgrade")
                .about("Downgrade to the given revision")
                .arg(
                    Arg::new("revision")
                        .required(true)
                        .help("Revision to which to downgrade"),
                )
                .arg(
                    Arg::new("execute")
                        .short('x')
                        .long("execute")
                        .help("Actually upgrade (otherwise will just print what will be done"),
                )
                .arg(
                    Arg::new("no-dump")
                        .long("--no-write-schema")
                        .env("NO_WRITE_SCHEMA")
                        .help("Do not write updated db/structure.sql when done"),
                ),
        )
        .subcommand(
            clap::Command::new("reset")
                .about("Drop all tables and totally reset the database (DANGEROUS)")
                .arg(
                    Arg::new("execute")
                        .short('x')
                        .long("execute")
                        .help("Actually reset"),
                ),
        )
}

fn main() -> anyhow::Result<()> {
    let matches = cli().get_matches();

    initialize_logging(&matches);

    let current_state = MigrationState::load(matches.value_of("migration_path").unwrap())?;

    let runner = MigrationRunner::from_matches(&matches)?;

    match matches.subcommand() {
        Some(("generate", smatches)) => {
            current_state.generate(smatches.value_of("label").unwrap())?
        }
        Some(("status", _)) => {
            command_status(current_state, runner)?;
        }
        Some(("upgrade", smatches)) => {
            command_apply_migrations(smatches, current_state, runner, true)?;
        }
        Some(("downgrade", smatches)) => {
            command_apply_migrations(smatches, current_state, runner, false)?;
        }
        Some(("apply-snapshot", smatches)) => {
            command_apply_snapshot(
                smatches,
                current_state,
                &runner,
                matches.is_present("quiet"),
            )?;
        }
        Some(("reset", smatches)) => command_reset(smatches, &runner, matches.is_present("quiet"))?,
        _ => {
            cli().print_help()?;
            anyhow::bail!("Must pass a command!");
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::cli;

    #[test]
    fn test_cli() {
        cli().debug_assert();
    }
}
