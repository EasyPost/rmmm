use std::collections::{BTreeMap, BTreeSet};
use std::env;

use anyhow::Context;
use clap::{Arg, SubCommand};
use derive_more::Display;
use itertools::Itertools;
use log::{debug, error, info};
use tabled::Tabled;

mod migration_runner;
mod migration_state;

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
    id: usize,
    label: String,
    status: MigrationStatus,
    executed_at: String,
}

fn command_status(state: MigrationState) -> anyhow::Result<()> {
    debug!("Starting command_status");
    let runner = migration_runner::MigrationRunner::new()?;
    let run_so_far = runner.list_run_migrations()?;
    let all_ids = state
        .all_ids()
        .union(&run_so_far.iter().map(|m| m.id).collect::<BTreeSet<usize>>())
        .cloned()
        .collect::<BTreeSet<usize>>();
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
                .map(|r| r.executed_at.to_rfc3339())
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
    let table = tabled::Table::new(&data).with(tabled::Style::pseudo_clean());
    println!("{}", table);
    Ok(())
}

#[derive(Tabled, Debug)]
struct MigrationPlanRow {
    id: usize,
    prev_id: String,
    sql_text: String,
}

fn command_upgrade(matches: &clap::ArgMatches, state: MigrationState) -> anyhow::Result<()> {
    debug!("Starting command_upgrade");
    let runner = migration_runner::MigrationRunner::new()?;
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
    let plan = runner.plan(&state, target_revision)?;
    if plan.is_empty() {
        info!("Nothing to do!");
        return Ok(());
    }
    let plan_data = plan
        .steps()
        .iter()
        .map(|ps| MigrationPlanRow {
            id: ps.id,
            prev_id: ps
                .prev_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| "(none)".to_string()),
            sql_text: ps.sql.clone(),
        })
        .collect::<Vec<_>>();
    let table = tabled::Table::new(&plan_data)
        .with(tabled::Style::pseudo_clean())
        .with(tabled::Modify::new(tabled::Column(2..=2)).with(tabled::Alignment::left()));
    println!("Migration plan:");
    println!("{}", table);
    if matches.is_present("execute") {
        info!("executing plan with {} steps", plan.steps().len());
        runner.execute(plan)?;
        info!("done!");
        println!("New version: {}", target_revision);
        let schema = runner.dump_schema()?;
        state.write_schema(&schema)?;
    } else {
        error!("rerun with --execute to execute this plan");
    }
    Ok(())
}

fn command_reset(matches: &clap::ArgMatches) -> anyhow::Result<()> {
    debug!("Starting command_reset");
    let runner = migration_runner::MigrationRunner::new()?;
    let tables = runner.list_tables()?;
    println!("Dropping the following tables:");
    for table in &tables {
        println!(" - {}", table);
    }
    if matches.is_present("execute") {
        for table in tables {
            runner.drop_table(&table)?;
        }
    } else {
        error!("rerun with --execute to execute this plan");
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let matches = clap::App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .global_settings(&[clap::AppSettings::ColorAuto, clap::AppSettings::ColoredHelp])
        .arg(
            Arg::with_name("migration_path")
                .short("p")
                .long("migration-path")
                .takes_value(true)
                .default_value("db")
                .env("MIGRATION_PATH")
                .help("Directory in which state is stored"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .help("Be less noisy when logging"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .multiple(true)
                .help("Be more noisy when logging"),
        )
        .subcommand(SubCommand::with_name("status").about("Show the current status of migrations"))
        .subcommand(
            SubCommand::with_name("generate")
                .about("Generate a new migration")
                .arg(
                    Arg::with_name("label")
                        .required(true)
                        .help("Descriptive one-line label for the migration"),
                ),
        )
        .subcommand(
            SubCommand::with_name("upgrade")
                .about("Upgrade to the given revision")
                .arg(
                    Arg::with_name("revision")
                        .required(true)
                        .help("Revision to which to upgrade (or 'latest')"),
                )
                .arg(
                    Arg::with_name("execute")
                        .short("x")
                        .long("execute")
                        .help("Actually upgrade (otherwise will just print what will be done"),
                ),
        )
        .subcommand(
            SubCommand::with_name("downgrade")
                .about("Downgrade to the given revision")
                .arg(
                    Arg::with_name("revision")
                        .required(true)
                        .help("Revision to which to downgrade"),
                )
                .arg(
                    Arg::with_name("execute")
                        .short("x")
                        .long("execute")
                        .help("Actually upgrade (otherwise will just print what will be done"),
                ),
        )
        .subcommand(
            SubCommand::with_name("reset")
                .about("Drop all tables and totally reset the database (DANGEROUS)")
                .arg(
                    Arg::with_name("execute")
                        .short("x")
                        .long("execute")
                        .help("Actually reset"),
                ),
        )
        .get_matches();

    initialize_logging(&matches);

    let current_state = MigrationState::load(matches.value_of("migration_path").unwrap())?;

    match matches.subcommand() {
        ("generate", Some(smatches)) => {
            current_state.generate(smatches.value_of("label").unwrap())?
        }
        ("status", _) => {
            command_status(current_state)?;
        }
        ("upgrade", Some(smatches)) => {
            command_upgrade(smatches, current_state)?;
        }
        ("downgrade", Some(smatches)) => {
            command_upgrade(smatches, current_state)?;
        }
        ("reset", Some(smatches)) => command_reset(smatches)?,
        (_, _) => {
            anyhow::bail!("Must pass a command!");
        }
    };
    Ok(())
}
