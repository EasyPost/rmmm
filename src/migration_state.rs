use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Context;
use itertools::Itertools;
use log::debug;

const DEFAULT_EDITOR: &str = "nano";

#[derive(Debug)]
pub(crate) struct Migration {
    pub id: usize,
    pub label: Option<String>,
    pub upgrade_text: String,
    pub downgrade_text: Option<String>,
}

impl Migration {
    fn read_sql_from_path(p: &Path) -> anyhow::Result<String> {
        lazy_static::lazy_static! {
            static ref ONE_LINE_COMMENT_RE: regex::Regex =
                regex::Regex::new(r"^-- (.*)$").unwrap();
            static ref MULTILINE_COMMENT_RE: regex::Regex =
                regex::Regex::new(r"/\* .* \*/").unwrap();
            static ref EMPTY_LINE_RE: regex::Regex =
                regex::Regex::new(r"^\s+\n").unwrap();
        }
        let s = std::fs::read_to_string(p)?;
        let s = ONE_LINE_COMMENT_RE.replace_all(&s, "");
        let s = MULTILINE_COMMENT_RE.replace_all(&s, "");
        let s = EMPTY_LINE_RE.replace_all(&s, "");
        Ok(s.to_string())
    }

    fn from_path(id: usize, p: &Path) -> anyhow::Result<Self> {
        let upgrade_file = std::fs::read_to_string(p)?;
        lazy_static::lazy_static! {
            static ref LABEL_RE: regex::Regex =
                regex::Regex::new(r"^/\* rmmm migration v[0-9]+ - (.*) \*/$").unwrap();
        }
        let label = upgrade_file
            .lines()
            .next()
            .and_then(|first_line| LABEL_RE.captures(first_line))
            .map(|c| c.get(1).unwrap().as_str());
        let upgrade_text = Migration::read_sql_from_path(p)?;
        let downgrade_p = p.with_file_name(format!("v{0}_downgrade.sql", id));
        let downgrade_text = if downgrade_p.exists() {
            Some(Migration::read_sql_from_path(&downgrade_p)?)
        } else {
            None
        };
        debug!("Found upgrade text {:?}", upgrade_text);
        debug!("Found downgrade text {:?}", downgrade_text);
        Ok(Migration {
            id,
            upgrade_text,
            downgrade_text,
            label: label.map(|s| s.to_string()),
        })
    }
}

pub(crate) struct MigrationState {
    root_path: PathBuf,
    pub migrations: Vec<Migration>,
    next_id: usize,
}

impl MigrationState {
    pub fn load<P: Into<PathBuf>>(root_path: P) -> anyhow::Result<Self> {
        let root_path = root_path.into();
        if !root_path.exists() {
            return Ok(MigrationState {
                root_path,
                migrations: vec![],
                next_id: 1,
            });
        }
        let migrations = (1..)
            .map(|id| {
                let expected_path = root_path.join("migrations").join(format!("v{0}.sql", id));
                if expected_path.exists() {
                    debug!("Loading migration from {:?}", expected_path);
                    Some(
                        Migration::from_path(id, &expected_path)
                            .with_context(|| format!("Could not load migration {0}", id))
                            .unwrap(),
                    )
                } else {
                    None
                }
            })
            .while_some()
            .collect::<Vec<_>>();
        let next_id = migrations.iter().map(|m| m.id).last().unwrap_or(0) + 1;
        Ok(MigrationState {
            root_path,
            migrations,
            next_id,
        })
    }

    pub fn generate(&self, label: &str) -> anyhow::Result<()> {
        let migrations_path = self.root_path.join("migrations");
        std::fs::create_dir_all(&migrations_path)?;
        let next_file = format!("v{0}.sql", self.next_id);
        let f = tempfile::Builder::new()
            .suffix(".sql")
            .tempfile_in(&migrations_path)?;
        {
            let mut f = f.as_file();
            writeln!(f, "/* rmmm migration v{0} - {1} */", self.next_id, label)?;
            writeln!(f, "\n-- Delete this comment and put your migration here. Blank lines and comments are ignored.")?;
            writeln!(
                f,
                "-- Create {0}/v{1}_downgrade.sql to make this migraiton reversible",
                migrations_path.to_string_lossy(),
                self.next_id
            )?;
            f.sync_all()?;
        }
        let editor = env::var("EDITOR").unwrap_or_else(|_| DEFAULT_EDITOR.to_string());
        let status = std::process::Command::new(editor)
            .arg(f.path())
            .status()
            .expect(
                "Could not invoke editor on migration; try setting $EDITOR to something useful",
            );
        if status.success() {
            f.persist_noclobber(migrations_path.join(next_file))?;
        } else {
            anyhow::bail!("Editor exited non-0, discarding migration");
        }
        Ok(())
    }

    pub fn migrations_by_id(&self) -> BTreeMap<usize, &Migration> {
        self.migrations.iter().map(|m| (m.id, m)).collect()
    }

    pub fn all_ids(&self) -> BTreeSet<usize> {
        self.migrations.iter().map(|m| m.id).collect()
    }

    pub fn highest_id(&self) -> usize {
        self.next_id - 1
    }

    pub fn write_schema(&self, schema: &str) -> anyhow::Result<()> {
        let schema_file = self.root_path.join("structure.sql");
        std::fs::write(schema_file, schema)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::MigrationState;

    #[test]
    fn test_basic_flow() {
        let wd = tempfile::TempDir::new().unwrap();
        let uut = MigrationState::load(wd.path()).expect("Should load empty dir");
        assert_eq!(uut.all_ids().len(), 0);
        assert_eq!(uut.highest_id(), 0);
        assert_eq!(uut.migrations_by_id().len(), 0);
    }

    #[test]
    fn test_exists() {
        let wd = tempfile::TempDir::new().unwrap();
        let v1 = "CREATE TABLE test(id INT PRIMARY KEY, value BLOB)";
        let v2 = "ALTER TABLE test ADD INDEX `idx_test_on_value` (`value`)";
        std::fs::create_dir_all(wd.path().join("migrations")).unwrap();
        std::fs::write(wd.path().join("migrations").join("v1.sql"), v1).unwrap();
        std::fs::write(wd.path().join("migrations").join("v2.sql"), v2).unwrap();
        let uut = MigrationState::load(wd.path()).expect("Should load full dir");
        assert_eq!(uut.all_ids().len(), 2);
        assert_eq!(uut.highest_id(), 2);
        assert_eq!(uut.migrations_by_id().len(), 2);
    }
}
