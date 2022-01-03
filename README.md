This is a small Rust application for managing database migrations for MySQL.

![CI](https://github.com/EasyPost/rmmm/workflows/CI/badge.svg?branch=master)

It compiles into a single binary called `rmmm`.

Configuration, by default, is through the `db/` directory of the directory in which `rmmm` is invoked. Migrations will
live in `db/migrations/v{version}.sql`, rollbacks in `db/migrations/v{version}_downgrade.sql`,
and structure will be dumped to `db/structure.sql`.

Basic usage:

 1. `cargo install rmmm`
 1. `rmmm generate foo` will pop up an editor for you to write a migration. Migrations may be any number of SQL statements on lines by themselves ending with the `;` character. Comments are stripped.
 1. `rmmm status` will show all pending migrations
 1. `rmmm upgrade latest` will apply pending migrations. You can also upgrade (or downgrade) to a specific version.

Modifying actions will only print out what they would do by default and must be run with `--execute` to make changes.

Schema versions are just incrementing integers for simplicity.

Configuration is typically through environment variables:

| Environment Variable | Meaning |
|----------------------|---------|
| `$DATABASE_URL` | URL (`mysql://`) to connect to MySQL |
| `$MIGRATION_PATH` | Path to store state (defaults to `./db`) |

This work is licensed under the ISC license, a copy of which can be found in [LICENSE.txt](LICENSE.txt).

Features
--------
RMMM supports the following feature flags:

| Name | Meaning | Enabled by default |
| `uuid` | Add support for native MySQL UUID types | ✓ |
| `native-tls` | Use [native-tls](https://crates.io/crates/native-tls) to get SSL support via the local library (OpenSSL, etc) | |
| `rustls-tls` | Use [rustls](https://crates.io/crates/rustls) to get SSL support | |

You can enable exactly zero or one of `native-tls` or `rustls-tls`.

Why?
----
There are lots of migration management tools. A popular stand-alone choice is
[dogfish](https://github.com/dwb/dogfish); there are also tools using richer libraries for various
ecosystems such as [barrel](https://git.irde.st/spacekookie/barrel) for diesel, or Python's
[alembic](https://alembic.sqlalchemy.org/en/latest/).

This tool is closest to dogfish, but avoids the various shell injection risks and uses the same `DATABASE_URL`
configuration string as other common frameworks (Rust's `mysql`, Python's `sqlalchemy`, Ruby's `activerecord`, etc).
