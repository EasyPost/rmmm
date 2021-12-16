This is a small Rust application for managing database migrations for MySQL.

It compiles into a single binary called `rmmm`.

Configuration, by default, is through the `db/` directory of the directory in which `rmmm` is invoked. Migrations will
live in `db/migrations/*.sql` and structure will be dumped to `db/structure.sql`.

Basic usage:

 1. `rmmm generate foo` will pop up an editor for you to generate a migration
 1. `rmmm status` will show all pending migrations
 1. `rmmm upgrade latest` will apply pending migrations. You can also upgrade (or downgrade) to a specific version.

Versions are just incrementing integers for simplicity.

Configuration is typically through environment variables:

| `$DATABASE_URL` | URL (`mysql://`) to connect to MySQL |
| `$MIGRATION_PATH` | Path to store state (defaults to `./db`) |

This work is licensed under the ISC license, a copy of which can be found in [LICENSE.txt](LICENSE.txt).
