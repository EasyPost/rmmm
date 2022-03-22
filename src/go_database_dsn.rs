use std::convert::TryInto;
use std::net::IpAddr;
use std::str::FromStr;

use anyhow::Context;
use once_cell::sync::Lazy;
use regex::Regex;

const DEFAULT_PORT: u16 = 3306;

#[derive(Debug, PartialEq, Eq)]
enum AddressName {
    Address(IpAddr),
    Name(String),
}

impl AddressName {
    fn into_mysql_string(self) -> String {
        match self {
            Self::Name(s) => s,
            Self::Address(IpAddr::V4(i)) => i.to_string(),
            Self::Address(IpAddr::V6(i)) => format!("[{}]", i),
        }
    }
}

impl FromStr for AddressName {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.parse() {
            Ok(ip_addr) => AddressName::Address(ip_addr),
            Err(_) => AddressName::Name(s.into()),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Address {
    name: AddressName,
    port: u16,
}

impl FromStr for Address {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(addr) = s.strip_prefix('[') {
            // IPv6 literal
            let (addr, rest) = addr
                .split_once(']')
                .ok_or_else(|| anyhow::anyhow!("invalid IPv6 literal in {}", s))?;
            let addr = AddressName::Address(addr.parse().context("invalid IPv6 literal")?);
            if let Some((_, port)) = rest.rsplit_once(':') {
                Ok(Address {
                    name: addr,
                    port: port.parse()?,
                })
            } else {
                Ok(Address {
                    name: addr,
                    port: DEFAULT_PORT,
                })
            }
        } else if let Some((address, port)) = s.rsplit_once(':') {
            Ok(Address {
                name: address.parse()?,
                port: port.parse()?,
            })
        } else {
            Ok(Address {
                name: s.parse()?,
                port: DEFAULT_PORT,
            })
        }
    }
}

static DSN_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?x)
        (?:
            (?P<username>[^:@]*)
            (?: : (?P<password>[^@]*) )?
        @
        )?
        (?P<protocol>[a-z]+)
        \(
            (?P<address>[^)]+)
        \)
        /
        (?P<dbname>[^?]+)
        (?:
            \?
            (?P<params>.*)
        )?
    ",
    )
    .unwrap()
});

#[derive(Debug)]
pub(crate) struct GoDatabaseDsn {
    username: Option<String>,
    password: Option<String>,
    address: Address,
    database: String,
}

impl FromStr for GoDatabaseDsn {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let caps = DSN_REGEX
            .captures(s)
            .ok_or_else(|| anyhow::anyhow!("Invalid DSN {}", s))?;
        let username = caps.name("username").map(|s| s.as_str().to_owned());
        let password = caps.name("password").map(|s| s.as_str().to_owned());
        match caps.name("protocol").map(|s| s.as_str()) {
            Some("tcp") => {}
            Some(other) => anyhow::bail!("unhandled DSN protocol {}", other),
            None => {}
        }
        let address = caps
            .name("address")
            .ok_or_else(|| anyhow::anyhow!("no address in DSN {}", s))?
            .as_str()
            .parse()?;
        let database = caps
            .name("dbname")
            .ok_or_else(|| anyhow::anyhow!("no dbname in DSN {}", s))?
            .as_str()
            .to_owned();
        Ok(GoDatabaseDsn {
            username,
            password,
            address,
            database,
        })
    }
}

impl TryInto<mysql::Opts> for GoDatabaseDsn {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<mysql::Opts, Self::Error> {
        Ok(mysql::OptsBuilder::new()
            .user(self.username)
            .pass(self.password)
            .db_name(Some(self.database))
            .tcp_port(self.address.port)
            .ip_or_hostname(Some(self.address.name.into_mysql_string()))
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::{Address, AddressName, GoDatabaseDsn};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use anyhow::Context;

    #[test]
    fn test_address_name_parser() {
        assert_eq!(
            "127.0.0.1".parse(),
            Ok(AddressName::Address(IpAddr::V4(Ipv4Addr::new(
                127, 0, 0, 1
            ))))
        );
        assert_eq!(
            "::1".parse(),
            Ok(AddressName::Address(IpAddr::V6(Ipv6Addr::new(
                0, 0, 0, 0, 0, 0, 0, 1
            ))))
        );
    }

    #[test]
    fn test_address_parser() {
        assert_eq!(
            "127.0.0.1".parse::<Address>().unwrap(),
            Address {
                name: AddressName::Address(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
                port: 3306,
            }
        );
        assert_eq!(
            "127.0.0.1:6603".parse::<Address>().unwrap(),
            Address {
                name: AddressName::Address(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
                port: 6603,
            }
        );
        assert_eq!(
            "[::2]".parse::<Address>().unwrap(),
            Address {
                name: AddressName::Address(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 2))),
                port: 3306,
            }
        );
        assert_eq!(
            "[::4]:3307".parse::<Address>().unwrap(),
            Address {
                name: AddressName::Address(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 4))),
                port: 3307,
            }
        );
    }

    #[test]
    fn test_parse() {
        let parsed: GoDatabaseDsn = "foo:bar@tcp(127.0.0.1:33606)/foodb?ignored=true"
            .parse()
            .expect("should parse");
        assert_eq!(
            parsed.address,
            Address {
                name: AddressName::Address("127.0.0.1".parse().unwrap()),
                port: 33606
            }
        );
        assert_eq!(parsed.username.as_deref(), Some("foo"));
        assert_eq!(parsed.password.as_deref(), Some("bar"));
        assert_eq!(parsed.database, "foodb".to_string());
        for s in &[
            "foo:bar@tcp([::1])/foo",
            "foo:bar@tcp([::1]:3300)/foo",
            "foo@tcp([::1])/foo",
            "tcp(127.0.0.1)/baz",
            "usps:sekret@tcp(dblb.local.easypo.net:36060)/usps",
        ] {
            s.parse::<GoDatabaseDsn>()
                .context(format!("attempting to parse {}", s))
                .expect("should parse");
        }
    }
}
