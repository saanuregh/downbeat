//! Who may do what: grants, to a person or a provider group, of a scope on a scheduler, optionally
//! limited to some pipelines. A person's scope is the highest any grant gives them; the first
//! administrators from the configuration administer every scheduler, so the console is never
//! locked.

use downbeat_api::Scope;

use crate::config::Config;
use crate::store::{Grant, Person};

/// What a person may do on one scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Access {
    pub(crate) scope: Scope,
    /// `None`: every pipeline.
    pub(crate) pipelines: Option<Vec<String>>,
}

/// Whether this person is one of the configured first administrators.
pub(crate) fn first_administrator(config: &Config, person: &Person) -> bool {
    config.local || config.first_administrators.iter().any(|email| email.eq_ignore_ascii_case(&person.email))
}

/// The most this person may do on a scheduler, for requests that need at least `needed`.
///
/// Among the person's grants on the scheduler that reach `needed`, the token takes the highest
/// scope, and is limited to the pipelines of the grants at that scope (unless one of them covers
/// every pipeline).
pub(crate) fn access(
    config: &Config,
    person: &Person,
    grants: &[Grant],
    scheduler: &str,
    needed: Scope,
) -> Option<Access> {
    if first_administrator(config, person) {
        return Some(Access { scope: Scope::Administer, pipelines: None });
    }
    let applicable: Vec<(Scope, &Option<Vec<String>>)> = grants
        .iter()
        .filter(|grant| grant.scheduler == scheduler)
        .filter_map(|grant| grant.scope.parse::<Scope>().ok().map(|scope| (scope, &grant.pipelines)))
        .filter(|(scope, _)| *scope >= needed)
        .collect();
    let top = applicable.iter().map(|(scope, _)| *scope).max()?;
    // For reading, every grant counts; for acting, only those at the scope the token will carry.
    let counted: Vec<&Option<Vec<String>>> = applicable
        .iter()
        .filter(|(scope, _)| needed == Scope::Read || *scope == top)
        .map(|(_, pipelines)| *pipelines)
        .collect();
    let scope = if needed == Scope::Read { Scope::Read } else { top };
    let pipelines = if counted.iter().any(|pipelines| pipelines.is_none()) {
        None
    } else {
        let mut all: Vec<String> =
            counted.iter().filter_map(|pipelines| pipelines.as_ref()).flatten().cloned().collect();
        all.sort();
        all.dedup();
        Some(all)
    };
    Some(Access { scope, pipelines })
}

/// The highest scope a person has on a scheduler, for the screens to show only what they may do.
pub(crate) fn highest(config: &Config, person: &Person, grants: &[Grant], scheduler: &str) -> Option<Scope> {
    access(config, person, grants, scheduler, Scope::Read).map(|_| {
        [Scope::Administer, Scope::Operate, Scope::Read]
            .into_iter()
            .find(|scope| access(config, person, grants, scheduler, *scope).is_some())
            .unwrap_or(Scope::Read)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        let mut config = Config::local(None, "http://127.0.0.1:7400").unwrap();
        config.local = false;
        config.first_administrators = vec!["boss@example.com".into()];
        config
    }

    fn person(email: &str, groups: &[&str]) -> Person {
        Person {
            person_id: 1,
            email: email.into(),
            name: email.into(),
            groups: groups.iter().map(|g| (*g).to_owned()).collect(),
        }
    }

    fn grant(scope: &str, pipelines: Option<&[&str]>) -> Grant {
        Grant {
            grant_id: 1,
            to_person: Some("asha@example.com".into()),
            to_group: None,
            scheduler: "prod".into(),
            scope: scope.into(),
            pipelines: pipelines.map(|list| list.iter().map(|p| (*p).to_owned()).collect()),
            created_by: "boss".into(),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn no_grant_means_nothing() {
        assert_eq!(access(&config(), &person("asha@example.com", &[]), &[], "prod", Scope::Read), None);
    }

    #[test]
    fn first_administrators_administer_everything() {
        let access = access(&config(), &person("boss@example.com", &[]), &[], "prod", Scope::Administer);
        assert_eq!(access, Some(Access { scope: Scope::Administer, pipelines: None }));
    }

    #[test]
    fn acting_uses_the_highest_grant_and_its_pipelines() {
        let grants = [grant("read", None), grant("operate", Some(&["eod"]))];
        let asha = person("asha@example.com", &[]);
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Read),
            Some(Access { scope: Scope::Read, pipelines: None })
        );
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Operate),
            Some(Access { scope: Scope::Operate, pipelines: Some(vec!["eod".into()]) })
        );
        assert_eq!(access(&config(), &asha, &grants, "prod", Scope::Administer), None);
        assert_eq!(highest(&config(), &asha, &grants, "prod"), Some(Scope::Operate));
    }
}
