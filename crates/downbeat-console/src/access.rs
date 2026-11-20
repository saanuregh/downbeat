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

/// What this person may do on a scheduler with the scope `needed`: that scope, limited to the
/// pipelines of every grant that reaches it (or none, when one of them covers every pipeline).
pub(crate) fn access(
    config: &Config,
    person: &Person,
    grants: &[Grant],
    scheduler: &str,
    needed: Scope,
) -> Option<Access> {
    if first_administrator(config, person) {
        return Some(Access { scope: needed, pipelines: None });
    }
    let reaching: Vec<&Option<Vec<String>>> = grants
        .iter()
        .filter(|grant| grant.scheduler == scheduler)
        .filter(|grant| grant.scope.parse::<Scope>().is_ok_and(|scope| scope >= needed))
        .map(|grant| &grant.pipelines)
        .collect();
    if reaching.is_empty() {
        return None;
    }
    let pipelines = if reaching.iter().any(|pipelines| pipelines.is_none()) {
        None
    } else {
        let mut all: Vec<String> =
            reaching.iter().filter_map(|pipelines| pipelines.as_ref()).flatten().cloned().collect();
        all.sort();
        all.dedup();
        Some(all)
    };
    Some(Access { scope: needed, pipelines })
}

/// Whether this person administers every pipeline of a scheduler: only they may change who may
/// do what on it, so nobody can grant more than they hold.
pub(crate) fn administers_fully(config: &Config, person: &Person, grants: &[Grant], scheduler: &str) -> bool {
    access(config, person, grants, scheduler, Scope::Administer).is_some_and(|access| access.pipelines.is_none())
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
    fn a_token_carries_the_scope_needed_and_the_pipelines_that_reach_it() {
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

    #[test]
    fn operating_everywhere_is_not_narrowed_by_a_pipeline_administration() {
        let grants = [grant("operate", None), grant("administer", Some(&["eod"]))];
        let asha = person("asha@example.com", &[]);
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Operate),
            Some(Access { scope: Scope::Operate, pipelines: None })
        );
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Administer),
            Some(Access { scope: Scope::Administer, pipelines: Some(vec!["eod".into()]) })
        );
        assert!(
            !administers_fully(&config(), &asha, &grants, "prod"),
            "administering one pipeline is not administering the scheduler"
        );
    }

    fn to_group(grant: Grant, group: &str) -> Grant {
        Grant { to_person: None, to_group: Some(group.into()), ..grant }
    }

    #[test]
    fn several_pipeline_grants_give_the_union_of_their_pipelines() {
        let grants = [
            grant("operate", Some(&["eod", "billing"])),
            to_group(grant("administer", Some(&["billing", "reports"])), "finance"),
            grant("read", None),
            Grant { scheduler: "uat".into(), ..grant("operate", None) },
        ];
        let asha = person("asha@example.com", &["finance"]);
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Operate),
            Some(Access {
                scope: Scope::Operate,
                pipelines: Some(vec!["billing".into(), "eod".into(), "reports".into()])
            }),
            "a lower grant with no pipelines and a grant on another scheduler do not count"
        );
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Administer),
            Some(Access { scope: Scope::Administer, pipelines: Some(vec!["billing".into(), "reports".into()]) })
        );
    }

    #[test]
    fn one_reaching_grant_with_no_pipelines_makes_the_access_unlimited() {
        let grants = [
            grant("operate", Some(&["eod"])),
            to_group(grant("operate", None), "ops"),
            grant("administer", Some(&["billing"])),
        ];
        let asha = person("asha@example.com", &["ops"]);
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Operate),
            Some(Access { scope: Scope::Operate, pipelines: None })
        );
        assert_eq!(
            access(&config(), &asha, &grants, "prod", Scope::Read),
            Some(Access { scope: Scope::Read, pipelines: None })
        );
    }
}
