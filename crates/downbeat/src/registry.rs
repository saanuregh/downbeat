//! The registry: a definition with its pipelines run and everything checked, as every engine
//! process uses it. Building it is what `check` does; a role refuses to start if it finds problems.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use chrono_tz::Tz;

use crate::definition::{Callback, Definition, Redact, Settings};
use crate::dependencies::Dependencies;
use crate::events::{EventDelivery, FinishedRun};
use crate::job::JobSpec;
use crate::pipeline::{Condition, Entry, LinkSpec, NeedSpec, NeedTest, Pipeline};
use crate::schedule::{CompiledSchedule, ScheduleForm, ScheduleSpec, Times};
use crate::schedule::times::parse_time;

/// A job with everything the pipelines say about it.
pub(crate) struct RegisteredJob {
    pub(crate) spec: JobSpec,
    pub(crate) pipeline: String,
    pub(crate) link: Option<LinkSpec>,
    pub(crate) if_changed: bool,
    pub(crate) needs: Vec<NeedSpec>,
    pub(crate) conditions: Vec<Condition>,
    /// The names of its schedules in this environment.
    pub(crate) schedules: Vec<String>,
}

/// Everything an engine process needs from the definition, checked.
pub(crate) struct Registry {
    pub(crate) zone: Tz,
    pub(crate) environment: Option<String>,
    pub(crate) jobs: BTreeMap<&'static str, RegisteredJob>,
    /// Schedules active in this environment, by name.
    pub(crate) schedules: BTreeMap<String, CompiledSchedule>,
    /// For each job, the jobs linked after it.
    pub(crate) children: HashMap<&'static str, Vec<&'static str>>,
    /// Each pipeline's jobs, in the order its function names them.
    pub(crate) pipelines: BTreeMap<String, Vec<&'static str>>,
    pub(crate) dependencies: Arc<Dependencies>,
    pub(crate) on_finish: Option<Callback<FinishedRun>>,
    pub(crate) on_event: Option<Callback<EventDelivery>>,
    pub(crate) log_link: Option<String>,
    pub(crate) redact: Option<Redact>,
    pub(crate) settings: Settings,
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("zone", &self.zone)
            .field("environment", &self.environment)
            .field("jobs", &self.jobs.keys().collect::<Vec<_>>())
            .field("schedules", &self.schedules.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Something `check` rejects, with the job or schedule it is about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Problem {
    pub(crate) subject: String,
    pub(crate) message: String,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.subject, self.message)
    }
}

impl Registry {
    /// Runs the definition's pipelines and checks the result. `environment` selects which
    /// schedules exist; `None` keeps every schedule (for `check`).
    pub(crate) fn build(definition: Definition, environment: Option<&str>) -> Result<Registry, Vec<Problem>> {
        let mut problems = Problems::default();
        let zone = parse_zone(&definition.zone, "definition", &mut problems);
        if let Some(environment) = environment
            && !definition.environments.iter().any(|known| known == environment)
            && !definition.environments.is_empty()
        {
            problems.add("definition", format!("environment '{environment}' is not one the definition lists"));
        }

        let mut jobs: BTreeMap<&'static str, RegisteredJob> = BTreeMap::new();
        let mut pipelines = BTreeMap::new();
        let mut schedules = BTreeMap::new();
        for (name, describe) in &definition.pipelines {
            let mut pipeline = Pipeline::new(name);
            describe(&mut pipeline);
            if pipelines.contains_key(name) {
                problems.add(name, "two pipelines have this name");
            }
            let mut names = Vec::new();
            for entry in pipeline.entries {
                let job_name = entry.job.name;
                names.push(job_name);
                if let Some(existing) = jobs.get(job_name) {
                    problems.add(job_name, format!("named in pipeline '{}' and again in '{name}'; a job belongs to one pipeline", existing.pipeline));
                    continue;
                }
                let registered = register(entry, name, zone, &definition, environment, &mut schedules, &mut problems);
                jobs.insert(job_name, registered);
            }
            pipelines.insert(name.clone(), names);
        }

        let children = check_graph(&jobs, &mut problems);
        check_dependencies(&jobs, &schedules, &definition.dependencies, &mut problems);
        if let Some(problem) = definition.settings.shutdown_problem() {
            problems.add("settings", problem);
        }

        if !problems.0.is_empty() {
            let mut list: Vec<Problem> = problems.0.into_iter().collect();
            list.sort();
            return Err(list);
        }
        Ok(Registry {
            zone,
            environment: environment.map(str::to_owned),
            jobs,
            schedules,
            children,
            pipelines,
            dependencies: Arc::new(definition.dependencies),
            on_finish: definition.on_finish,
            on_event: definition.on_event,
            log_link: definition.log_link,
            redact: definition.redact,
            settings: definition.settings,
        })
    }

    /// Shutdown keeps this long to release runs back to the queue.
    pub(crate) fn release_time() -> std::time::Duration {
        Settings::RELEASE_TIME
    }

    pub(crate) fn job(&self, name: &str) -> Option<&RegisteredJob> {
        self.jobs.get(name)
    }

    /// The names of every job, for the claim's "jobs this worker knows".
    pub(crate) fn job_names(&self) -> Vec<String> {
        self.jobs.keys().map(|name| (*name).to_owned()).collect()
    }

    /// The link to a run's logs.
    pub(crate) fn log_link_for(&self, run_id: uuid::Uuid) -> Option<String> {
        self.log_link.as_ref().map(|template| template.replace("{run_id}", &run_id.to_string()))
    }
}

#[derive(Default)]
struct Problems(BTreeSet<Problem>);

impl Problems {
    fn add(&mut self, subject: impl Into<String>, message: impl Into<String>) {
        self.0.insert(Problem { subject: subject.into(), message: message.into() });
    }
}

fn parse_zone(zone: &str, subject: &str, problems: &mut Problems) -> Tz {
    if zone.is_empty() {
        problems.add(subject, "no zone is set; call .zone(\"Area/City\") (there is no implicit UTC)");
        return chrono_tz::UTC;
    }
    zone.parse().unwrap_or_else(|_| {
        problems.add(subject, format!("'{zone}' is not a time zone; use an IANA name such as Asia/Kolkata"));
        chrono_tz::UTC
    })
}

fn register(
    entry: Entry,
    pipeline: &str,
    zone: Tz,
    definition: &Definition,
    environment: Option<&str>,
    schedules: &mut BTreeMap<String, CompiledSchedule>,
    problems: &mut Problems,
) -> RegisteredJob {
    let job_name = entry.job.name;
    if entry.if_changed && !entry.job.takes_input {
        problems.add(job_name, ".if_changed() needs a job that takes an input; with none, every run is the same");
    }
    let mut names = Vec::new();
    for (index, schedule) in entry.schedules.into_iter().enumerate() {
        let name = schedule.name.clone().unwrap_or_else(|| job_name.to_owned());
        if index > 0 && schedule.name.is_none() {
            problems.add(job_name, "a job's second schedule needs a name: add .named(\"…\")");
        }
        if let Some(environments) = &schedule.environments {
            for unknown in environments.iter().filter(|e| !definition.environments.contains(e)) {
                problems.add(&name, format!("environment '{unknown}' is not one the definition lists"));
            }
            if environment.is_some_and(|current| !environments.iter().any(|e| e == current)) {
                continue;
            }
        }
        if let Some(compiled) = compile_schedule(&name, job_name, schedule, zone, problems) {
            if schedules.insert(name.clone(), compiled).is_some() {
                problems.add(&name, "two schedules have this name");
            }
            names.push(name);
        }
    }
    for need in &entry.needs {
        if matches!(need.test, NeedTest::DoneFor(_)) && need.job == job_name {
            problems.add(job_name, "a job cannot need itself");
        }
    }
    RegisteredJob {
        spec: entry.job,
        pipeline: pipeline.to_owned(),
        link: entry.link,
        if_changed: entry.if_changed,
        needs: entry.needs,
        conditions: entry.conditions,
        schedules: names,
    }
}

fn compile_schedule(name: &str, job: &'static str, spec: ScheduleSpec, zone: Tz, problems: &mut Problems) -> Option<CompiledSchedule> {
    let zone = match &spec.zone {
        Some(own) => parse_zone(own, name, problems),
        None => zone,
    };
    let window = match &spec.window {
        Some((start, end)) => match (parse_time(start), parse_time(end)) {
            (Ok(start), Ok(end)) => Some((start, end)),
            (Err(error), _) | (_, Err(error)) => {
                problems.add(name, error);
                return None;
            }
        },
        None => None,
    };
    let times = match spec.form {
        ScheduleForm::DailyAt(texts) => {
            if window.is_some() {
                problems.add(name, ".between(…) is for every(…); daily_at already lists its times");
            }
            if texts.is_empty() {
                problems.add(name, "daily_at needs at least one time");
            }
            let mut times = Vec::new();
            for text in texts {
                match parse_time(&text) {
                    Ok(time) => times.push(time),
                    Err(error) => problems.add(name, error),
                }
            }
            times.sort();
            Times::DailyAt(times)
        }
        ScheduleForm::Every(interval) => {
            if interval.as_secs() < 1 {
                problems.add(name, "the shortest interval is one second");
            }
            Times::Every { interval, window }
        }
    };
    if let Some(days) = &spec.month_days
        && days.iter().any(|day| !(1..=31).contains(day))
    {
        problems.add(name, "days of the month run from 1 to 31");
    }
    Some(CompiledSchedule {
        name: name.to_owned(),
        job,
        times,
        zone,
        weekdays: spec.weekdays,
        month_days: spec.month_days,
        calendar: spec.calendar,
        input: spec.input,
        expire_after: spec.expire_after,
    })
}

/// Checks links and needs, and returns each job's children.
fn check_graph(jobs: &BTreeMap<&'static str, RegisteredJob>, problems: &mut Problems) -> HashMap<&'static str, Vec<&'static str>> {
    let mut children: HashMap<&'static str, Vec<&'static str>> = HashMap::new();
    for (name, job) in jobs {
        if let Some(link) = &job.link {
            for parent in &link.parents {
                if jobs.contains_key(parent) {
                    children.entry(parent).or_default().push(name);
                } else {
                    problems.add(*name, format!("its parent '{parent}' is named in no pipeline; add pipeline.job({parent})"));
                }
            }
            let distinct: BTreeSet<_> = link.parents.iter().collect();
            if distinct.len() != link.parents.len() {
                problems.add(*name, "a parent is listed twice");
            }
        }
        for need in &job.needs {
            if !jobs.contains_key(need.job) {
                problems.add(*name, format!("it needs '{}', which is named in no pipeline", need.job));
            } else if matches!(need.test, NeedTest::DoneFor(_)) && !jobs[need.job].spec.takes_input {
                problems.add(*name, format!("'{}' takes no input; use done_within(…) instead of done_for(…)", need.job));
            }
        }
    }
    for list in children.values_mut() {
        list.sort_unstable();
    }

    // Cycles: walk from every job; a job met again on the current path closes a cycle.
    let parents_of = |name: &str| -> Vec<&'static str> {
        jobs.get(name).and_then(|job| job.link.as_ref()).map(|link| link.parents.clone()).unwrap_or_default()
    };
    let mut cyclic = BTreeSet::new();
    for start in jobs.keys() {
        let mut stack = vec![(*start, vec![*start])];
        while let Some((at, path)) = stack.pop() {
            for parent in parents_of(at) {
                if parent == *start {
                    cyclic.insert(*start);
                } else if !path.contains(&parent) {
                    let mut next = path.clone();
                    next.push(parent);
                    stack.push((parent, next));
                }
            }
        }
    }
    for name in &cyclic {
        problems.add(*name, "its links form a cycle");
    }

    // Joins: every parent must be downstream of one common job, so they can end in one chain.
    if cyclic.is_empty() {
        for (name, job) in jobs {
            let Some(link) = &job.link else { continue };
            if link.parents.len() < 2 {
                continue;
            }
            let mut common: Option<BTreeSet<&str>> = None;
            for parent in &link.parents {
                let ancestors = ancestors_and_self(parent, &parents_of);
                common = Some(match common {
                    None => ancestors,
                    Some(so_far) => so_far.intersection(&ancestors).copied().collect(),
                });
            }
            if common.is_some_and(|set| set.is_empty()) {
                problems.add(*name, "its parents can never end in the same chain: they share no first job. Give them a common first job (see the pipelines chapter)");
            }
        }
    }
    children
}

fn ancestors_and_self<'a>(name: &'a str, parents_of: &impl Fn(&str) -> Vec<&'static str>) -> BTreeSet<&'a str> {
    let mut seen = BTreeSet::from([name]);
    let mut stack = vec![name];
    while let Some(at) = stack.pop() {
        for parent in parents_of(at) {
            if seen.insert(parent) {
                stack.push(parent);
            }
        }
    }
    seen
}

fn check_dependencies(
    jobs: &BTreeMap<&'static str, RegisteredJob>,
    schedules: &BTreeMap<String, CompiledSchedule>,
    dependencies: &Dependencies,
    problems: &mut Problems,
) {
    for (name, job) in jobs {
        for dependency in &job.spec.dependencies {
            if !dependencies.provides(*dependency) {
                problems.add(*name, format!("it takes &{} but the definition does not provide one; add definition.provide(…)", dependency.type_name));
            }
        }
    }
    for (name, schedule) in schedules {
        if let Some(rule) = &schedule.calendar
            && !dependencies.provides(rule.dependency)
        {
            problems.add(name, format!("its day rule {} takes &{} but the definition does not provide one", rule.name, rule.dependency.type_name));
        }
    }
}
