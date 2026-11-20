//! Jobs for tests of schedules: registration, firing, overrides and backfills. Schedules here
//! run only in the `rules` environment, so they fire in no other test.

use downbeat::prelude::*;

pub(crate) fn scheduling_rules_jobs(pipeline: &mut Pipeline) {
    let _ = pipeline;
}
