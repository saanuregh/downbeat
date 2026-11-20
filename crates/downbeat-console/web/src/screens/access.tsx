// Grants: who may do what on which scheduler; and the people who have signed in.
import { useId, useState } from "react";
import { ActionButton, ActionDialog } from "../components/action";
import {
  Empty,
  ErrorNote,
  Loading,
  PageHeader,
  Section,
  Time,
} from "../components/ui";
import { useLoad } from "../load";
import { allows, useSession } from "../session";
import type { Grant, Person, Scope } from "../types";

const scopeWords: Record<Scope, string> = {
  read: "see",
  operate: "see and run",
  administer: "see, run, pause, override, backfill and grant access to",
};

function whoOf(grant: {
  to_person: string | null;
  to_group: string | null;
}): string {
  return (
    grant.to_person ?? (grant.to_group ? `group ${grant.to_group}` : "nobody")
  );
}

export function AccessScreen() {
  const { me } = useSession();
  const grants = useLoad<Grant[]>("/api/grants");
  const people = useLoad<Person[]>("/api/people");
  const administered = me.schedulers.filter((item) =>
    allows(item.scope, "administer"),
  );
  const labelOf = (name: string) =>
    me.schedulers.find((item) => item.name === name)?.label ?? name;

  return (
    <>
      <PageHeader
        title="Access"
        subtitle="Who may do what. A grant names a person or a group, one scheduler, a scope, and optionally some pipelines."
        actions={
          administered.length > 0 && (
            <AddGrantButton
              schedulers={administered.map((item) => ({
                name: item.name,
                label: item.label,
              }))}
              people={people.data ?? []}
              onDone={grants.reload}
            />
          )
        }
      />
      <Section title="Grants">
        {grants.error && (
          <ErrorNote error={grants.error} onRetry={grants.reload} />
        )}
        {!grants.data && !grants.error && <Loading />}
        {grants.data && grants.data.length === 0 && (
          <Empty>No grants yet.</Empty>
        )}
        {grants.data && grants.data.length > 0 && (
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th scope="col">Who</th>
                  <th scope="col">May</th>
                  <th scope="col">Scheduler</th>
                  <th scope="col">Pipelines</th>
                  <th scope="col">Granted</th>
                  <th scope="col">
                    <span className="visually-hidden">Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {grants.data.map((grant) => (
                  <tr key={grant.grant_id}>
                    <td>{whoOf(grant)}</td>
                    <td>{grant.scope}</td>
                    <td>{labelOf(grant.scheduler)}</td>
                    <td>
                      {grant.pipelines?.length
                        ? grant.pipelines.join(", ")
                        : "All"}
                    </td>
                    <td>
                      by {grant.created_by}, <Time at={grant.created_at} />
                    </td>
                    <td>
                      {administered.some(
                        (item) => item.name === grant.scheduler,
                      ) && (
                        <ActionButton
                          label="Remove"
                          small
                          spec={{
                            title: `Remove a grant from ${whoOf(grant)}`,
                            confirm: "Remove grant",
                            method: "DELETE",
                            path: `/api/grants/${encodeURIComponent(String(grant.grant_id))}`,
                            danger: true,
                            localPreview: [
                              `${whoOf(grant)} would no longer be able to ${scopeWords[grant.scope]} ${
                                grant.pipelines?.length
                                  ? `pipelines ${grant.pipelines.join(", ")} on `
                                  : ""
                              }${labelOf(grant.scheduler)} through this grant; other grants they hold still apply.`,
                            ],
                          }}
                          onDone={grants.reload}
                        />
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
      <Section title="People">
        {people.error && (
          <ErrorNote error={people.error} onRetry={people.reload} />
        )}
        {!people.data && !people.error && <Loading />}
        {people.data && people.data.length === 0 && (
          <Empty>Nobody has signed in yet.</Empty>
        )}
        {people.data && people.data.length > 0 && (
          <div className="table-scroll">
            <table className="table">
              <thead>
                <tr>
                  <th scope="col">Name</th>
                  <th scope="col">Email</th>
                  <th scope="col">Groups</th>
                  <th scope="col">Last seen</th>
                </tr>
              </thead>
              <tbody>
                {people.data.map((person) => (
                  <tr key={person.email}>
                    <td>{person.name}</td>
                    <td>{person.email}</td>
                    <td>
                      {person.groups.length ? (
                        person.groups.join(", ")
                      ) : (
                        <span className="muted">None</span>
                      )}
                    </td>
                    <td className="nowrap">
                      <Time at={person.last_seen_at} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </>
  );
}

function AddGrantButton(props: {
  schedulers: { name: string; label: string }[];
  people: Person[];
  onDone: () => void;
}) {
  const [open, setOpen] = useState(false);
  const [toKind, setToKind] = useState<"person" | "group">("person");
  const [who, setWho] = useState("");
  const [scheduler, setScheduler] = useState(props.schedulers[0]?.name ?? "");
  const [scope, setScope] = useState<Scope>("read");
  const [pipelinesText, setPipelinesText] = useState("");
  const ids = {
    kind: useId(),
    who: useId(),
    people: useId(),
    scheduler: useId(),
    scope: useId(),
    pipelines: useId(),
  };
  const pipelines = pipelinesText
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
  const label =
    props.schedulers.find((item) => item.name === scheduler)?.label ??
    scheduler;
  const target = who.trim();
  const invalid =
    target === ""
      ? `Write the ${toKind === "person" ? "person's email" : "group's name"}.`
      : null;
  const body = {
    ...(toKind === "person" ? { to_person: target } : { to_group: target }),
    scheduler,
    scope,
    ...(pipelines.length > 0 ? { pipelines } : {}),
  };
  const whoText = toKind === "person" ? target : `everyone in group ${target}`;

  return (
    <>
      <button
        type="button"
        className="button button-primary"
        onClick={() => setOpen(true)}
      >
        Add grant
      </button>
      {open && (
        <ActionDialog
          spec={{
            title: "Add a grant",
            confirm: "Add grant",
            path: "/api/grants",
            body,
            invalid,
            localPreview: [
              `${whoText} would be able to ${scopeWords[scope]} ${
                pipelines.length > 0
                  ? `pipelines ${pipelines.join(", ")} on `
                  : "everything on "
              }${label}.`,
            ],
          }}
          onClose={() => setOpen(false)}
          onDone={props.onDone}
        >
          <div className="form-row">
            <div>
              <label htmlFor={ids.kind}>To</label>
              <select
                id={ids.kind}
                value={toKind}
                onChange={(event) =>
                  setToKind(event.target.value === "group" ? "group" : "person")
                }
              >
                <option value="person">A person</option>
                <option value="group">A group</option>
              </select>
            </div>
            <div>
              <label htmlFor={ids.who}>
                {toKind === "person" ? "Email" : "Group"}
              </label>
              <input
                id={ids.who}
                value={who}
                list={toKind === "person" ? ids.people : undefined}
                onChange={(event) => setWho(event.target.value)}
              />
              <datalist id={ids.people}>
                {props.people.map((person) => (
                  <option key={person.email} value={person.email}>
                    {person.name}
                  </option>
                ))}
              </datalist>
            </div>
          </div>
          <div className="form-row">
            <div>
              <label htmlFor={ids.scheduler}>Scheduler</label>
              <select
                id={ids.scheduler}
                value={scheduler}
                onChange={(event) => setScheduler(event.target.value)}
              >
                {props.schedulers.map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.label}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label htmlFor={ids.scope}>Scope</label>
              <select
                id={ids.scope}
                value={scope}
                onChange={(event) => setScope(event.target.value as Scope)}
              >
                <option value="read">read: see</option>
                <option value="operate">operate: see and run</option>
                <option value="administer">administer: everything</option>
              </select>
            </div>
          </div>
          <label htmlFor={ids.pipelines}>
            Pipelines (comma-separated; empty for all)
          </label>
          <input
            id={ids.pipelines}
            value={pipelinesText}
            onChange={(event) => setPipelinesText(event.target.value)}
          />
        </ActionDialog>
      )}
    </>
  );
}
