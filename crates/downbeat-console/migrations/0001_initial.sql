-- The console's own tables, in its own database (schema `console`). It never holds product data.

CREATE TABLE console.person (
    person_id    bigserial PRIMARY KEY,
    -- The identity provider's stable subject id.
    subject      text NOT NULL UNIQUE,
    email        text NOT NULL,
    name         text NOT NULL,
    -- The groups the provider reported at the last sign-in.
    groups       text[] NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE console.grant (
    grant_id   bigserial PRIMARY KEY,
    to_person  bigint REFERENCES console.person ON DELETE CASCADE,
    to_group   text,
    scheduler  text NOT NULL,
    scope      text NOT NULL CHECK (scope IN ('read', 'operate', 'administer')),
    -- Null: every pipeline.
    pipelines  text[],
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((to_person IS NULL) <> (to_group IS NULL))
);

CREATE TABLE console.session (
    -- Only a hash of the session id is stored.
    session_hash text PRIMARY KEY,
    person_id    bigint NOT NULL REFERENCES console.person ON DELETE CASCADE,
    csrf_token   text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);

-- A sign-in in progress: what the callback must check.
CREATE TABLE console.sign_in (
    state        text PRIMARY KEY,
    pkce_verifier text NOT NULL,
    nonce        text NOT NULL,
    return_to    text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE console.audit (
    audit_id bigserial PRIMARY KEY,
    at       timestamptz NOT NULL DEFAULT now(),
    actor    text NOT NULL,
    action   text NOT NULL,
    detail   jsonb NOT NULL DEFAULT '{}'
);

CREATE TABLE console.preference (
    person_id bigint PRIMARY KEY REFERENCES console.person ON DELETE CASCADE,
    data      jsonb NOT NULL DEFAULT '{}'
);
