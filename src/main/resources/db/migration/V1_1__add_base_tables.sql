CREATE SCHEMA IF NOT EXISTS lim;


CREATE TABLE lim.limit_data
(
    id                              bigserial                                                        NOT NULL,
    name                            character varying                                                NOT NULL,
    created_at                      date                                                             NOT NULL,
    wtime                           timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    CONSTRAINT limit_data_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX limit_data_unq_idx ON lim.limit_data USING btree (name);



CREATE TABLE lim.limit_context
(
    id                              bigserial                                                        NOT NULL,
    limit_id                        bigint                                                           NOT NULL,
    context                         character varying,
    wtime                           timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    CONSTRAINT limit_context_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX limit_context_unq_idx ON lim.limit_context USING btree (limit_id);



CREATE TYPE lim.operation_state AS ENUM ('HOLD', 'COMMIT', 'ROLLBACK');

CREATE TABLE lim.operation
(
    id                              bigserial                                                        NOT NULL,
    limit_id                        bigint                                                           NOT NULL,
    operation_id                    character varying                                                NOT NULL,
    state                           lim.operation_state                                              NOT NULL,
    amount                          bigint                                                           NOT NULL,
    created_at                      timestamp without time zone DEFAULT timezone('utc'::text, now()) NOT NULL,
    CONSTRAINT operation_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX operation_unq_idx ON lim.operation USING btree (limit_id, operation_id);
CREATE INDEX operation_idx ON lim.operation USING btree (limit_id, state, created_at, operation_id);
