ALTER TABLE lim.operation_state_history ADD COLUMN limit_data_id bigint;

CREATE INDEX operation_history_idx ON lim.operation_state_history USING btree (limit_data_id, state, created_at, operation_id);

