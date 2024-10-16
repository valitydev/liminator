CREATE INDEX operation_history_idx ON lim.operation_state_history USING btree (limit_name, state, created_at, operation_id);

ALTER TABLE lim.operation_state_history ADD COLUMN limit_data_id bigint;
