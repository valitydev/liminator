CREATE INDEX IF NOT EXISTS operation_state_history_operation_created_at_idx
    ON lim.operation_state_history USING btree (operation_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS operation_state_history_operation_state_limit_idx
    ON lim.operation_state_history USING btree (operation_id, state, limit_data_id);
