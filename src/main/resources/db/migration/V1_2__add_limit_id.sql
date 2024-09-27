ALTER TABLE lim.limit_context RENAME COLUMN limit_id TO limit_data_id;
ALTER TABLE lim.limit_data ADD COLUMN limit_id character varying;
