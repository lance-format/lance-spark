ALTER TABLE customer_address CREATE INDEX idx_ca_state USING btree (ca_state);
ALTER TABLE customer_address CREATE INDEX idx_ca_gmt_offset USING btree (ca_gmt_offset);
