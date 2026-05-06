ALTER TABLE promotion CREATE INDEX idx_p_channel_tv USING btree (p_channel_tv);
ALTER TABLE promotion CREATE INDEX idx_p_channel_email USING btree (p_channel_email);
