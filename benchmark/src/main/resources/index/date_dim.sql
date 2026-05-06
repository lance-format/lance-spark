ALTER TABLE date_dim CREATE INDEX idx_d_year USING btree (d_year);
ALTER TABLE date_dim CREATE INDEX idx_d_month_seq USING btree (d_month_seq);
ALTER TABLE date_dim CREATE INDEX idx_d_date USING btree (d_date);
ALTER TABLE date_dim CREATE INDEX idx_d_qoy USING btree (d_qoy);
ALTER TABLE date_dim CREATE INDEX idx_d_moy USING btree (d_moy);
