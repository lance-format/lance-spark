ALTER TABLE household_demographics CREATE INDEX idx_hd_dep_count USING btree (hd_dep_count);
ALTER TABLE household_demographics CREATE INDEX idx_hd_buy_potential USING btree (hd_buy_potential);
