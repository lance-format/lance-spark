ALTER TABLE item CREATE INDEX idx_i_category USING btree (i_category);
ALTER TABLE item CREATE INDEX idx_i_class USING btree (i_class);
ALTER TABLE item CREATE INDEX idx_i_brand USING btree (i_brand);
ALTER TABLE item CREATE INDEX idx_i_current_price USING btree (i_current_price);
ALTER TABLE item CREATE INDEX idx_i_color USING btree (i_color);
ALTER TABLE item CREATE INDEX idx_i_manufact_id USING btree (i_manufact_id);
ALTER TABLE item CREATE INDEX idx_i_manager_id USING btree (i_manager_id);
