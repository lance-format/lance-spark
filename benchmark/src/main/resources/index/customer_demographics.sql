ALTER TABLE customer_demographics CREATE INDEX idx_cd_gender USING btree (cd_gender);
ALTER TABLE customer_demographics CREATE INDEX idx_cd_marital_status USING btree (cd_marital_status);
ALTER TABLE customer_demographics CREATE INDEX idx_cd_education_status USING btree (cd_education_status);
