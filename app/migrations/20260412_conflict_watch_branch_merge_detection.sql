ALTER TABLE cw_branches
ADD COLUMN IF NOT EXISTS merged_detected_by VARCHAR(50);
