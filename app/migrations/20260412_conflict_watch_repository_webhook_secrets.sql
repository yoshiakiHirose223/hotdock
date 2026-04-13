ALTER TABLE cw_repositories
ADD COLUMN IF NOT EXISTS github_webhook_secret VARCHAR(255);

ALTER TABLE cw_repositories
ADD COLUMN IF NOT EXISTS backlog_webhook_secret VARCHAR(255);
