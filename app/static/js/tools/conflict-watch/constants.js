export const PROVIDERS = [
  { value: "github", label: "GitHub" },
  { value: "backlog", label: "Backlog" },
];

export const BRANCH_STATUS_LABELS = {
  active: "active",
  quiet: "quiet",
  stale: "stale",
  branch_excluded: "branch_excluded",
};

export const CONFLICT_STATUS_LABELS = {
  warning: "warning",
  notice: "notice",
  resolved: "resolved",
  conflict_ignored: "conflict_ignored",
};

export const CHANGE_TYPE_LABELS = {
  added: "added",
  modified: "modified",
  removed: "removed",
  renamed: "renamed",
  copied: "copied",
};

export const CHANGE_TYPES = Object.keys(CHANGE_TYPE_LABELS);

export const DEFAULT_SETTINGS = {
  staleDays: 15,
  longUnresolvedDays: 7,
  rawPayloadRetentionDays: 14,
  processingTraceEnabled: true,
  forcePushNoteEnabled: true,
  suppressNoticeNotifications: false,
  notificationDestination: "#conflict-watch",
  slackWebhookUrl: "",
  githubWebhookEndpoint: "/tools/conflict-watch/webhooks/github",
  backlogWebhookEndpoint: "/tools/conflict-watch/webhooks/backlog",
  githubWebhookSecret: "ghs_demo_hotdock",
  backlogWebhookSecret: "backlog_demo_secret",
};

export const DEFAULT_IGNORE_PATTERNS = [
  "package-lock.json",
  "yarn.lock",
  "pnpm-lock.yaml",
  "composer.lock",
  "dist/**",
  "build/**",
  "node_modules/**",
  "vendor/**",
  "tmp/**",
  "log/**",
  "*.png",
  "*.jpg",
  "*.jpeg",
  "*.gif",
  "*.webp",
  "*.pdf",
  "*.zip",
];

export const WEBHOOK_FORM_DEFAULTS = {
  provider: "github",
  deliveryId: "",
  branchName: "feature/conflict-dashboard",
  pusher: "yoshiaki0223",
  signatureStatus: "valid",
  deletedState: "false",
  simulateFailure: false,
  isForced: false,
  added: "",
  modified: "app/conflicts/service.py\napp/conflicts/dashboard.py",
  removed: "",
  renamed: "",
};

export const QUICK_WEBHOOK_PRESETS = [
  {
    id: "preset-ui",
    label: "UI 側で同一ファイルに追記",
    description: "別 branch が service.py を同時に変更するケース。",
    draft: {
      provider: "github",
      deliveryId: "",
      branchName: "feature/webhook-ui",
      pusher: "front-dev",
      signatureStatus: "valid",
      deletedState: "false",
      simulateFailure: false,
      isForced: false,
      added: "",
      modified: "app/conflicts/service.py\napp/conflicts/table.py",
      removed: "",
      renamed: "",
    },
  },
  {
    id: "preset-force",
    label: "force push を受信",
    description: "観測状態が揺らぐケースを再現する。",
    draft: {
      provider: "github",
      deliveryId: "",
      branchName: "feature/slack-notifier",
      pusher: "ops-bot",
      signatureStatus: "valid",
      deletedState: "false",
      simulateFailure: false,
      isForced: true,
      added: "",
      modified: "app/notifications/slack.py\napp/conflicts/service.py",
      removed: "",
      renamed: "",
    },
  },
  {
    id: "preset-remove",
    label: "削除と変更の衝突を追加",
    description: "removed も衝突対象に残す仕様を確認する。",
    draft: {
      provider: "backlog",
      deliveryId: "",
      branchName: "feature/cleanup-run",
      pusher: "cleanup-bot",
      signatureStatus: "valid",
      deletedState: "false",
      simulateFailure: false,
      isForced: false,
      added: "",
      modified: "",
      removed: "app/export/csv.py",
      renamed: "",
    },
  },
  {
    id: "preset-invalid-signature",
    label: "署名検証失敗",
    description: "security log にだけ残し、branch 状態を更新しない。",
    draft: {
      provider: "github",
      deliveryId: "",
      branchName: "feature/security-probe",
      pusher: "",
      signatureStatus: "invalid",
      deletedState: "false",
      simulateFailure: false,
      isForced: false,
      added: "",
      modified: "app/conflicts/service.py",
      removed: "",
      renamed: "",
    },
  },
  {
    id: "preset-processing-error",
    label: "非同期処理失敗",
    description: "queue 登録後に failed になり、reprocess を試せる。",
    draft: {
      provider: "github",
      deliveryId: "",
      branchName: "feature/queue-failure",
      pusher: "queue-worker",
      signatureStatus: "valid",
      deletedState: "false",
      simulateFailure: true,
      isForced: false,
      added: "app/conflicts/worker.py",
      modified: "app/conflicts/service.py",
      removed: "",
      renamed: "",
    },
  },
];

export const CONFLICT_ACTIONS = [
  { value: "warning", label: "warning に戻す" },
  { value: "notice", label: "notice にする" },
  { value: "resolved", label: "resolved にする" },
  { value: "conflict_ignored", label: "conflict_ignored にする" },
];
