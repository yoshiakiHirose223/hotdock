from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ConflictWatchApiResponse(BaseModel):
    message: str
    tone: str = "success"
    state: dict[str, Any]


class RepositoryCreateRequest(BaseModel):
    providerType: str
    repositoryName: str
    externalRepoId: str


class RepositoryWebhookSecretsUpdateRequest(BaseModel):
    githubWebhookSecret: str = ""
    backlogWebhookSecret: str = ""


class SettingsUpdateRequest(BaseModel):
    staleDays: int
    longUnresolvedDays: int
    rawPayloadRetentionDays: int
    processingTraceEnabled: bool
    forcePushNoteEnabled: bool
    suppressNoticeNotifications: bool
    notificationDestination: str
    slackWebhookUrl: str = ""
    githubWebhookEndpoint: str
    backlogWebhookEndpoint: str
    githubWebhookSecret: str
    backlogWebhookSecret: str


class IgnoreRuleCreateRequest(BaseModel):
    repositoryId: int
    pattern: str


class BranchMemoUpdateRequest(BaseModel):
    memo: str = ""


class BranchActionRequest(BaseModel):
    action: str


class ConflictMemoUpdateRequest(BaseModel):
    memo: str = ""


class ConflictStatusUpdateRequest(BaseModel):
    status: str


class BranchFileIgnoreCreateRequest(BaseModel):
    branchId: int
    normalizedFilePath: str
    memo: str = ""


class BranchFileIgnoreTargetRequest(BaseModel):
    branchId: int
    normalizedFilePath: str


class BranchFileIgnoreMemoUpdateRequest(BaseModel):
    branchId: int
    normalizedFilePath: str
    memo: str = ""


class SimulatedWebhookRequest(BaseModel):
    repositoryId: int
    provider: str
    deliveryId: str = ""
    branchName: str
    pusher: str = ""
    signatureStatus: str = "valid"
    deletedState: str = "false"
    simulateFailure: bool = False
    isForced: bool = False
    added: str = ""
    modified: str = ""
    removed: str = ""
    renamed: str = ""


class ConflictWatchWebhookAccepted(BaseModel):
    accepted: bool = True
    message: str
    deliveryId: str = Field(alias="delivery_id")

    model_config = {"populate_by_name": True}


class WebhookEventRawPayloadResponse(BaseModel):
    eventId: int
    providerType: str
    deliveryId: str
    rawPayloadRef: str = ""
    rawPayloadExpiredAt: str | None = None
    isAvailable: bool = False
    content: str = ""


class WebhookEventProcessingTraceResponse(BaseModel):
    eventId: int
    providerType: str
    deliveryId: str
    processingTraceRef: str = ""
    processingTraceExpiredAt: str | None = None
    isAvailable: bool = False
    content: str = ""
