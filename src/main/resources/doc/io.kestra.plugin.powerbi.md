# How to use the Power BI plugin

Trigger Power BI dataset refreshes from Kestra flows.

## Authentication

Set `tenantId`, `clientId`, and `clientSecret` (all required) to authenticate via Azure AD service principal using the OAuth 2.0 client credentials flow. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`RefreshGroupDataset` triggers a dataset refresh — set `groupId` (workspace ID) and `datasetId`. By default `wait` is `false` (fire-and-forget); set `wait: true` to poll until the refresh completes. Control polling with `pollDuration` (default 5 seconds) and `waitDuration` (default 10 minutes). The output always includes `requestId`; when `wait: true` it also includes `status`, `extendedStatus`, `refreshType`, `startTime`, and `endTime`.
