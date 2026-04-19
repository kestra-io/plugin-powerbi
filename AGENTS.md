# Kestra PowerBI Plugin

## What

- Provides plugin components under `io.kestra.plugin.powerbi`.
- Includes classes such as `RefreshGroupDataset`, `Refreshes`, `Refresh`.

## Why

- What user problem does this solve? Teams need to refresh Microsoft Power BI datasets from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Power BI steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Power BI.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `powerbi`

### Key Plugin Classes

- `io.kestra.plugin.powerbi.RefreshGroupDataset`

### Project Structure

```
plugin-powerbi/
├── src/main/java/io/kestra/plugin/powerbi/models/
├── src/test/java/io/kestra/plugin/powerbi/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
