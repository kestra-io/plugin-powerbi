# Kestra PowerBI Plugin

## What

- Provides plugin components under `io.kestra.plugin.powerbi`.
- Includes classes such as `RefreshGroupDataset`, `Refreshes`, `Refresh`.

## Why

- This plugin integrates Kestra with Power BI.
- It provides tasks that refresh Microsoft Power BI datasets via the API.

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
