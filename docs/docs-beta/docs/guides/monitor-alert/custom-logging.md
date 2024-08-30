---
title: "Setting up custom logging"
sidebar_position: 20
---


# Custom loggers

Customize loggers to emit your logs in a way that's more useful to you, whether to make them more readable or
easier to process by your log management system.


## What you'll learn

- How to write a custom logger
- How to configure your jobs to use this custom logger

<details>
  <summary>Prerequisites</summary>

To follow the steps in this guide, you'll need:

- A basic understanding of Dagster concepts such as assets, jobs and definitions
- A working knowledge of the Python logging module

</details>


## Step 1: Add a prebuilt custom logger to your jobs

This step shows how to add an existing custom logger to your jobs.

### Add the custom logger to your asset jobs

The following example shows how to add the custom logger to your code location definitions and configure an asset job to use it.

<CodeExample filePath="guides/monitor-alert/custom-logging/asset-job-example.py" language="python" title="Add custom logger to asset job" />


### Add the custom logger to your task-based jobs

Configuring a task job to use the custom logger slightly differs from the asset job example. The following example shows how:

<CodeExample filePath="guides/monitor-alert/custom-logging/task-job-example.py" language="python" title="Add custom logger to task job" />


### Changing the logger configuration in the Dagster UI

You can also change the logger configuration in the Dagster UI. This is useful if you want to change the logger configuration without changing the code, to use the custom logger on an ad-hoc asset materialization, or change the verbosity of the logs.

```yaml
loggers:
  console:
    config:
      log_level: DEBUG
```

## Step 2: Write your custom logger

In this example, we'll create a logger implementation that produces comma separated values from selected fields in the
log record. Other examples can be found in the codebase, in the built-in loggers such as `json_console_logger`.

<CodeExample filePath="guides/monitor-alert/custom-logging/customlogger.py" language="python" title="Example custom logger" />

Sample output:

```csv
2024-08-30T14:24:13.490405,dagster,INFO,3e092f8a-f2d7-4e34-8ff9-88b6c6a62255,demo_job,hello_logs,demo_job - 3e092f8a-f2d7-4e34-8ff9-88b6c6a62255 - hello_logs - Hello, world!
```

The available fields emitted by the logger are defined in the [`LogRecord`](https://docs.python.org/3/library/logging.html#logrecord-objects) object.
In addition, Dagster specific information can be found in the `dagster_meta` attribute of the log record. The previous
example already some of these attributes.

It contains the following fields:

- `dagster_event`: string
- `dagster_event_batch_metadata`: string
- `job_name`: string
- `job_tags`: a dictionary of strings
- `log_message_id`: string
- `log_timestamp`: string
- `op_name`: string
- `run_id`: string
- `step_key`: string

## Next steps

Import your own custom logger by modifying the example provided in step 1.

## Limitations

It's not currently possible to globally configure the logger for all jobs in a repository. This is a feature that's
planned for a future release. In the meantime, utility functions such as the `with_default_logger` provided in the
example can be used to apply the logger to all jobs while less duplication.
