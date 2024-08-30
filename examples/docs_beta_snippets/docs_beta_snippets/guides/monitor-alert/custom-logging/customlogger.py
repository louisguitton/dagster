import logging
import sys
from datetime import datetime

import dagster as dg
import dagster._config as dg_config
import dagster._core.definitions.logger_definition as dg_logging


@dg_logging.logger(
    {
        "log_level": dg_config.Field(str, is_required=False, default_value="INFO"),
        "name": dg_config.Field(str, is_required=False, default_value="dagster"),
    },
    description="A comma separated console logger.",
)
def readable_console_logger(init_context: dg.InitLoggerContext) -> logging.Logger:
    level = init_context.logger_config["log_level"]
    name = init_context.logger_config["name"]

    klass = logging.getLoggerClass()
    logger_ = klass(name, level=level)

    handler = logging.StreamHandler(stream=sys.stdout)

    class CommaSeparatedRecordFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord):
            dagster_meta = record.__dict__["dagster_meta"]
            fields = [
                datetime.fromtimestamp(record.created).isoformat(),
                record.name,
                record.levelname,
                dagster_meta.get("run_id", "-"),
                dagster_meta.get("job_name", "-"),
                dagster_meta.get("op_name", "-"),
                record.msg,
            ]

            return ",".join(fields)

    handler.setFormatter(CommaSeparatedRecordFormatter())
    logger_.addHandler(handler)

    return logger_
