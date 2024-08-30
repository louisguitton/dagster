import os

import pandas as pd

import dagster as dg

# Create the PartitionDefinition
region_partitions = dg.DynamicPartitionsDefinition(name="regions")


# Define the partitioned asset
@dg.asset(partitions_def=region_partitions)
def regional_sales_data(context: dg.AssetExecutionContext) -> None:
    # Simulate fetching daily sales data
    region = context.partition_key
    df = pd.DataFrame({"region": [region], "sales": [1000]})  # Placeholder data

    os.makedirs("regional_sales", exist_ok=True)
    filename = f"regional_sales/sales_{region}.csv"
    df.to_csv(filename, index=False)

    context.log.info(f"Regional sales data written to {filename}")


@dg.asset(
    partitions_def=region_partitions,
    deps=[regional_sales_data],
)
def daily_sales_summary(context):
    region = context.partition_key
    # Read the CSV file for the given partition date
    filename = f"regional_sales/sales_{region}.csv"
    df = pd.read_csv(filename)

    # Summarize daily sales
    summary = {
        "region": region,
        "total_sales": df["sales"].sum(),
    }

    context.log.info(f"Regional sales summary for {region}: {summary}")


# Create a partitioned asset job
regional_sales_job = dg.define_asset_job(
    name="regional_sales_job",
    selection=[regional_sales_data, daily_sales_summary],
    partitions_def=region_partitions,
)


@dg.sensor(job=regional_sales_job)
def all_regions_sensor(context: dg.SensorEvaluationContext):
    # Simulate fetching all regions from an external system
    all_regions = ["us", "eu", "jp", "ca", "uk", "au"]

    return dg.SensorResult(
        run_requests=[dg.RunRequest(partition_key=region) for region in all_regions],
        dynamic_partitions_requests=[region_partitions.build_add_request(all_regions)],
    )


# Define the Definitions object
defs = dg.Definitions(
    assets=[regional_sales_data, daily_sales_summary],
    jobs=[regional_sales_job],
    sensors=[all_regions_sensor],
)
