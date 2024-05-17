import os
import logging
from pyspark.sql import SparkSession
from delta.tables import *

def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

# Define function to map and rename records
def map_and_rename(value):
    """
    Maps and renames records based on specified criteria.

    Args:
        value (str): The value to be mapped and renamed.

    Returns:
        str: The mapped and renamed value.
    """
    if "AllProfiles" in value:
        return "total_profiles"
    elif "AnonymousOnly" in value:
        return "total_anonymous_profiles"
    elif "KnownProfiles" in value:
        return "total_knwn_profiles"
    else:
        return value


# Define a function to extract the first word from the 'Profile' column as initiative
def extract_first_word(value):
    """
    Extracts the first word from the given value.

    Args:
        value (str): The value from which to extract the first word.

    Returns:
        str: The first word extracted from the value.
    """
    return value.split("_")[0]


# Define a function to extract the second word from the 'Profile' column as country
def extract_country(value):
    """
    Extracts the second word from the given value.

    Args:
        value (str): The value from which to extract the second word.

    Returns:
        str: The second word extracted from the value.
    """
    return value.split("_")[1]


def mount_blob_storage(mount_point, selected_environment):
    """
    Mounts the Blob Storage container based on the selected environment.

    Args:
        mount_point (str): The mount point for the Blob Storage.
        selected_environment (str): The selected environment (e.g., 'prd' or 'dev').

    Raises:
        ValueError: If an invalid environment is selected.
    """
    spark = SparkSession.builder.appName("mount_blob_storage").getOrCreate()
    dbutils = DBUtils(spark)

    # Define container details based on the selected environment
    container_name = "aepsandbox-profiles-kpi"
    tenant_id = dbutils.secrets.get(scope="cq-secret", key="tenant-id")

    if selected_environment == "prd":
        storage_account_name = "sweuprdcqreporting"
        sp_id = dbutils.secrets.get(scope="cq-secret", key="sp-cq-reporting-prd-id")
        sp_secret = dbutils.secrets.get(scope="cq-secret", key="sp-cq-reporting-prd")
    elif selected_environment == "dev":
        storage_account_name = "sweudevcqreporting"
        sp_id = dbutils.secrets.get(scope="cq-secret", key="sp-cq-reporting-dev-id")
        sp_secret = dbutils.secrets.get(scope="cq-secret", key="sp-cq-reporting-dev")
    else:
        raise ValueError("Invalid environment selected")

    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)

    # Mount the appropriate Blob Storage container
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": sp_id,
        "fs.azure.account.oauth2.client.secret": sp_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs,
    )


def write_data_to_delta(mount_point, target_df):
    """
    Writes data to Delta Table.

    Args:
        mount_point (str): The mount point for the Delta Table.
        target_df (DataFrame): The DataFrame containing the data to be written.
    """
    spark = SparkSession.builder.appName("write_data_to_delta").getOrCreate()
    dbutils = DBUtils(spark)

    # Existing delta table
    deltaTable = DeltaTable.forPath(spark, mount_point)

    # Execute deduplication
    deltaTable.alias("aepsandbox").merge(
        target_df.alias("updates"),
        "aepsandbox.Extracted_date = updates.Extracted_date"
        + " AND aepsandbox.Global_view = updates.Global_view"
        + " AND aepsandbox.Country = updates.Country"
        + " AND aepsandbox.initiative = updates.initiative"
        + " AND aepsandbox.total_profiles = updates.total_profiles"
        + " AND aepsandbox.total_anonymous_profiles = updates.total_anonymous_profiles"
        + " AND aepsandbox.total_knwn_profiles = updates.total_knwn_profiles",
    ).whenNotMatchedInsertAll().execute()


if __name__ == '__main__':
  main()