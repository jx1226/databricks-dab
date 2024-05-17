# Databricks notebook source
# Creating a list of sandboxes, for adding a new sandbox, just add one more item to the list below:
sandboxes = [
    "prod-de",
    "prod",
    "prod-fr",
    "prod-ch",
    "prod-pl",
    "prod-es",
    "adhesive",
    "prod-b2b",
    "sweden",
    "prod-us",
    "prod-it",
    "prod-jp",
    "latam",
    "prod-at",
]


# COMMAND ----------

import aepp
import json
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col, when, lit
from functools import reduce
from aepp import segmentation
from datetime import datetime
from aep_profiles_kpi import *

# Enable Schema Evolution
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", True)

# Define the secrets
secret_scope = "cq-secret"
json_secret_name = "adobe-kpi-env-file"
json_secret_value = dbutils.secrets.get(secret_scope, json_secret_name)

# Load the JSON string
json_data = json.loads(json_secret_value)

# Get Client ID
client_id = None
for item in json_data["values"]:
    if item["key"] == "API_KEY":
        client_id = item["value"]
        break

# Get Client Secret
client_secret = None
for item in json_data["values"]:
    if item["key"] == "CLIENT_SECRET":
        client_secret = item["value"]
        break

# Get IMS_ORG
IMS_ORG = None
for item in json_data["values"]:
    if item["key"] == "IMS_ORG":
        IMS_ORG = item["value"]
        break


# COMMAND ----------

# Create dictSandboxInstances
dictSandboxInstances = {
    sandb: aepp.configure(
        org_id=IMS_ORG,
        secret=client_secret,
        client_id=client_id,
        scopes="openid,AdobeID,read_organizations,additional_info.projectedProductContext,session",
        connectInstance=True,
        sandbox=sandb,
    )
    for sandb in sandboxes
}


# COMMAND ----------

# Register the function as a UDF (User Defined Function)
extract_first_word_udf = spark.udf.register("extract_first_word", extract_first_word)

extract_country_udf = spark.udf.register("extract_country", extract_country)

# Register the UDF
map_and_rename_udf = udf(map_and_rename, StringType())

# List of keys to extract for segments
keys_to_extract = ["AllProfiles", "AnonymousOnly", "KnownProfiles"]

rows = []  ## will list all the dataframes
for sandb in sandboxes:  ## looping on sandboxes
    segmentSandbox = segmentation.Segmentation(
        config=dictSandboxInstances[sandb]
    )  ## connecting to the segmentation
    sandboxSegments = segmentSandbox.getSegments()  ## all segments
    for seg in sandboxSegments:
        # iterate all segments from each sandboxes and extract segments which have keys in keys_to_extract
        if any(
            len(seg["name"].split("_")) > 2 and key == seg["name"].split("_")[2]
            for key in keys_to_extract
        ):
            # Check if "name" and "totalProfiles" keys exist in the dictionary
            if "name" in seg and "metrics" in seg:
                name = seg["name"]
                total_profiles = seg["metrics"]["data"]["totalProfiles"]

                rows.append(
                    Row(
                        name=name,
                        total_profiles=total_profiles,
                    )
                )
            else:
                # Handle the case where "name" or "totalProfiles" does not exist
                print(
                    "Skipping segment due to missing 'name' or 'totalProfiles': "
                    + seg["name"]
                )


# Create a DataFrame from the list of Rows
raw_df = spark.createDataFrame(rows)

# Get the list of columns to check for not null values
profile_columns = raw_df.columns[1:]

# Create the "Country" column dynamically
conditions = [F.col(col).isNotNull() for col in profile_columns]
country_df = raw_df.withColumn(
    "Country",
    F.when(reduce(lambda x, y: x | y, conditions), extract_country_udf("name")),
)

# Apply the UDF to the 'Profile' column
renamed_df = country_df.withColumn("initiative", extract_first_word_udf("name"))

# Apply the UDF to the first column
pivot_df = renamed_df.withColumn("name", map_and_rename_udf(col("name")))

# Pivot the DataFrame
pivoted_df = (
    pivot_df.groupBy("initiative", "Country")
    .pivot("name")
    .agg(F.first("total_profiles"))
    .drop("name")
    .fillna(0)
)

# Add a new column for the current date
current_date = datetime.now().strftime("%Y-%m-%d")

result_df = pivoted_df.withColumn("Extracted_date", lit(current_date))

result_df = result_df.withColumn("Global_view", lit("Global"))

# Convert "ALL" to "Overall" in the "Status" column
result_df = result_df.withColumn(
    "initiative",
    when(result_df["initiative"] == "ALL", "Overall").otherwise(
        result_df["initiative"]
    ),
)


# COMMAND ----------

mount_point = "/mnt/aepsandbox-profiles-kpi"

env = dbutils.widgets.get("environment")

mount_blob_storage(mount_point, env)

write_data_to_delta(mount_point, result_df)
