# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3708c374-1738-4ced-9a7e-244443974c32",
# META       "default_lakehouse_name": "POC_Decrypt",
# META       "default_lakehouse_workspace_id": "524090e0-5827-4f82-982c-596f787629d1",
# META       "known_lakehouses": [
# META         {
# META           "id": "3708c374-1738-4ced-9a7e-244443974c32"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install python-gnupg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import gnupg
import json
import os
from pyspark.sql import functions as F

BASE_PATH   = "/lakehouse/default/Files/bronze/concur/sftp/poc"
DECRYPT_DIR = f"{BASE_PATH}/decrypted"
GPG_HOME    = "/tmp/gpg_concur_poc"

os.makedirs(GPG_HOME,    exist_ok=True)
os.makedirs(DECRYPT_DIR, exist_ok=True)
os.chmod(GPG_HOME, 0o700)

print("Setup complete")
print("Base path  :", BASE_PATH)
print("Decrypt dir:", DECRYPT_DIR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

INP_FILE = f"{BASE_PATH}/concur_credentials.inp"

with open(INP_FILE, 'r') as f:
    credentials = json.load(f)

pgp_key_id = credentials["pgp_key_id"]
passphrase = credentials["passphrase"]
key_file   = f"{BASE_PATH}/{credentials['key_file']}"

print("Credentials loaded")
print("Key ID  :", pgp_key_id)
print("Key file:", key_file)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gpg = gnupg.GPG(gnupghome=GPG_HOME)

with open(key_file, 'r') as f:
    private_key_data = f.read()

import_result = gpg.import_keys(private_key_data)
print("Keys imported:", import_result.count)
print("Fingerprints :", import_result.fingerprints)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def decrypt_pgp_file(encrypted_path, output_path, passphrase):
    with open(encrypted_path, 'rb') as f:
        result = gpg.decrypt_file(
            f,
            passphrase=passphrase,
            output=output_path
        )
    print(f"File   : {os.path.basename(encrypted_path)}")
    print(f"Success: {result.ok}")
    print(f"Status : {result.status}")
    if not result.ok:
        print(f"Error  : {result.stderr}")
    return result.ok

print("Decrypt function ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

attendee_pgp = f"{BASE_PATH}/extract_attendee_detail_dummy_20260618.txt.pgp"
attendee_out = f"{DECRYPT_DIR}/extract_attendee_detail_dummy_20260618.txt"

print("=== Decrypting Attendee file ===")
attendee_ok = decrypt_pgp_file(attendee_pgp, attendee_out, passphrase)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sae_pgp = f"{BASE_PATH}/extract_CES_SAE_v3_dummy_20260618.txt.pgp"
sae_out = f"{DECRYPT_DIR}/extract_CES_SAE_v3_dummy_20260618.txt"

print("=== Decrypting SAE file ===")
sae_ok = decrypt_pgp_file(sae_pgp, sae_out, passphrase)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if attendee_ok:
    df_attendee = spark.read \
        .option("delimiter", "|") \
        .option("header", "true") \
        .csv("Files/bronze/concur/sftp/poc/decrypted/extract_attendee_detail_dummy_20260618.txt")

    df_attendee = df_attendee \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("batch_date",          F.lit("2026-06-18")) \
        .withColumn("source_system",       F.lit("concur")) \
        .withColumn("source_filename",     F.lit("extract_attendee_detail_dummy_20260618.txt")) \
        .withColumn("file_type",           F.lit("attendee"))

    print("=== Attendee DataFrame ===")
    df_attendee.show(truncate=False)
else:
    print("Attendee decryption failed — skipping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if sae_ok:
    df_sae = spark.read \
        .option("delimiter", "|") \
        .option("header", "true") \
        .csv("Files/bronze/concur/sftp/poc/decrypted/extract_CES_SAE_v3_dummy_20260618.txt")

    df_sae = df_sae \
        .withColumn("ingestion_timestamp", F.current_timestamp()) \
        .withColumn("batch_date",          F.lit("2026-06-18")) \
        .withColumn("source_system",       F.lit("concur")) \
        .withColumn("source_filename",     F.lit("extract_CES_SAE_v3_dummy_20260618.txt")) \
        .withColumn("file_type",           F.lit("sae"))

    print("=== SAE DataFrame ===")
    df_sae.show(truncate=False)
else:
    print("SAE decryption failed — skipping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if attendee_ok:
    df_attendee.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("batch_date") \
        .saveAsTable("bronze_concur_attendee_raw")
    print("Attendee written to bronze_concur_attendee_raw")

if sae_ok:
    df_sae.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("batch_date") \
        .saveAsTable("bronze_concur_sae_raw")
    print("SAE written to bronze_concur_sae_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== bronze_concur_attendee_raw ===")
spark.sql("SELECT * FROM bronze_concur_attendee_raw").show(truncate=False)

print("=== bronze_concur_sae_raw ===")
spark.sql("SELECT * FROM bronze_concur_sae_raw").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
