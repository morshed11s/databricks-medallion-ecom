# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

import hashlib, json, os
from datetime import datetime, timezone

EXPECTED = {
    "orders.csv": {"bytes": 66468913, "data_rows": 1000000,
                   "sha256": "e453eec15117b97ed0de6945f53364c762c10769fb56c39959cd665500423529"},
    "transactions.csv": {"bytes": 71233922, "data_rows": 1000000,
                         "sha256": "07c875be34b47c68babb3ff28a2a26d538ed7ed4ffe69d131019e272ffa36313"},
}
try:
    with open(os.path.join(os.getcwd(), "..", "generator", "manifest.json")) as f:
        m = json.load(f)
    EXPECTED = {x["path"]: {"bytes": x["bytes"], "data_rows": x["data_rows_excluding_header"],
                            "sha256": x["sha256"]} for x in m["files"]}
    print("Loaded expectations from generator/manifest.json")
except Exception as e:
    print("Using embedded expectations (manifest not readable here):", type(e).__name__)

# COMMAND ----------

def scan(path):
    h, lines = hashlib.sha256(), 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
            lines += chunk.count(b"\n")
    return h.hexdigest(), lines - 1

checked_at = datetime.now(timezone.utc)
results = []
for fi in dbutils.fs.ls(RAW_PATH):
    if not fi.name.endswith(".csv"):
        continue
    sha, rows = scan(fi.path.replace("dbfs:", ""))
    exp = EXPECTED.get(fi.name, {})
    results.append((fi.name, fi.path.replace("dbfs:", ""), int(fi.size), int(rows), sha,
                    exp.get("bytes"), exp.get("data_rows"), exp.get("sha256"),
                    fi.size == exp.get("bytes") and rows == exp.get("data_rows") and sha == exp.get("sha256"),
                    checked_at))

schema = ("file_name string, file_path string, bytes long, data_rows long, sha256 string, "
          "expected_bytes long, expected_data_rows long, expected_sha256 string, "
          "matches_manifest boolean, checked_at timestamp")
landing = spark.createDataFrame(results, schema)
display(landing)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('ops_landing_file_log')} ({schema})
COMMENT 'Audit log of source files found in the landing volume: bytes, exact data-row counts, sha256 vs generator manifest'
""")
landing.write.mode("append").saveAsTable(t("ops_landing_file_log").replace("`", ""))

assert len(results) == 2, f"Expected 2 CSV files in {RAW_PATH}, found {len(results)}"
assert all(r[8] for r in results), "A landed file does not match the generator manifest!"
print("Both files landed unchanged: bytes, row counts and SHA-256 match the manifest.")

# COMMAND ----------

display(spark.sql(f"""
SELECT _metadata.file_name            AS file_name,
       _metadata.file_size            AS bytes,
       COUNT(*) - 1                   AS data_rows_excluding_header
FROM read_files('{RAW_PATH}/', format => 'text')
GROUP BY ALL
ORDER BY file_name
"""))
