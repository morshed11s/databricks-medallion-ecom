-- Reproducible source row count for files in the landing volume (excluding the header row).
-- Expected: 1,000,000 for each file. Byte sizes: orders.csv 66,468,913 and transactions.csv 71,233,922.
SELECT _metadata.file_name AS file_name,
       _metadata.file_size AS bytes,
       COUNT(*) - 1        AS data_rows_excluding_header
FROM read_files('/Volumes/workspace/ecom_poc/raw_files/', format => 'text')
GROUP BY ALL
ORDER BY file_name;
