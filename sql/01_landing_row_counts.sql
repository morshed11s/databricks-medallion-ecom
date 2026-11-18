SELECT _metadata.file_name AS file_name,
       _metadata.file_size AS bytes,
       COUNT(*) - 1        AS data_rows_excluding_header
FROM read_files('/Volumes/ecom_catalog/ecom_poc/raw_files/', format => 'text')
GROUP BY ALL
ORDER BY file_name;
