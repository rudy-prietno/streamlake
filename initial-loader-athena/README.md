# Initial Loader data from Source to Athena Data Lake
<img width="981" height="321" alt="image" src="https://github.com/user-attachments/assets/492c7793-665b-4599-b90d-edb46d4e775f" />

# Run 
``` bash
python3 backfill_athena.py \
  --region ap-southeast-3 \
  --aws-access-key-id access-key \
  --aws-secret-access-key secret-key \
  --workgroup primary \
  --s3-output s3://bucket-name/backfill/databasename/tablename/ \
  --source-catalog AwsDataCatalog \
  --source-db aws-glue-catalog-database \
  --source-table tablename \
  --dest-catalog AwsDataCatalog \
  --dest-db aws-glue-catalog-database \
  --dest-table tablename \
  --pk id \
  --updated-col updated_at \
  --filter-col created_at \
  --start-ts '2025-09-17 00:00:00' \
  --end-ts   '2025-09-18 00:00:00'

or

python3 backfill_athena.py \
  --region ap-southeast-3 \
  --profile profile-ec2-name \
  --workgroup primary \
  --s3-output s3://bucket-name/backfill/databasename/tablename/ \
  --source-catalog AwsDataCatalog \
  --source-db aws-glue-catalog-database \
  --source-table tablename \
  --dest-catalog AwsDataCatalog \
  --dest-db aws-glue-catalog-database \
  --dest-table tablename \
  --pk id \
  --updated-col updated_at \
  --filter-col created_at \
  --start-ts '2025-09-17 00:00:00' \
  --end-ts   '2025-09-18 00:00:00'
```
