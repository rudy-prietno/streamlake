# Initial Loader data from Source to Athena Data Lake
<img width="981" height="321" alt="image" src="https://github.com/user-attachments/assets/492c7793-665b-4599-b90d-edb46d4e775f" />

reference: https://dev.to/aws-builders/how-to-access-data-from-other-aws-account-using-athena-32ie
# Iam policy s3 cross-account access
### policy in AWS Account Production
``` bash
{
    "Version": "2012-10-17",
    "Id": "cross-account-bucket-policy-prod-to-datalake",
    "Statement": [
        {
            "Sid": "CrossAccountPermission",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::<AWS-Account-ID-Datalake>:root"
            },
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                "arn:aws:s3:::bucket-name-datalake",
                "arn:aws:s3:::bucket-name-datalake/daily-export/*"
            ]
        }
    ]
}
```

### policy in AWS Account Data Lake
``` bash
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ReadOnlyBucketProd",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListMultipartUploadParts",
                "s3:AbortMultipartUpload"
            ],
            "Resource": [
                "arn:aws:s3:::bucket-name-datalake",
                "arn:aws:s3:::bucket-name-datalake/daily-export/*",
                "arn:aws:s3:::aws-athena-query-results-ap-southeast-3",
                "arn:aws:s3:::aws-athena-query-results-ap-southeast-3/*"
            ]
        }
    ]
}
```

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
