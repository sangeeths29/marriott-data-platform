env                = "dev"
region             = "us-east-1"
glue_workers       = 2
monthly_budget_usd = 20
enable_glue_jobs   = false # AWS support case open to enable Glue ETL; flip to true when granted
# alert_email and ingestion_principal_arn come from TF_VAR_* (local env / GitHub variables),
# so no personal or account-specific values are committed.
