env                       = "prod"
region                    = "us-east-1"
glue_workers              = 4
monthly_budget_usd        = 50
freshness_threshold_hours = 26
# alert_email and ingestion_principal_arn come from TF_VAR_* (local env / GitHub variables),
# so no personal or account-specific values are committed.
