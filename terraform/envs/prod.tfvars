env                       = "prod"
region                    = "us-east-1"
glue_workers              = 4
monthly_budget_usd        = 50
freshness_threshold_hours = 26
ingestion_principal_arn   = "arn:aws:iam::<ACCOUNT_ID>:role/<jenkins-agent-role>"
alert_email               = ""
