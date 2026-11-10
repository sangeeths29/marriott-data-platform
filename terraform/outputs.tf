output "lake_bucket" { value = aws_s3_bucket.lake.id }
output "artifacts_bucket" { value = aws_s3_bucket.artifacts.id }
output "glue_job_name" { value = aws_glue_job.bronze_to_silver.name }
output "glue_crawler_name" { value = aws_glue_crawler.silver.name }
output "ingestion_role_arn" { value = aws_iam_role.ingestion.arn }
output "athena_workgroup" { value = aws_athena_workgroup.this.name }
output "athena_staging_dir" { value = "s3://${aws_s3_bucket.artifacts.id}/athena-results/" }
output "silver_database" { value = aws_glue_catalog_database.silver.name }
output "gold_database" { value = aws_glue_catalog_database.gold.name }
output "alerts_topic_arn" { value = aws_sns_topic.alerts.arn }
output "reservations_glue_job_name" { value = aws_glue_job.reservations_cdc_merge.name }
output "ops_database" { value = aws_glue_catalog_database.ops.name }
output "airflow_env" {
  description = "Environment variables for the Airflow (MWAA) environment running airflow/dags"
  value = {
    PIPELINE_ENV          = "aws"
    DATA_ROOT             = "s3://${aws_s3_bucket.lake.id}"
    GLUE_JOB_REFERENCE    = aws_glue_job.bronze_to_silver.name
    GLUE_JOB_RESERVATIONS = aws_glue_job.reservations_cdc_merge.name
    GLUE_CRAWLER          = aws_glue_crawler.silver.name
    ALERT_SNS_TOPIC_ARN   = aws_sns_topic.alerts.arn
    ATHENA_STAGING_DIR    = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
    ATHENA_WORKGROUP      = aws_athena_workgroup.this.name
    SILVER_SCHEMA         = aws_glue_catalog_database.silver.name
    OPS_SCHEMA            = aws_glue_catalog_database.ops.name
    ATHENA_SCHEMA         = aws_glue_catalog_database.gold.name
  }
}
