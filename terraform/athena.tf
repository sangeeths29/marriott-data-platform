resource "aws_athena_workgroup" "this" {
  name          = local.prefix
  force_destroy = true

  configuration {
    # Not enforced, so dbt can write gold tables to the lake (s3_data_dir = .../gold/) instead
    # of the query-results prefix, which expires after 7 days. The per-query scan cutoff
    # below still applies to every query in the workgroup.
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true
    # Cost guardrail: kill any single query scanning more than 10 GB.
    bytes_scanned_cutoff_per_query = 10 * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
