resource "aws_athena_workgroup" "this" {
  name          = local.prefix
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    # Cost guardrail: kill any single query scanning more than 10 GB.
    bytes_scanned_cutoff_per_query = 10 * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${aws_s3_bucket.artifacts.id}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}
