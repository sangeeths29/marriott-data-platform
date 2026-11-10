# ---------- Alert channel ----------
resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

# ---------- Failure alerts: Glue job / crawler ----------
resource "aws_cloudwatch_event_rule" "glue_job_failed" {
  name = "${local.prefix}-glue-job-failed"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.bronze_to_silver.name, aws_glue_job.reservations_cdc_merge.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_rule" "crawler_failed" {
  name = "${local.prefix}-crawler-failed"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Crawler State Change"]
    detail = {
      crawlerName = [aws_glue_crawler.silver.name]
      state       = ["Failed"]
    }
  })
}

resource "aws_cloudwatch_event_target" "to_sns" {
  for_each = {
    job     = aws_cloudwatch_event_rule.glue_job_failed.name
    crawler = aws_cloudwatch_event_rule.crawler_failed.name
  }
  rule = each.value
  arn  = aws_sns_topic.alerts.arn
}

# ---------- Freshness SLA ----------
# scripts/check_freshness.py publishes HoursSinceSilverRefresh after every run.
# No data for a day (pipeline didn't run at all) also counts as breaching.
resource "aws_cloudwatch_metric_alarm" "silver_freshness" {
  alarm_name          = "${local.prefix}-silver-stale"
  alarm_description   = "Silver layer not refreshed within ${var.freshness_threshold_hours}h. See ops/runbook.md#stale-data"
  namespace           = "MarriottDataPlatform"
  metric_name         = "HoursSinceSilverRefresh"
  dimensions          = { Environment = var.env }
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = var.freshness_threshold_hours
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# ---------- Ops dashboard ----------
resource "aws_cloudwatch_dashboard" "pipeline" {
  dashboard_name = local.prefix
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title   = "Glue job elapsed time (ms)"
          region  = var.region
          stat    = "Maximum"
          metrics = [["Glue", "glue.driver.aggregate.elapsedTime", "JobName", aws_glue_job.bronze_to_silver.name, "JobRunId", "ALL", "Type", "count"]]
        }
      },
      {
        type = "metric", x = 12, y = 0, width = 12, height = 6
        properties = {
          title       = "Hours since silver refresh"
          region      = var.region
          stat        = "Maximum"
          metrics     = [["MarriottDataPlatform", "HoursSinceSilverRefresh", "Environment", var.env]]
          annotations = { horizontal = [{ value = var.freshness_threshold_hours, label = "SLA" }] }
        }
      },
      {
        type = "metric", x = 0, y = 6, width = 12, height = 6
        properties = {
          title   = "Rows loaded to bronze"
          region  = var.region
          stat    = "Sum"
          metrics = [["MarriottDataPlatform", "BronzeRowsLoaded", "Environment", var.env]]
        }
      },
      {
        type = "metric", x = 12, y = 6, width = 12, height = 6
        properties = {
          title   = "Athena bytes scanned"
          region  = var.region
          stat    = "Sum"
          metrics = [["AWS/Athena", "ProcessedBytes", "WorkGroup", aws_athena_workgroup.this.name]]
        }
      }
    ]
  })
}

# ---------- Cost guardrail ----------
resource "aws_budgets_budget" "monthly" {
  count        = var.alert_email == "" ? 0 : 1
  name         = "${local.prefix}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:project$marriott-data-platform"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
