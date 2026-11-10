# ---------- Glue role: least privilege per data zone ----------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_data" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, aws_s3_bucket.artifacts.arn]
  }
  statement {
    sid       = "ReadBronze"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/bronze/*", "${aws_s3_bucket.lake.arn}/silver/*"]
  }
  statement {
    sid       = "IcebergWarehouse"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/iceberg/*"]
  }
  statement {
    sid     = "IcebergCatalog"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
    resources = [
      "arn:aws:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.silver.arn,
      aws_glue_catalog_database.ops.arn,
      "arn:aws:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.silver.name}/*",
      "arn:aws:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.ops.name}/*",
    ]
  }
  statement {
    sid       = "WriteSilver"
    actions   = ["s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/silver/*"]
  }
  statement {
    sid       = "ReadCode"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/glue/*"]
  }
  statement {
    sid       = "ReadPiiSalt"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.pii_salt.arn]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "data-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

# ---------- Ingestion role: assumed by Jenkins to land + validate raw files ----------
data "aws_iam_policy_document" "ingestion_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.ingestion_principal_arn]
    }
  }
}

resource "aws_iam_role" "ingestion" {
  name                 = "${local.prefix}-ingestion"
  assume_role_policy   = data.aws_iam_policy_document.ingestion_assume.json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "ingestion" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid     = "RawZones"
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/landing/*",
      "${aws_s3_bucket.lake.arn}/bronze/*",
      "${aws_s3_bucket.lake.arn}/quarantine/*",
    ]
  }
  statement {
    sid       = "RunTransform"
    actions   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:StartCrawler", "glue:GetCrawler"]
    resources = ["*"]
  }
  statement {
    sid       = "PublishFreshness"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["MarriottDataPlatform"]
    }
  }
}

resource "aws_iam_role_policy" "ingestion" {
  name   = "ingestion"
  role   = aws_iam_role.ingestion.id
  policy = data.aws_iam_policy_document.ingestion.json
}
