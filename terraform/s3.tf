# ---------- Data lake: landing / bronze / silver / quarantine ----------
resource "aws_s3_bucket" "lake" {
  bucket = "${local.prefix}-lake-${local.account_id}"
}

# ---------- Artifacts: Glue scripts, packaged code, Athena query results ----------
resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.prefix}-artifacts-${local.account_id}"
}

locals {
  buckets = {
    lake      = aws_s3_bucket.lake
    artifacts = aws_s3_bucket.artifacts
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-landing"
    status = "Enabled"
    filter { prefix = "landing/" }
    expiration { days = 90 }
    noncurrent_version_expiration { noncurrent_days = 7 }
  }

  rule {
    id     = "expire-quarantine"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = 30 }
  }

  rule {
    id     = "old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 14 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
}

# Deny any non-TLS access.
data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [each.value.arn, "${each.value.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each   = local.buckets
  bucket     = each.value.id
  policy     = data.aws_iam_policy_document.tls_only[each.key].json
  depends_on = [aws_s3_bucket_public_access_block.this]
}
