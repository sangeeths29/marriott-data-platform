# One-time account bootstrap (run once per AWS account, from a laptop with admin access):
#   cd terraform/bootstrap && terraform init && terraform apply -var github_repo=<owner>/<repo>
#
# Creates the things the main stack and CI/CD depend on:
#   * S3 bucket for Terraform remote state (versioned, encrypted; S3-native state locking)
#   * GitHub Actions OIDC identity provider (no long-lived AWS keys in GitHub)
#   * plan role   - assumed by pull-request workflows: read-only + state access
#   * deploy role - assumed by workflows on main (GitHub "dev"/"prod" environments): can apply
#
# Its own state is local on purpose (chicken-and-egg); it's tiny and rarely changes.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "marriott-data-platform", managed_by = "terraform", stack = "bootstrap" }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  gh_sub     = "repo:${var.github_repo}"
}

# ---------------------------------------------------------------- remote state
resource "aws_s3_bucket" "tf_state" {
  bucket = "marriott-dp-tfstate-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration { status = "Enabled" } # recover from a bad apply
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket                  = aws_s3_bucket.tf_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------- GitHub OIDC
resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS validates GitHub's certificate chain itself; thumbprints kept for older tooling.
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1", "1c58a3a8518e8759bf075b76b750d4f2df264fcd"]
}

data "aws_iam_policy_document" "plan_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${local.gh_sub}:pull_request", "${local.gh_sub}:ref:refs/heads/*"]
    }
  }
}

data "aws_iam_policy_document" "deploy_trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github.arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      # only jobs bound to a protected GitHub environment can deploy
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["${local.gh_sub}:environment:dev", "${local.gh_sub}:environment:prod"]
    }
  }
}

data "aws_iam_policy_document" "state_access" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.tf_state.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.tf_state.arn}/*"] # PutObject/DeleteObject needed for the .tflock file
  }
}

# ---- plan role: read-only
resource "aws_iam_role" "plan" {
  name                 = "marriott-dp-github-plan"
  assume_role_policy   = data.aws_iam_policy_document.plan_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy_attachment" "plan_readonly" {
  role       = aws_iam_role.plan.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

resource "aws_iam_role_policy" "plan_state" {
  name   = "terraform-state"
  role   = aws_iam_role.plan.id
  policy = data.aws_iam_policy_document.state_access.json
}

# ---- deploy role: can manage this project's resources only
data "aws_iam_policy_document" "deploy" {
  statement {
    sid = "ProjectServices"
    actions = [
      "s3:*", "glue:*", "athena:*", "sns:*", "events:*", "cloudwatch:*", "logs:*",
      "ssm:*", "budgets:*",
    ]
    resources = ["*"]
  }
  statement {
    sid = "ProjectRolesOnly"
    actions = [
      "iam:CreateRole", "iam:DeleteRole", "iam:GetRole", "iam:UpdateRole", "iam:TagRole", "iam:UntagRole",
      "iam:PutRolePolicy", "iam:GetRolePolicy", "iam:DeleteRolePolicy", "iam:ListRolePolicies",
      "iam:AttachRolePolicy", "iam:DetachRolePolicy", "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole", "iam:UpdateAssumeRolePolicy", "iam:PassRole",
    ]
    resources = ["arn:aws:iam::${local.account_id}:role/marriott-dp-*"]
  }
  statement {
    sid       = "ReadIdentity"
    actions   = ["sts:GetCallerIdentity", "iam:ListRoles", "iam:GetPolicy", "iam:GetPolicyVersion"]
    resources = ["*"]
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "marriott-dp-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.deploy_trust.json
  max_session_duration = 3600
}

resource "aws_iam_role_policy" "deploy" {
  name   = "project-deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}

resource "aws_iam_role_policy" "deploy_state" {
  name   = "terraform-state"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.state_access.json
}
