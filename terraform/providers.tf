terraform {
  required_version = ">= 1.10"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }

  # Remote state in the bootstrap bucket, S3-native locking (no DynamoDB needed).
  # Bucket/key are passed at init so nothing account-specific is committed:
  #   terraform init -backend-config="bucket=marriott-dp-tfstate-<account>" -backend-config="key=<env>/main.tfstate"
  backend "s3" {
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project     = "marriott-data-platform"
      environment = var.env
      managed_by  = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  prefix     = "marriott-dp-${var.env}"
  db_prefix  = "marriott_dp_${var.env}"
  account_id = data.aws_caller_identity.current.account_id
}
