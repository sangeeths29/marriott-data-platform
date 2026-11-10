terraform {
  required_version = ">= 1.5"
  required_providers {
    aws    = { source = "hashicorp/aws", version = "~> 5.0" }
    random = { source = "hashicorp/random", version = "~> 3.6" }
  }

  # Remote state with locking. Create the bucket + table once (bootstrap), then uncomment.
  # backend "s3" {
  #   bucket         = "<your-tf-state-bucket>"
  #   key            = "marriott-dp/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "<your-tf-lock-table>"
  #   encrypt        = true
  # }
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
