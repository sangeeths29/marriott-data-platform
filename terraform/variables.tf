variable "env" {
  description = "Environment name (dev | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env must be dev or prod."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "alert_email" {
  description = "Email subscribed to pipeline failure / freshness / budget alerts (confirm the subscription email)."
  type        = string
  default     = ""
}

variable "ingestion_principal_arn" {
  description = "IAM principal allowed to assume the ingestion role (e.g. the Airflow worker's role). Empty = any principal in this account that has sts:AssumeRole permission."
  type        = string
  default     = ""
}

variable "glue_workers" {
  type    = number
  default = 2
}

variable "monthly_budget_usd" {
  type    = number
  default = 20
}

variable "freshness_threshold_hours" {
  description = "Alarm if silver has not been refreshed within this many hours."
  type        = number
  default     = 30
}

variable "enable_glue_jobs" {
  description = "Create the Glue ETL jobs + crawler. Set false while AWS has Glue ETL disabled for the account (hybrid mode)."
  type        = bool
  default     = true
}
