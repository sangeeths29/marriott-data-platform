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
  description = "IAM principal (e.g. the Jenkins agent role) allowed to assume the ingestion role."
  type        = string
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
