variable "access_key" {
  description = "access_key"
  type        = string
  sensitive   = true
}

variable "secret_key" {
  description = "secret_key"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS Region"
  default = "eu-west-1"
}

variable "default_tags" {
  description = "Standard tags applied to all resources"
  type        = map(string)
  default = {
    owner              = "email@email.com"
    env                = "dev"
    project            = "dbt_on_databricks_test_project"
    managed_by         = "terraform"
    data_classification = "internal"
    creation_date      = "20250908"
    retention_days     = "200"
  }
}

variable "schedule_expression" {
  type        = string
  default     = "cron(0 3 * * ? *)" 
  description = "EventBridge schedule expression"
}
