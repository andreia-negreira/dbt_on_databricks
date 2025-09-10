terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region     = var.aws_region
}

resource "aws_s3_bucket" "dbt_db_bucket" {
  bucket = "fake-bronze-data-bucket-5578" 
  force_destroy = true

  tags = var.default_tags
}