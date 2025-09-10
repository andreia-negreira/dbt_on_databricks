resource "aws_iam_role" "lambda_fake_api_dbt_db_test_role" {
  name = "fake_api_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "lambda_fake_api_dbt_db_test_policy" {
  name        = "fake_api_lambda_policy"
  description = "IAM policy for Glue metrics Lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
        Effect   = "Allow"
      },
      {
        Action = [
          "s3:*"
        ]
        Resource = "*"
        Effect   = "Allow"
      }
    ]
  })
}


resource "aws_iam_role_policy_attachment" "lambda_fake_api_dbt_db_test_policy_attachment" {
  role       = aws_iam_role.lambda_fake_api_dbt_db_test_role.name
  policy_arn = aws_iam_policy.lambda_fake_api_dbt_db_test_policy.arn
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "./../scripts/index.py"
  output_path = "index.zip"
}

resource "aws_lambda_function" "lambda_fake_api_dbt_db_test" {
  filename      = "index.zip" 
  function_name = "lambda_fake_api_dbt_db_test"
  role          = aws_iam_role.lambda_fake_api_dbt_db_test_role.arn
  handler       = "index.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.dbt_db_bucket.bucket
      BASE_URL        = "https://fakeapi.net"
      TABLE_ENDPOINTS = "/products,/users,/orders,/reviews"
      PAGE_PARAM      = "page"
      START_PAGE      = "1"
      PAGE_SIZE_PARAM = "limit"
      PAGE_SIZE       = "100"
      MAX_PAGES       = "0"
      S3_PREFIX       = "fake_ecom/raw"
      }
    }

  tags = var.default_tags
}