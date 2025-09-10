resource "aws_cloudwatch_event_rule" "daily_ingestion" {
  name                = "${aws_lambda_function.lambda_fake_api_dbt_db_test.function_name}-daily"
  description         = "Triggers ${aws_lambda_function.lambda_fake_api_dbt_db_test.function_name} once per day"
  schedule_expression = var.schedule_expression
}

resource "aws_cloudwatch_event_target" "daily_ingestion_target" {
  rule      = aws_cloudwatch_event_rule.daily_ingestion.name
  target_id = "lambda"
  arn       = aws_lambda_function.lambda_fake_api_dbt_db_test.arn
}

resource "aws_lambda_permission" "allow_events" {
  statement_id  = "AllowExecutionFromEventBridgeDaily"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_fake_api_dbt_db_test.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_ingestion.arn
}
