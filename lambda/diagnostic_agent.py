import json
import boto3
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# All pipeline resources (Glue, CloudWatch, SNS) live in ap-south-2
REGION = "ap-south-2"

# Claude model to use via direct Anthropic API
# Using Haiku — fast, cheap, more than capable for log diagnosis
MODEL_ID = "claude-haiku-4-5"

# API key is stored as a Lambda environment variable (encrypted at rest by AWS KMS)
# Never hardcode API keys in source code
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

# Glue Python Shell jobs write their error output to this CloudWatch log group.
# The log stream inside this group is named after the Glue job run ID (e.g. jr_abc123).
GLUE_ERROR_LOG_GROUP = "/aws-glue/python-jobs/error"

# How many log lines to pull — enough to see the full traceback without noise
MAX_LOG_LINES = 50

# SNS topic ARN comes from an environment variable set on the Lambda.
# We never hardcode ARNs in code — that couples code to infrastructure.
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
logs_client = boto3.client("logs",   region_name=REGION)
sns_client  = boto3.client("sns",    region_name=REGION)

# ---------------------------------------------------------------------------
# STEP 1 — Parse the Step Functions failure event
#
# When a Glue job fails, Step Functions sends a Catch payload to this Lambda.
# The payload looks like:
#   {
#     "pipeline_error": {
#       "Error": "Glue.AmazonGlueException",
#       "Cause": "{\"JobName\":\"silver-transform\",\"JobRunId\":\"jr_abc123\",...}"
#     }
#   }
#
# Important: "Cause" is a JSON string inside a string — we must json.loads() it
# before we can access individual fields like JobName and JobRunId.
# ---------------------------------------------------------------------------
def get_failed_job_info(event: dict) -> tuple[str, str, str]:
    pipeline_error = event.get("pipeline_error", {})
    error_type     = pipeline_error.get("Error", "Unknown")
    cause_str      = pipeline_error.get("Cause", "{}")

    try:
        # Convert the nested JSON string into a real Python dictionary
        cause = json.loads(cause_str)
        job_name    = cause.get("JobName",      "unknown-job")
        job_run_id  = cause.get("JobRunId", cause.get("Id", ""))
        error_msg   = cause.get("ErrorMessage", cause_str)
    except (json.JSONDecodeError, AttributeError):
        # If Cause is not valid JSON, fall back to raw string
        job_name   = "unknown-job"
        job_run_id = ""
        error_msg  = cause_str

    logger.info("Parsed failure — Job: %s | RunID: %s", job_name, job_run_id)
    return job_name, job_run_id, error_msg


# ---------------------------------------------------------------------------
# STEP 2 — Fetch CloudWatch logs for the failed Glue run
#
# Step Functions only tells us THAT a job failed.
# CloudWatch tells us WHY — the actual Python traceback, the KeyError,
# the exact function and line number.
# Without this, Claude would be guessing. With this, Claude can pinpoint
# the exact problem.
#
# Each Glue job run gets its own log stream named after its job run ID.
# We pull the last MAX_LOG_LINES lines (most recent = most relevant).
# ---------------------------------------------------------------------------
def fetch_cloudwatch_logs(job_run_id: str) -> str:
    if not job_run_id:
        return "No job run ID available — cannot retrieve CloudWatch logs."

    try:
        response = logs_client.get_log_events(
            logGroupName  = GLUE_ERROR_LOG_GROUP,
            logStreamName = job_run_id,
            limit         = MAX_LOG_LINES,
            startFromHead = False,   # False = most recent lines first
        )
        events = response.get("events", [])
        if not events:
            return "No log events found in CloudWatch for this job run."

        # Join all log messages into one block of text for Claude to read
        return "\n".join(e["message"] for e in events)

    except logs_client.exceptions.ResourceNotFoundException:
        return f"Log stream '{job_run_id}' not found in {GLUE_ERROR_LOG_GROUP}."
    except Exception as exc:
        return f"Error fetching CloudWatch logs: {exc}"


# ---------------------------------------------------------------------------
# STEP 3 — Call Claude via AWS Bedrock Mantle
#
# AnthropicBedrock() is the Anthropic SDK client for Bedrock.
# It automatically signs requests using the Lambda's IAM role credentials
# (SigV4 signing) — no API keys, no secrets in code.
#
# We build a prompt that gives Claude:
#   - Context about what this pipeline does
#   - Which job failed and what the error type is
#   - The actual CloudWatch log lines (the evidence)
#
# The quality of Claude's answer directly depends on the quality of our prompt.
# More context = more accurate diagnosis.
# ---------------------------------------------------------------------------
def call_claude(job_name: str, error_msg: str, log_output: str) -> str:
    # Direct Anthropic API — no Bedrock, no AWS Marketplace, no regional quotas.
    # Authenticates using the API key stored in Lambda environment variables.
    from anthropic import Anthropic
    client = Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""You are an expert AWS data engineering assistant diagnosing a pipeline failure.

Pipeline: Medallion Architecture with 3 AWS Glue Python Shell jobs:
1. bronze-to-silver   - reads CSV files from S3, writes Parquet
2. silver-transform   - cleans and transforms data (deduplication, normalization, type casting)
3. silver-to-gold     - builds star schema (dim_customers, dim_products, fact_sales)

Failed Job: {job_name}
Error: {error_msg}

CloudWatch Logs (last {MAX_LOG_LINES} lines):
{log_output}

Diagnose this failure:
1. Root cause in plain English
2. Exact file or function that failed
3. Specific code fix needed
4. Is it safe to re-run the pipeline?"""

    message = client.messages.create(
        model      = MODEL_ID,
        max_tokens = 1024,
        messages   = [{"role": "user", "content": prompt}],
    )

    return message.content[0].text


# ---------------------------------------------------------------------------
# STEP 4 — Send the diagnosis via SNS email
#
# SNS (Simple Notification Service) lets us send a message to a topic.
# Anyone subscribed to that topic (e.g. your email) receives the message.
# One API call — the simplest possible notification mechanism in AWS.
# ---------------------------------------------------------------------------
def send_sns_notification(job_name: str, diagnosis: str, execution_arn: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    subject = f"[PIPELINE FAILURE] {job_name} failed — AI diagnosis inside"

    body = f"""AWS Medallion Pipeline — Failure Report
=========================================
Time        : {timestamp}
Failed Job  : {job_name}
Execution   : {execution_arn}

AI DIAGNOSIS  (Claude {MODEL_ID} via Anthropic API)
--------------------------------------------------
{diagnosis}

--
This report was generated automatically by the AI Diagnostic Agent.
Check CloudWatch logs at: {GLUE_ERROR_LOG_GROUP}
"""

    sns_client.publish(
        TopicArn = SNS_TOPIC_ARN,
        Subject  = subject[:100],   # SNS enforces a 100-character subject limit
        Message  = body,
    )
    logger.info("Diagnosis emailed via SNS topic: %s", SNS_TOPIC_ARN)


# ---------------------------------------------------------------------------
# Lambda entry point
#
# AWS calls lambda_handler automatically when the Lambda is triggered.
# `event`   — the JSON payload passed by Step Functions (contains the error)
# `context` — Lambda runtime info (function name, request ID, time remaining)
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    logger.info("AI Diagnostic Agent triggered")
    logger.info("Event received: %s", json.dumps(event, default=str))

    execution_arn = event.get("execution_arn", context.invoked_function_arn)

    # Step 1 — figure out which job failed and get the run ID
    job_name, job_run_id, error_msg = get_failed_job_info(event)

    # Step 2 — pull the actual error logs from CloudWatch
    logger.info("Fetching CloudWatch logs for run: %s", job_run_id)
    log_output = fetch_cloudwatch_logs(job_run_id)

    # Step 3 — send everything to Claude and get a diagnosis
    logger.info("Calling Claude via Anthropic API (model: %s)...", MODEL_ID)
    diagnosis = call_claude(job_name, error_msg, log_output)
    logger.info("Diagnosis received (%d characters)", len(diagnosis))

    # Step 4 — email the diagnosis via SNS
    send_sns_notification(job_name, diagnosis, execution_arn)

    return {
        "statusCode"      : 200,
        "diagnosed_job"   : job_name,
        "diagnosis_length": len(diagnosis),
    }
