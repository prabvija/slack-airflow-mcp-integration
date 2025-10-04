from typing import Any, Dict, Optional
import logging
from mcp.server.fastmcp import FastMCP
import os
import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables first
from dotenv import load_dotenv

load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("Airflow MCP server")

# Airflow configuration
AIRFLOW_BASE_URL = os.environ["AIRFLOW_BASE_URL"]
AIRFLOW_USERNAME = os.environ["AIRFLOW_USERNAME"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]


class AirflowClient:
    """Client for interacting with Airflow REST API"""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
        self.headers = {"Accept": "application/json", "Content-Type": "application/json"}

    async def _make_request(self, endpoint: str, method: str = "GET", data: Optional[Dict] = None) -> Dict[str, Any]:
        """Make HTTP request to Airflow API"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        async with httpx.AsyncClient(verify=False) as client:  # Disable SSL verification if needed
            try:
                if method.upper() == "GET":
                    response = await client.get(url, auth=self.auth, headers=self.headers, timeout=30.0)
                elif method.upper() == "POST":
                    response = await client.post(url, auth=self.auth, headers=self.headers, json=data, timeout=30.0)
                elif method.upper() == "PATCH":
                    response = await client.patch(url, auth=self.auth, headers=self.headers, json=data, timeout=30.0)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as e:
                logger.error(f"HTTP error occurred: {e}")
                raise
            except Exception as e:
                logger.error(f"An error occurred: {e}")
                raise


# Initialize Airflow client
airflow_client = AirflowClient(AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


@mcp.tool()
async def health_check() -> Dict[str, Any]:
    """
    Check the health of the Airflow connection

    Returns:
        Dictionary containing health status
    """
    try:
        result = await airflow_client._make_request("health")
        return {
            "success": True,
            "data": result,
            "message": "Airflow connection is healthy",
            "airflow_url": AIRFLOW_BASE_URL,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Airflow connection failed health check",
            "airflow_url": AIRFLOW_BASE_URL,
        }


@mcp.tool()
async def get_task_logs(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> Dict[str, Any]:
    """
    Get logs for a specific task instance

    Args:
        dag_id: The ID of the DAG
        dag_run_id: The ID of the DAG run
        task_id: The ID of the task
        try_number: The try number (default: 1)
        full_content: Whether to return full log content (default: True)

    Returns:
        Dictionary containing task logs
    """
    try:
        endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
        logger.info(f"endpoint {endpoint}")
        logger.info(f"get_task_logs endpoint: {endpoint}")

        # For logs, we might get text response instead of JSON
        url = f"{airflow_client.base_url}/{endpoint.lstrip('/')}"

        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(url, auth=airflow_client.auth, headers={"Accept": "text/plain"}, timeout=30.0)
            response.raise_for_status()
            # logger.info(f'response: {response.text}')
            extraction_result = extract_error_from_logs(response.text)
            log_content = extraction_result.get("data", {}).get("extracted_log", "Could not extract logs.")

            logger.info(f"log_content: {log_content}")

            # log_content = response.text

        return {
            "success": True,
            "data": {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "task_id": task_id,
                "try_number": try_number,
                "log_content": log_content,
            },
            "message": f"Retrieved logs for task: {task_id} in DAG: {dag_id}",
        }
    except Exception as e:
        logger.error(f"log_content: {str(e)}")
        return {"success": False, "error": str(e), "message": f"Failed to retrieve logs for task: {task_id}"}


@mcp.tool()
async def get_latest_failed_task(old_dag_id: str) -> Dict[str, Any]:
    """
    Get the latest failed task for a given DAG.
    It searches backwards from the most recent DAG run to find the first run with a failure,
    and then returns the latest failed task within that run.

    Args:
        old_dag_id: The ID of the DAG to inspect.

    Returns:
        Dictionary containing the details of the latest failed task instance.
    """
    try:
        dag_id = await extract_matching_dags(old_dag_id)

        # Get the latest DAG runs, ordered by execution date descending
        dag_runs_endpoint = f"dags/{dag_id}/dagRuns?order_by=-execution_date"
        logger.info(f"dag_runs_endpoint {dag_runs_endpoint}")
        dag_runs_result = await airflow_client._make_request(dag_runs_endpoint)
        dag_runs = dag_runs_result.get("dag_runs", [])

        if not dag_runs:
            return {"success": True, "data": None, "message": f"No DAG runs found for DAG: {dag_id}"}

        # Iterate through DAG runs to find the first one with a failed task
        for dag_run in dag_runs:
            dag_run_id = dag_run.get("dag_run_id")
            if not dag_run_id:
                continue

            # Get task instances for the current DAG run
            task_instances_endpoint = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
            logger.info(f"task_instances_endpoint: {task_instances_endpoint}")
            task_instances_result = await airflow_client._make_request(task_instances_endpoint)
            task_instances = task_instances_result.get("task_instances", [])

            # Filter for failed task instances that have an end_date
            failed_tasks = [task for task in task_instances if task.get("state") == "failed" and task.get("end_date")]

            if failed_tasks:
                # Sort failed tasks by end_date to find the most recent one
                latest_failed_task = sorted(failed_tasks, key=lambda x: x["end_date"], reverse=True)[0]

                return {
                    "success": True,
                    "data": latest_failed_task,
                    "message": f"Found latest failed task in DAG run: {dag_run_id}",
                }

        # If no failed tasks were found in any run
        return {"success": True, "data": None, "message": f"No failed tasks found in recent runs for DAG: {dag_id}"}

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": f"Failed to retrieve latest failed task for DAG: {dag_id}",
        }


@mcp.tool()
async def extract_matching_dags(dag_id: str) -> Any:
    """
    Find the exact DAG ID from a potentially mismatched user-provided DAG ID.
    This function fetches all available DAGs from Airflow and performs a case-insensitive and underscore-agnostic comparison to find a match.

    Args:
        dag_id: The user-provided DAG ID, which might have different casing or underscores.

    Returns:
        The matching DAG ID as a string if found, otherwise None.
    """
    get_dag_endpoint = f"dags"
    dag_result = await airflow_client._make_request(get_dag_endpoint)
    dag_objects = dag_result.get("dags", [])
    all_dags = [dag.get("dag_id") for dag in dag_objects if dag.get("dag_id")]

    logger.info(all_dags)

    normalized_input_dag = dag_id.replace("_", "").lower()

    for actual_dag in all_dags:
        normalized_actual_dag = actual_dag.replace("_", "").lower()
        if normalized_input_dag == normalized_actual_dag:
            logger.info(f"DAG found: {actual_dag}")
            return actual_dag

    return None


def extract_error_from_logs(log_content: str) -> Dict[str, Any]:
    """
    Extracts and summarizes error-related lines from a given log content.

    This tool is useful for quickly identifying the root cause of a failure from
    verbose log files by filtering for lines containing error-related keywords
    and providing surrounding context.

    Args:
        log_content: The full string content of the logs to be analyzed.

    Returns:
        A dictionary containing the extracted error log snippet and a flag indicating if an error was found.
    """
    try:
        if not log_content:
            return {
                "success": True,
                "data": {"extracted_log": "No log content available", "error_found": False},
                "message": "No log content provided.",
            }

        # Split log into lines for processing
        lines = log_content.split("\n")
        error_lines = []

        # Keywords that typically indicate errors
        error_keywords = [
            "error",
            "exception",
            "failed",
            "failure",
            "sqlexception",
            "java.lang.exception",
            "timeout",
            "connection error",
            "dbt error",
            "snowflakesqlexception",
        ]

        # Context window - number of lines to include before/after error
        context_window = 3
        error_found = False

        for i, line in enumerate(lines):
            line_lower = line.lower()

            # Check if this line contains an error keyword
            if any(keyword in line_lower for keyword in error_keywords):
                error_found = True

                # Add context lines before the error
                start_idx = max(0, i - context_window)
                end_idx = min(len(lines), i + context_window + 1)

                # Add timestamp if available
                for j in range(start_idx, end_idx):
                    if j < len(lines):
                        error_lines.append(lines[j])

                error_lines.append("---")  # Separator between different error blocks

        # Join and limit the total length
        result = "\n".join(error_lines)

        return {
            "success": True,
            "data": {"extracted_log": result, "error_found": error_found},
            "message": "Successfully extracted error logs." if error_found else "No errors found in logs.",
        }
    except Exception as e:
        return {"success": False, "error": str(e), "message": "Failed to extract error from logs."}


if __name__ == "__main__":
    print("Starting Airflow MCP Server...")
    print(f"Airflow URL: {AIRFLOW_BASE_URL}")
    mcp.run()
