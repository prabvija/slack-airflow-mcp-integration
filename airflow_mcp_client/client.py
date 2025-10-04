# app.py
import os
import re
import logging
from typing import Dict, Any

# Load environment variables first
from dotenv import load_dotenv

load_dotenv()


# PydanticAI imports
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStdio
from pydantic_ai.models.openai import OpenAIModel
from pydantic_ai.providers.openai import OpenAIProvider
from pydantic_ai.messages import ModelMessage, ToolCallPart, TextPart
from openai import AsyncAzureOpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = AsyncAzureOpenAI(
    azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
    api_version=os.environ["AZURE_OPENAI_API_VERSION"],
    api_key=os.environ["AZURE_OPENAI_API_KEY"],
    azure_deployment=os.environ["AZURE_OPENAI_DEPLOYMENT"],
)

# Airflow analysis system prompt
AIRFLOW_ANALYSIS_PROMPT = """
You are an expert Airflow Data Pipeline Analyst. Your role is to analyze Airflow DAGs, task instances, logs, and failures to provide intelligent insights and recommendations.

Your capabilities include:
1. Analyzing DAG structures and dependencies
2. Investigating task failures and their root causes
3. Providing troubleshooting recommendations
4. Summarizing pipeline health and status
5. Identifying patterns in failures

When analyzing Airflow data:
- Focus on actionable insights
- Identify root causes of failures
- Suggest specific remediation steps
- Highlight critical issues that need immediate attention
- Provide clear, concise summaries as if you're replying it in a slack chat with below template

Generate a response as if you're replying to a slack thread, use the markdown format with the following structure:
**DAG**: <dag_id>

**Failed Task**: <task_id> 

**Error**: Found an error with message <exact error message>

**Root Cause**: <root cause analysis>

Always be factual and base your analysis only on the data provided through the MCP tools.
"""


class AirflowMCPClient:
    """MCP Client for Airflow integration using PydanticAI"""

    def __init__(self):
        self.agent = None
        self.model_messages: list[ModelMessage] = []
        self._initialize_agent()

    def _initialize_agent(self):
        """Initialize the PydanticAI agent with MCP server"""
        try:
            # Initialize OpenAI model
            model = OpenAIModel(
                "gpt-4o-mini",  # or 'gpt-4' for more powerful analysis
                provider=OpenAIProvider(openai_client=client),
            )

            # Create MCP server connection to our Airflow server
            server = MCPServerStdio("python", args=["airflow_mcp_server/server.py"], cwd=os.getcwd())

            # Create the agent with MCP toolset
            self.agent = Agent(model=model, toolsets=[server], instructions=AIRFLOW_ANALYSIS_PROMPT)

            logger.info("Airflow MCP Agent initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize MCP Agent: {e}")
            self.agent = None

    async def analyze_airflow_issue(self, prompt: str) -> str:
        """Analyze an Airflow issue using the MCP agent"""
        if not self.agent:
            return "MCP Agent is not available. Please check the server connection."

        try:
            # Run the agent with the prompt
            response_text = ""
            tool_calls_made = []

            async with self.agent.iter(prompt, message_history=self.model_messages) as agent_run:
                async for node in agent_run:
                    if self.agent.is_call_tools_node(node):
                        self.model_messages.append(node.model_response)
                        for part in node.model_response.parts:
                            if isinstance(part, ToolCallPart):
                                tool_calls_made.append(f"🔧 Called: {part.tool_name}")
                            elif isinstance(part, TextPart):
                                response_text += part.content
                    elif self.agent.is_model_request_node(node):
                        self.model_messages.append(node.request)

            # Format the response with tool information
            if tool_calls_made:
                tools_info = "\n".join(tool_calls_made)
                return f"{response_text}\n\n*Tools used:*\n{tools_info}"

            return response_text if response_text else "❓ No analysis available"

        except Exception as e:
            logger.error(f"Error during MCP analysis: {e}")
            return f"Analysis failed: {str(e)}"

    async def health_check(self) -> Dict[str, Any]:
        """Check MCP server health"""
        if not self.agent:
            return {"success": False, "error": "Agent not initialized"}

        try:
            result = await self.analyze_airflow_issue("Check the health of the Airflow connection")
            return {"success": True, "message": result}
        except Exception as e:
            return {"success": False, "error": str(e)}


# Global MCP client instance
mcp_client = AirflowMCPClient()


async def handle_airflow_alert(message):
    """Handle Airflow failure alerts using AI analysis"""
    try:
        text = message.get("text", "")

        # Extract DAG information
        # Pattern for "DAG <dag_id> has failed" or "DAG: dag_id" formats
        dag_id_match = re.search(r"DAG\s*:?\s*<?([\w.-]+)>?", text, re.IGNORECASE)
        dag_id = dag_id_match.group(1) if dag_id_match else None
        logger.info(f"Extracted DAG ID: {dag_id if dag_id else 'None'}")

        # Build analysis prompt based on extracted information
        if dag_id:
            analysis_prompt = f"""
            CRITICAL INSTRUCTIONS: You are analyzing an Airflow DAG failure. Follow these steps EXACTLY in order. Do NOT skip steps.

            REQUIRED WORKFLOW - Execute these steps sequentially:

            STEP 1: FIND THE CORRECT DAG
            - Find the exact DAG name that matches the provided dag_id='{dag_id}'.
            - Wait for the response before proceeding.
            - If no matching DAG is found, stop and report "DAG not found".

            STEP 2: IDENTIFY THE LATEST FAILED TASK
            - Using the DAG ID, find the most recent failed task instance.
            - This should provide details like the exact DAG ID, the run ID, and the task ID.
            - Wait for the complete response before proceeding.
            - If no failed tasks are found, stop and report "No recent failures found".

            STEP 3: RETRIEVE AND FILTER TASK LOGS
            - Retrieve the logs for the failed task identified in the previous step.
            - The logs should be automatically filtered to show only relevant error messages.
            - Perform this action ONLY ONCE.
            - Wait for the log content before proceeding.

            STEP 4: ANALYZE ERROR CONTENT
            - The log content from Step 3 is already filtered for errors.
            - Look specifically for these patterns in the returned content:
              * SQLException messages (highest priority)
              * java.lang.Exception traces
              * Lines containing "Error in job" followed by specific error details
              * ERROR or CRITICAL level log messages
              * Data quality (DQ) validation failures
              * Connection timeout or resource limit errors
            - Do NOT make assumptions - only analyze what is actually present in the logs.
            - Prioritize database/SQL errors over generic Spark exceptions.

            STEP 5: PROVIDE STRUCTURED SUMMARY
            - Use ONLY the information gathered from the steps above.
            - Do NOT add information not present in the analysis results.
            - Format the response EXACTLY as follows:

            **DAG**: [Use dag_id from Step 2 response]
            **Failed Task**: [Use task_id from Step 2 response] 
            **Error**: [Quote the EXACT error message found in Step 3 logs]
            **Root Cause**: [Based ONLY on the error analysis, provide likely cause]

            CONSTRAINTS:
            - Perform each step sequentially and only once.
            - Do not hallucinate or add information not gathered from the analysis.
            - If any step fails, stop and report the failure reason.
            - Use only factual information.

            Original alert message for context: {text}
            
            BEGIN ANALYSIS NOW - Follow steps 1-5 sequentially.
            """

            logger.info(analysis_prompt)
        else:
            analysis_prompt = f"""
            Return as 'Proper DAG not found'
            """
            logger.info(analysis_prompt)

        result = await mcp_client.analyze_airflow_issue(analysis_prompt)

        # Format and send the analysis
        if len(result) > 3000:
            chunks = [result[i : i + 3000] for i in range(0, len(result), 3000)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    return f"** Agent Analysis Report (Part {i + 1}/{len(chunks)}):**\n{chunk}"
                else:
                    return f"**Part {i + 1}/{len(chunks)}:**\n{chunk}"
        else:
            return f"**Agent Analysis Report :**\n{result}"

    except Exception as e:
        logger.error(f"Error handling Airflow alert: {e}")
