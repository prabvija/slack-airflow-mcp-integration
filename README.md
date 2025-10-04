# Slack-Airflow MCP Integration

A comprehensive integration solution that connects Slack with Apache Airflow using Model Context Protocol (MCP) and AI agents. This system enables intelligent monitoring, analysis, and interaction with Airflow DAGs directly through Slack channels.

## 🏗️ Architecture

The system consists of three main components:

1. **MCP Server** (`airflow_mcp_server/`) - Provides MCP-compliant interface to Airflow REST API
2. **Slack Bot** (`slack_integration_kit/`) - Handles Slack interactions and message processing
3. **MCP Client** (`airflow_mcp_client/`) - AI agent that communicates with the MCP server for intelligent analysis

## 🚀 Features

- **Real-time Airflow Monitoring** - Monitor DAG runs, task statuses, and failures
- **Intelligent Analysis** - AI-powered analysis of Airflow logs and failures
- **Slack Integration** - Seamless interaction through Slack channels
- **Docker Support** - Easy deployment with Docker Compose
- **Secure Configuration** - Environment-based credential management

## 📋 Prerequisites

- Python 3.11+
- Docker & Docker Compose (for containerized deployment)
- Slack workspace with bot permissions
- Azure OpenAI access (or compatible OpenAI API)
- Apache Airflow instance with REST API enabled

## 🛠️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd slack-airflow-mcp-integration
```

### 2. Environment Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` file with your credentials:

```bash
# Slack Configuration
SLACK_BOT_TOKEN="xoxb-your-bot-token"
SLACK_APP_TOKEN="xapp-your-app-token"

# Azure OpenAI Configuration
AZURE_OPENAI_API_KEY="your-api-key"
AZURE_OPENAI_ENDPOINT="https://your-endpoint.openai.azure.com"
AZURE_OPENAI_DEPLOYMENT="your-deployment-name"
AZURE_OPENAI_API_VERSION="2024-10-21"

# Airflow Configuration
AIRFLOW_BASE_URL="https://your-airflow-instance.com/api/v1"
AIRFLOW_USERNAME="your-username"
AIRFLOW_PASSWORD="your-password"
```

### 3. Slack App Setup

1. Create a new Slack app at [api.slack.com](https://api.slack.com/apps)
2. Enable Socket Mode and generate App-Level Token
3. Add Bot Token Scopes:
   - `messages:channels`
4. Install the app to your workspace
5. Copy the Bot User OAuth Token and App-Level Token to your `.env` file

### 4. Installation & Running

#### Option A: Docker Compose (Recommended)

```bash

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

#### Option B: Local Development

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Start MCP Server:**
```bash
python airflow_mcp_server/server.py
```

3. **Start Slack Bot (in another terminal):**
```bash
python slack_integration_kit/chat_int.py
```

4. **Test MCP Client (optional, in another terminal):**
```bash
python airflow_mcp_client/client.py
```

## 🔧 Configuration Details

### Slack Bot Permissions

Ensure your Slack bot has the following OAuth scopes:
- `messages:channels`

### Airflow Requirements

- Airflow REST API must be enabled
- Authentication credentials with appropriate permissions
- Network access from the application to Airflow instance
