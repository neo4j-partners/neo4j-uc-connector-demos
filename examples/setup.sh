#!/bin/bash
#
# Setup Databricks secrets for Neo4j UC JDBC Test Suite
# Reads credentials from .env file and creates secrets in Databricks
#

set -e

SCOPE_NAME="neo4j-uc-creds"
ENV_FILE=".env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check for .env file
if [[ ! -f "$ENV_FILE" ]]; then
    log_error ".env file not found"
    echo "Copy .env.sample to .env and fill in your Neo4j credentials:"
    echo "  cp .env.sample .env"
    exit 1
fi

# Check for databricks CLI
if ! command -v databricks &> /dev/null; then
    log_error "Databricks CLI not found"
    echo "Install with: pip install databricks-cli"
    echo "Or: brew install databricks"
    exit 1
fi

# Load .env file
log_info "Loading credentials from $ENV_FILE"
set -a
source "$ENV_FILE"
set +a

# Validate required variables
missing=()
[[ -z "$NEO4J_HOST" ]] && missing+=("NEO4J_HOST")
[[ -z "$NEO4J_USERNAME" ]] && missing+=("NEO4J_USERNAME")
[[ -z "$NEO4J_PASSWORD" ]] && missing+=("NEO4J_PASSWORD")
[[ -z "$UC_CONNECTION_NAME" ]] && missing+=("UC_CONNECTION_NAME")
[[ -z "$JDBC_JAR_PATH" ]] && missing+=("JDBC_JAR_PATH")
[[ -z "$LAKEHOUSE_CATALOG" ]] && missing+=("LAKEHOUSE_CATALOG")
[[ -z "$LAKEHOUSE_SCHEMA" ]] && missing+=("LAKEHOUSE_SCHEMA")

if [[ ${#missing[@]} -gt 0 ]]; then
    log_error "Missing required variables in .env: ${missing[*]}"
    exit 1
fi

# Create secret scope (ignore error if already exists)
log_info "Creating secret scope: $SCOPE_NAME"
if databricks secrets create-scope "$SCOPE_NAME" 2>/dev/null; then
    log_info "Secret scope created"
else
    log_warn "Secret scope already exists (or failed to create)"
fi

# Function to set a secret
set_secret() {
    local key=$1
    local value=$2
    log_info "Setting secret: $key"
    echo -n "$value" | databricks secrets put-secret "$SCOPE_NAME" "$key"
}

# Set secrets
set_secret "host" "$NEO4J_HOST"
set_secret "user" "$NEO4J_USERNAME"
set_secret "password" "$NEO4J_PASSWORD"
set_secret "connection_name" "$UC_CONNECTION_NAME"
set_secret "jdbc_jar_path" "$JDBC_JAR_PATH"
set_secret "lakehouse_catalog" "$LAKEHOUSE_CATALOG"
set_secret "lakehouse_schema" "$LAKEHOUSE_SCHEMA"

# Set optional database (default is "neo4j")
if [[ -n "$NEO4J_DATABASE" ]]; then
    set_secret "database" "$NEO4J_DATABASE"
fi

log_info "Done! Secrets configured in scope: $SCOPE_NAME"
echo ""
echo "Verify with:"
echo "  databricks secrets list-secrets $SCOPE_NAME"
echo ""
echo "Use in Databricks notebook:"
echo "  NEO4J_HOST = dbutils.secrets.get('neo4j-uc-creds', 'host')"
echo "  LAKEHOUSE_CATALOG = dbutils.secrets.get('neo4j-uc-creds', 'lakehouse_catalog')"
