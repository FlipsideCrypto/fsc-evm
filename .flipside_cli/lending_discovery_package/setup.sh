#!/bin/bash
# Lending Discovery Agent Setup
# Run this script to deploy the skill and agent

set -e

echo "=========================================="
echo "Lending Discovery Agent Setup"
echo "=========================================="
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Deploy the skill
echo "Deploying lending_discovery skill..."
flipside skills push "$SCRIPT_DIR/lending_discovery.skill.yaml"
echo ""

# Deploy the agent
echo "Deploying lending_discovery_agent..."
flipside agent push "$SCRIPT_DIR/lending_discovery.agent.yaml"
echo ""

echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Run the agent with:"
echo "  flipside agent run lending_discovery_agent --message \"Find missing lending protocols\""
echo ""
echo "Or start Claude Code with context:"
echo "  claude"
echo ""
echo "The CLAUDE.md file provides context about the lending models architecture."
