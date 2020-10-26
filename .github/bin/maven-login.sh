#!/usr/bin/env bash

set -euo pipefail

mavenCredentials=$(aws codeartifact get-authorization-token \
        --domain starburstdata-sep-cicd  \
        --query authorizationToken \
        --output text \
        --region us-east-2 \
        --domain-owner=843985043183)

echo "mavenUser=aws" >> "$GITHUB_ENV"
echo "::add-mask::$mavenCredentials"
echo "mavenCredentials=$mavenCredentials" >> "$GITHUB_ENV"
