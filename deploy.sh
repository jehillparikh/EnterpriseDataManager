#!/bin/bash

# Google Cloud Run Deployment Script for Mutual Fund API
# Usage: ./deploy.sh [PROJECT_ID] [REGION]

set -e

# Configuration
PROJECT_ID=${1:-"your-project-id"}
REGION=${2:-"us-central1"}
SERVICE_NAME="mutual-fund-api"

echo "🚀 Deploying Mutual Fund API to Google Cloud Run"
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service: $SERVICE_NAME"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ gcloud CLI is not installed. Please install it first:"
    echo "https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Set the project
echo "📋 Setting up Google Cloud project..."
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "🔧 Enabling required Google Cloud APIs..."
gcloud services enable run.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable containerregistry.googleapis.com

# Build and deploy using Cloud Build
echo "🏗️ Building and deploying with Cloud Build..."
gcloud builds submit --config cloudbuild.yaml \
    --substitutions=_DATABASE_URL="$DATABASE_URL",_PGUSER="$PGUSER",_PGPASSWORD="$PGPASSWORD",_PGHOST="$PGHOST",_PGPORT="$PGPORT",_PGDATABASE="$PGDATABASE"

# Get the service URL
echo "🌐 Getting service URL..."
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)")

echo ""
echo "✅ Deployment completed successfully!"
echo "🔗 Service URL: $SERVICE_URL"
echo ""
echo "📝 Next steps:"
echo "1. Test your API endpoints:"
echo "   curl $SERVICE_URL/api/funds"
echo ""
echo "2. Set up custom domain (optional):"
echo "   gcloud run domain-mappings create --service=$SERVICE_NAME --domain=your-domain.com --region=$REGION"
echo ""
echo "3. Monitor your service:"
echo "   https://console.cloud.google.com/run/detail/$REGION/$SERVICE_NAME"