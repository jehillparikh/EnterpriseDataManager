# Google Cloud Deployment Guide

## Overview
Your mutual fund API is ready to deploy on Google Cloud Run. This guide provides step-by-step instructions for deploying your Flask application.

## Prerequisites

1. **Google Cloud Account**: Create one at https://cloud.google.com
2. **Google Cloud CLI**: Install from https://cloud.google.com/sdk/docs/install
3. **Docker** (optional): For local testing

## Deployment Options

### Option 1: Quick Deploy (Recommended)

1. **Set up Google Cloud CLI**:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Deploy directly**:
   ```bash
   gcloud run deploy mutual-fund-api \
     --source . \
     --region us-central1 \
     --allow-unauthenticated \
     --set-env-vars DATABASE_URL="postgresql://postgres:your-password@your-host:5432/your-database"
   ```

### Option 2: Using the Deployment Script

1. **Make the script executable**:
   ```bash
   chmod +x deploy.sh
   ```

2. **Set environment variables**:
   ```bash
   export DATABASE_URL="postgresql://postgres:your-password@your-host:5432/your-database"
   export PGUSER="postgres"
   export PGPASSWORD="your-password"
   export PGHOST="your-host"
   export PGPORT="5432"
   export PGDATABASE="your-database"
   ```

3. **Run deployment**:
   ```bash
   ./deploy.sh YOUR_PROJECT_ID us-central1
   ```

### Option 3: Manual Steps

1. **Enable APIs**:
   ```bash
   gcloud services enable run.googleapis.com
   gcloud services enable cloudbuild.googleapis.com
   ```

2. **Build container**:
   ```bash
   gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/mutual-fund-api
   ```

3. **Deploy to Cloud Run**:
   ```bash
   gcloud run deploy mutual-fund-api \
     --image gcr.io/YOUR_PROJECT_ID/mutual-fund-api \
     --region us-central1 \
     --platform managed \
     --allow-unauthenticated \
     --set-env-vars DATABASE_URL="YOUR_DATABASE_URL"
   ```

## Environment Variables

Set these environment variables for your deployment:

- `DATABASE_URL`: Your PostgreSQL connection string
- `PGUSER`: Database username
- `PGPASSWORD`: Database password
- `PGHOST`: Database host
- `PGPORT`: Database port (usually 5432)
- `PGDATABASE`: Database name

## Cost Estimation

**Google Cloud Run Pricing** (as of 2025):
- **Free tier**: 2 million requests/month
- **CPU**: $0.00002400 per vCPU-second
- **Memory**: $0.00000250 per GiB-second
- **Requests**: $0.40 per million requests

**Typical monthly costs**:
- Small API (< 100K requests): $0-5
- Medium API (< 1M requests): $5-25
- Large API (> 1M requests): $25-100

## Security Configuration

1. **Service Account**: Create a dedicated service account with minimal permissions
2. **VPC**: Connect to your Google Cloud SQL instance via private networking
3. **Secrets**: Use Google Secret Manager for sensitive data
4. **Authentication**: Add Cloud IAM for API access control

## Monitoring and Logging

1. **Cloud Monitoring**: Automatic metrics collection
2. **Cloud Logging**: Application logs in real-time
3. **Error Reporting**: Automatic error detection
4. **Cloud Trace**: Request tracing for performance

## Custom Domain Setup

1. **Map custom domain**:
   ```bash
   gcloud run domain-mappings create \
     --service mutual-fund-api \
     --domain api.yourdomain.com \
     --region us-central1
   ```

2. **Configure DNS**: Point your domain to Cloud Run

## CI/CD Pipeline

The included `cloudbuild.yaml` sets up automatic deployment:

1. **Connect to GitHub**: Link your repository
2. **Create trigger**: Deploy on push to main branch
3. **Set substitutions**: Configure environment variables

## Database Connection

Your app already connects to Google Cloud SQL. Ensure:

1. **Network access**: Cloud Run can reach your SQL instance
2. **Connection limits**: Monitor concurrent connections
3. **SSL**: Your connection uses SSL (already configured)

## Testing Deployment

After deployment, test your API:

```bash
# Get service URL
SERVICE_URL=$(gcloud run services describe mutual-fund-api --region=us-central1 --format="value(status.url)")

# Test endpoints
curl $SERVICE_URL/api/funds
curl $SERVICE_URL/api/funds/INF090I01239
```

## Troubleshooting

**Common issues**:

1. **Port binding**: Ensure your app uses `PORT` environment variable
2. **Database connection**: Check firewall and SSL settings
3. **Memory limits**: Increase if needed (default: 512Mi)
4. **Timeout**: Increase for long-running imports

**Logs**:
```bash
gcloud logs tail "projects/YOUR_PROJECT_ID/logs/run.googleapis.com%2Fstderr" --limit 50
```

## Production Optimizations

1. **Health checks**: Add `/health` endpoint
2. **Graceful shutdown**: Handle SIGTERM signals
3. **Connection pooling**: Configure SQLAlchemy pool settings
4. **Caching**: Add Redis for frequently accessed data
5. **Load balancing**: Use Cloud Load Balancer for high traffic

## Scaling Configuration

Cloud Run auto-scales based on:
- **Min instances**: 0 (scales to zero)
- **Max instances**: 100 (configurable)
- **Concurrency**: 80 requests per instance
- **CPU allocation**: Only during requests

## Support

- **Google Cloud Support**: https://cloud.google.com/support
- **Cloud Run Documentation**: https://cloud.google.com/run/docs
- **Pricing Calculator**: https://cloud.google.com/products/calculator