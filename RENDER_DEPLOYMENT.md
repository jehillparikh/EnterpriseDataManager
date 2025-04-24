# Deploying to Render.com

This guide explains how to deploy the Mutual Fund API to Render.com.

## Prerequisites

1. A Render.com account
2. Access to the PostgreSQL database credentials

## Steps for Deployment

### Option 1: Using the .env File (Recommended)

1. Log in to your Render.com account
2. Click on "New" and select "Web Service"
3. Connect your repository (GitHub, GitLab, etc.)
4. Use the following settings:
   - Name: mutual-fund-api (or your preferred name)
   - Environment: Python
   - Build Command: `curl -sSL https://install.python-poetry.org | python3 - && $HOME/.local/bin/poetry install --no-interaction --no-ansi`
   - Start Command: `gunicorn --bind 0.0.0.0:$PORT --reuse-port main:app`

5. Click "Create Web Service"

The application includes a `.env` file that Render will automatically detect and use for environment variables.

### Option 2: Manual Environment Variable Setup

If you prefer to set up environment variables manually:

1. Log in to your Render.com account
2. Click on "New" and select "Web Service"
3. Connect your repository (GitHub, GitLab, etc.)
4. Use the following settings:
   - Name: mutual-fund-api (or your preferred name)
   - Environment: Python
   - Build Command: `curl -sSL https://install.python-poetry.org | python3 - && $HOME/.local/bin/poetry install --no-interaction --no-ansi`
   - Start Command: `gunicorn --bind 0.0.0.0:$PORT --reuse-port main:app`

5. Add the following environment variables:
   - `DATABASE_URL`: Your PostgreSQL connection string
   - `PYTHON_VERSION`: 3.11.1
   - `POETRY_VIRTUALENVS_CREATE`: false

6. Click "Create Web Service"

## Verifying the Deployment

Once deployed, you can check if your API is working by visiting:

- Homepage: `https://your-app-name.onrender.com/`
- API Documentation: `https://your-app-name.onrender.com/docs`
- API Endpoint: `https://your-app-name.onrender.com/api/funds`

## Troubleshooting

If you encounter the error "Either 'SQLALCHEMY_DATABASE_URI' or 'SQLALCHEMY_BINDS' must be set":

1. Make sure the `DATABASE_URL` environment variable is set in the Render dashboard
2. Check if the application can access the environment variable
3. Verify that your database is accessible from Render.com

## Notes

- The application uses Poetry for dependency management
- The `render.yaml` file in the repository provides a configuration for Render.com
- You can also deploy using the Render Blueprint functionality