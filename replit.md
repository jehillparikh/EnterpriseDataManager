# Mutual Fund API

## Overview

This is a comprehensive mutual fund management API built with Flask and PostgreSQL. The application provides RESTful endpoints for managing mutual funds, factsheets, returns data, portfolio holdings, and NAV history. The system is designed around ISIN (International Securities Identification Number) as the primary identifier for all fund-related data.

## System Architecture

### Backend Architecture
- **Framework**: Flask (Python 3.11+)
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Authentication**: JWT-based authentication system
- **Deployment**: Gunicorn WSGI server with auto-scaling support
- **Dependencies**: Poetry for dependency management

### Database Design
The application uses a PostgreSQL database with the following key architectural decisions:
- **ISIN-centric design**: All fund data is organized around ISIN codes as primary keys
- **Normalized schema**: Separate tables for funds, factsheets, returns, holdings, and NAV history
- **Relationship mapping**: SQLAlchemy relationships for efficient data access
- **Indexing strategy**: Optimized indexes on frequently queried fields (AMC name, fund type)

### API Architecture
- **RESTful design**: Standard HTTP methods and status codes
- **Modular structure**: Separate blueprints for different API domains
- **Service layer**: Business logic separated from API controllers
- **Schema validation**: Marshmallow for request/response validation
- **Error handling**: Centralized exception handling with custom error types

## Key Components

### Models (`new_models_updated.py`)
- **Fund**: Core fund information with ISIN as primary key
- **FundFactSheet**: Detailed fund information (AUM, expense ratio, fund manager)
- **FundReturns**: Performance data across multiple time periods
- **PortfolioHolding**: Fund portfolio composition and holdings
- **NavHistory**: Historical Net Asset Value data

### Services (`new_services.py`)
- **FundService**: CRUD operations for fund data
- **FactSheetService**: Factsheet management
- **ReturnsService**: Returns data management
- **PortfolioService**: Portfolio holdings management
- **NavHistoryService**: NAV history operations

### API Endpoints (`fund_api.py`)
- **GET /api/funds**: List funds with filtering and pagination
- **Fund-specific endpoints**: CRUD operations for all fund-related data
- **Bulk operations**: Support for importing large datasets

### Data Import System (`data/`)
- **FundDataImporter**: Centralized Excel data import functionality
- **Bulk processing**: Efficient handling of large datasets
- **Error handling**: Comprehensive logging and rollback capabilities
- **File formats**: Support for multiple Excel file formats

## Data Flow

### Fund Data Management
1. **Data Import**: Excel files imported via FundDataImporter
2. **Validation**: Schema validation using Marshmallow
3. **Storage**: PostgreSQL with optimized indexes
4. **API Access**: RESTful endpoints with filtering and pagination

### Authentication Flow
1. **User Registration**: JWT token generation
2. **Token Validation**: Middleware-based authentication
3. **Protected Routes**: Token-required decorators

### Database Transactions
1. **ACID Compliance**: All operations wrapped in transactions
2. **Rollback Support**: Error handling with automatic rollback
3. **Bulk Operations**: Optimized for large dataset imports

## External Dependencies

### Core Dependencies
- **Flask**: Web framework and routing
- **SQLAlchemy**: ORM and database abstraction
- **Marshmallow**: Schema validation and serialization
- **PyJWT**: JSON Web Token implementation
- **Psycopg2**: PostgreSQL database adapter
- **Pandas**: Excel file processing and data manipulation
- **Openpyxl**: Excel file reading capabilities
- **Gunicorn**: Production WSGI server

### Development Dependencies
- **Pytest**: Testing framework
- **Black**: Code formatting
- **Poetry**: Dependency management

### External Services
- **PostgreSQL Database**: Google Cloud SQL (migrated from Render.com)
- **File Storage**: Local file system for Excel uploads

## Deployment Strategy

### Production Deployment
- **Platform**: Render.com with auto-scaling
- **Server**: Gunicorn with multiple workers
- **Database**: Managed PostgreSQL instance
- **Environment**: Production-optimized configuration

### Development Setup
- **Local Development**: Flask development server
- **Database**: PostgreSQL connection via environment variables
- **File Processing**: Local Excel file handling

### Configuration Management
- **Environment Variables**: Database URLs, JWT secrets
- **Config Files**: Application-specific settings
- **Deployment Files**: render.yaml for Render.com deployment

### Monitoring and Logging
- **Application Logging**: Structured logging throughout the application
- **Import Tracking**: Status tracking for background processes
- **Error Handling**: Comprehensive exception management

## Changelog

- June 27, 2025: Initial setup
- June 27, 2025: Successfully migrated database from Render.com PostgreSQL to Google Cloud SQL
  - Updated connection configuration to prioritize Google Cloud database
  - Added validation for database connection strings
  - Verified all tables and data migrated successfully (86 funds, 12 factsheets, 80 holdings, 30 NAV records)
- June 27, 2025: Completed Google Cloud SQL integration
  - Configured IP whitelist (34.82.231.20/32) in Google Cloud SQL authorized networks
  - Created mutualfundpro database in Google Cloud SQL instance
  - Successfully established live connection to Google Cloud SQL
  - Application now fully operational on Google Cloud infrastructure
- June 27, 2025: Enhanced portfolio upload system
  - Updated to use new column structure with Scheme ISIN for fund linking
  - Added support for columns: Name of Instrument, ISIN, Coupon, Industry, Quantity, Market Value, % to Net Assets, Yield, Type, AMC, Scheme Name, Scheme ISIN
  - Improved fund matching accuracy using direct ISIN lookup
- June 27, 2025: Completed PortfolioHolding to FundHolding model migration
  - Renamed model to better reflect fund holdings vs future user portfolios
  - Updated all references across services, APIs, and data import functionality
  - Added robust ISIN validation to skip invalid/NaN values in portfolio uploads
- June 27, 2025: Removed fallback database logic
  - Application now requires Google Cloud SQL connection to operate
  - Removed all fallback to local database functionality per user preference
  - Enhanced error handling to stop application if Google Cloud SQL connection fails

## User Preferences

Preferred communication style: Simple, everyday language.