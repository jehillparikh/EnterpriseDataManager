# Mutual Fund API Documentation

## Overview

This API provides comprehensive data for mutual funds using ISIN as the primary identifier. It includes factsheets, returns data, portfolio holdings, and NAV history.

## Base URL

```
http://localhost:5000/api
```

## Authentication

No authentication is currently required. All endpoints are publicly accessible.

## Endpoints

### Get All Funds

Retrieves a list of all available funds with optional filtering capabilities.

```
GET /funds
```

**Query Parameters:**
- `amc` (optional): Filter funds by AMC name (e.g., `HDFC`, `ICICI`)
- `type` (optional): Filter funds by fund type (e.g., `Equity`, `Hybrid`, `Debt`)
- `page` (optional): Page number for pagination (default: 1)
- `per_page` (optional): Items per page (default: 20)

**Response Format:**
```json
{
  "funds": [
    {
      "isin": "TEST00000001",
      "scheme_name": "Test HDFC Balanced Advantage Fund",
      "fund_type": "Hybrid",
      "fund_subtype": "Balanced Advantage",
      "amc_name": "HDFC"
    }
  ],
  "pagination": {
    "current_page": 1,
    "per_page": 20,
    "total_items": 1,
    "total_pages": 1
  }
}
```

### Get Fund Details

Retrieves basic details for a specific fund.

```
GET /funds/{isin}
```

**Response Format:**
```json
{
  "isin": "TEST00000001",
  "scheme_name": "Test HDFC Balanced Advantage Fund",
  "fund_type": "Hybrid",
  "fund_subtype": "Balanced Advantage",
  "amc_name": "HDFC",
  "created_at": "2025-04-17T03:41:26.955936",
  "updated_at": "2025-04-17T03:41:26.955940"
}
```

### Get Fund Factsheet

Retrieves factsheet information for a specific fund.

```
GET /funds/{isin}/factsheet
```

**Response Format:**
```json
{
  "isin": "TEST00000001",
  "fund_manager": "Test Manager",
  "aum": 10000.0,
  "expense_ratio": 0.01,
  "launch_date": "2020-01-01",
  "exit_load": "1% for less than 1 year",
  "last_updated": "2025-04-17T03:41:27.118692"
}
```

### Get Fund Returns

Retrieves performance metrics for a specific fund.

```
GET /funds/{isin}/returns
```

**Response Format:**
```json
{
  "isin": "TEST00000001",
  "return_1m": 1.5,
  "return_3m": 4.2,
  "return_6m": 8.7,
  "return_ytd": 5.3,
  "return_1y": 12.1,
  "return_3y": 36.5,
  "return_5y": 65.8,
  "last_updated": "2025-04-17T03:41:27.281429"
}
```

### Get Fund Portfolio Holdings

Retrieves portfolio holdings for a specific fund.

```
GET /funds/{isin}/holdings
```

**Query Parameters:**
- `sector` (optional): Filter by sector
- `type` (optional): Filter by instrument type (e.g., `Equity`, `Debt`, `Cash`)
- `page` (optional): Page number for pagination (default: 1)
- `per_page` (optional): Items per page (default: 20)

**Response Format:**
```json
{
  "holdings": [
    {
      "id": 1,
      "instrument_name": "HDFC Bank Ltd",
      "instrument_type": "Equity",
      "sector": "Financial Services",
      "percentage_to_nav": 12.5,
      "quantity": 12500.0,
      "value": 2500000.0,
      "coupon": null,
      "yield_value": null,
      "instrument_isin": null,
      "last_updated": "2025-04-17T03:43:54.720554"
    }
  ],
  "pagination": {
    "current_page": 1,
    "per_page": 20,
    "total_items": 7,
    "total_pages": 1
  }
}
```

### Get Fund NAV History

Retrieves NAV history for a specific fund.

```
GET /funds/{isin}/nav
```

**Query Parameters:**
- `start_date` (optional): Filter NAV from this date (format: YYYY-MM-DD)
- `end_date` (optional): Filter NAV until this date (format: YYYY-MM-DD)
- `page` (optional): Page number for pagination (default: 1)
- `per_page` (optional): Items per page (default: 30)

**Response Format:**
```json
{
  "nav_history": [
    {
      "id": 262,
      "date": "2025-04-17",
      "nav": 104.1682
    },
    {
      "id": 261,
      "date": "2025-04-16",
      "nav": 104.1582
    }
  ],
  "pagination": {
    "current_page": 1,
    "per_page": 30,
    "total_items": 262,
    "total_pages": 9
  }
}
```

### Get All Fund Data

Retrieves all available data for a specific fund in a single request.

```
GET /funds/{isin}/all
```

**Response Format:**
```json
{
  "isin": "TEST00000001",
  "scheme_name": "Test HDFC Balanced Advantage Fund",
  "fund_type": "Hybrid",
  "fund_subtype": "Balanced Advantage",
  "amc_name": "HDFC",
  "created_at": "2025-04-17T03:41:26.955936",
  "updated_at": "2025-04-17T03:41:26.955940",
  "factsheet": {
    "fund_manager": "Test Manager",
    "aum": 10000.0,
    "expense_ratio": 0.01,
    "launch_date": "2020-01-01",
    "exit_load": "1% for less than 1 year",
    "last_updated": "2025-04-17T03:41:27.118692"
  },
  "returns": {
    "return_1m": 1.5,
    "return_3m": 4.2,
    "return_6m": 8.7,
    "return_ytd": 5.3,
    "return_1y": 12.1,
    "return_3y": 36.5,
    "return_5y": 65.8,
    "last_updated": "2025-04-17T03:41:27.281429"
  },
  "latest_nav": {
    "date": "2025-04-17",
    "nav": 104.1682
  }
}
```

## Error Responses

The API returns appropriate HTTP status codes:

- `200 OK`: Request successful
- `404 Not Found`: Fund not found
- `400 Bad Request`: Invalid parameters
- `500 Internal Server Error`: Server-side error

Error responses include a JSON object with an error message:

```json
{
  "error": "Fund with ISIN 'INVALID123456' not found"
}
```

## Database Schema

The API uses the following database structure:

### Fund (mf_fund)
- Primary key: ISIN (12-character string)
- Attributes: scheme_name, fund_type, fund_subtype, amc_name, created_at, updated_at

### Fund Factsheet (mf_factsheet)
- Foreign key: ISIN references Fund
- Attributes: fund_manager, aum, expense_ratio, launch_date, exit_load, last_updated

### Fund Returns (mf_returns)
- Foreign key: ISIN references Fund
- Attributes: return_1m, return_3m, return_6m, return_ytd, return_1y, return_3y, return_5y, last_updated

### Portfolio Holdings (mf_portfolio_holdings)
- Primary key: id (auto-increment)
- Foreign key: ISIN references Fund
- Attributes: instrument_name, instrument_type, sector, percentage_to_nav, quantity, value, coupon, yield_value, instrument_isin, last_updated

### NAV History (mf_nav_history)
- Primary key: id (auto-increment)
- Foreign key: ISIN references Fund
- Attributes: date, nav

## Data Import Utilities

The API includes utilities for importing fund data from Excel files:

- `import_basic_data.py`: Imports fund and factsheet data
- `import_portfolio_nav.py`: Imports portfolio holdings and NAV history
- `import_test_holdings.py`: Creates sample test data

## Implementation Notes

- All money values are in INR (₹)
- Returns are stored as percentage values (e.g., 10.5 for 10.5%)
- NAV values represent the value per unit of the fund
- Dates are in ISO format (YYYY-MM-DD)