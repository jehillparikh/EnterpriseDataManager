# Mutual Fund API

A comprehensive RESTful API for mutual fund management, built with Flask and PostgreSQL. This API focuses exclusively on fund operations and tracking, including factsheets, portfolio management, returns analysis, and fund holdings tracking.

## Features

- **Fund Management**: Comprehensive fund data organization using ISIN as primary key
- **Factsheet Management**: Store and retrieve fund factsheets with key fund information
- **Returns Tracking**: Monitor fund performance with standardized return periods
- **Portfolio Holdings**: Track fund investments with detailed holding information
- **NAV History**: Historical NAV data with time series capabilities
- **Sector Analysis**: Get insights into fund holdings by sector allocation

## Installation

The project uses Poetry for dependency management. Set up the project with:

```bash
# Clone the repository
git clone https://github.com/yourusername/mutual-fund-api.git
cd mutual-fund-api

# Install dependencies with Poetry
poetry install

# Or using pip
pip install -r requirements.txt
```

## Configuration

Set your PostgreSQL database URL in the environment:

```bash
export DATABASE_URL=postgresql://username:password@localhost:5432/mutual_fund_db
```

## Running the API

Start the application with:

```bash
# Using Poetry
poetry run python main.py

# Or directly
python main.py

# For production with gunicorn
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

The API will be available at http://localhost:5000/

## API Documentation

### Base URL

```
/api
```

### Fund Endpoints

#### List Funds

```
GET /funds
```

Query Parameters:
- `amc_name` (optional): Filter by AMC name
- `fund_type` (optional): Filter by fund type
- `page` (optional, default: 1): Page number
- `per_page` (optional, default: 20): Items per page

#### Get Fund Details

```
GET /funds/{isin}
```

#### Get Fund Factsheet

```
GET /funds/{isin}/factsheet
```

#### Get Fund Returns

```
GET /funds/{isin}/returns
```

#### Get Fund Holdings

```
GET /funds/{isin}/holdings
```

Query Parameters:
- `page` (optional, default: 1): Page number
- `per_page` (optional, default: 20): Items per page

#### Get NAV History

```
GET /funds/{isin}/nav
```

Query Parameters:
- `start_date` (optional): Filter by start date (YYYY-MM-DD)
- `end_date` (optional): Filter by end date (YYYY-MM-DD)
- `page` (optional, default: 1): Page number
- `per_page` (optional, default: 30): Items per page

#### Get All Fund Data

```
GET /funds/{isin}/all
```

Returns basic fund information, factsheet, returns, and the most recent NAV in a single request.

#### Get Complete Fund Data with Sector Analysis

```
GET /funds/{isin}/complete
```

Returns comprehensive fund data including:
- Fund basic information
- Factsheet details
- Returns data
- Latest NAV
- NAV history (last 30 days)
- Portfolio holdings
- Sector analysis with top sectors
- Asset allocation breakdown (equity, debt, cash, other)

### Example Response (Complete Fund Data)

```json
{
  "fund": {
    "isin": "TEST00000001",
    "scheme_name": "Test HDFC Balanced Advantage Fund",
    "fund_type": "Hybrid",
    "fund_subtype": "Balanced Advantage",
    "amc_name": "HDFC",
    "created_at": "2025-04-17T03:41:26.955936",
    "updated_at": "2025-04-17T03:41:26.955940"
  },
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
  },
  "nav_history": [
    {
      "date": "2025-04-17",
      "nav": 104.1682
    },
    {
      "date": "2025-04-16",
      "nav": 104.1582
    }
  ],
  "holdings": [
    {
      "instrument_name": "HDFC Bank Ltd",
      "instrument_type": "Equity",
      "sector": "Financial Services",
      "percentage_to_nav": 12.5,
      "quantity": 12500.0,
      "value": 2500000.0,
      "coupon": null,
      "yield_value": null
    }
  ],
  "analysis": {
    "top_sectors": [
      {
        "sector": "Financial Services",
        "allocation": 16.5
      },
      {
        "sector": "Information Technology",
        "allocation": 16.5
      }
    ],
    "asset_allocation": {
      "equity": 39.0,
      "debt": 10.0,
      "cash": 10.0,
      "other": 0.0
    }
  }
}
```

## Data Import Utilities

The project includes utilities for importing fund data from Excel files:

- `import_basic_data.py`: Import fund and factsheet data
- `import_portfolio_nav.py`: Import portfolio holdings and NAV history
- `import_test_holdings.py`: Create sample test data

## Database Schema

The API uses the following database structure:

### Fund
- Primary key: ISIN (12-character string)
- Attributes: scheme_name, fund_type, fund_subtype, amc_name, created_at, updated_at

### Fund Factsheet
- Foreign key: ISIN references Fund
- Attributes: fund_manager, aum, expense_ratio, launch_date, exit_load, last_updated

### Fund Returns
- Foreign key: ISIN references Fund
- Attributes: return_1m, return_3m, return_6m, return_ytd, return_1y, return_3y, return_5y, last_updated

### Portfolio Holdings
- Primary key: id (auto-increment)
- Foreign key: ISIN references Fund
- Attributes: instrument_name, instrument_type, sector, percentage_to_nav, quantity, value, coupon, yield_value, instrument_isin, last_updated

### NAV History
- Primary key: id (auto-increment)
- Foreign key: ISIN references Fund
- Attributes: date, nav

## Implementation Notes

- All money values are in INR (₹)
- Returns are stored as percentage values (e.g., 10.5 for 10.5%)
- NAV values represent the value per unit of the fund
- Dates are in ISO format (YYYY-MM-DD)

## License

This project is licensed under the MIT License - see the LICENSE file for details.