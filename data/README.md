# Mutual Fund Data Import Module

This module provides a comprehensive solution for importing mutual fund data from Excel files into the database.

## Key Features

- Single consolidated class `FundDataImporter` to handle all data imports
- ISIN-based fund identification across all data sources
- Bulk import capabilities with efficient database transactions
- Detailed logging and statistics
- Support for clearing existing data before import
- Flexible command-line interface

## Excel Data Files

The importer works with the following Excel files located in the `attached_assets` directory:

1. **factsheet_testdata.xlsx**: Contains basic fund information and factsheets
   - ISIN, Scheme Name, Fund Type, Fund Subtype, Fund Manager(s), AUM, etc.

2. **returns_testdata.xlsx**: Contains fund returns data
   - ISIN, 1M Return, 3M Return, 6M Return, 1Y Return, etc.

3. **mutual_portfolio_testdata.xlsx**: Contains fund portfolio holdings
   - ISIN, Name Of the Instrument, Sector, Quantity, % to NAV, etc.

4. **navtimeseries_testdata.xlsx**: Contains NAV history
   - ISIN, Scheme Name, Date, Net Asset Value

## Usage

### Importing All Data

To import all data at once:

```bash
python -m data.import_all
```

### Using the Python API

```python
from data.fund_data_importer import FundDataImporter

importer = FundDataImporter()

# Import all data
stats = importer.import_all_data(clear_existing=True)

# Or import specific data
importer.import_factsheet_data()
importer.import_returns_data()
importer.import_portfolio_data()
importer.import_nav_data()
```

### Command-Line Interface

The `fund_data_importer.py` script also provides a command-line interface:

```bash
python -m data.fund_data_importer --all --clear    # Import all data, clearing existing first
python -m data.fund_data_importer --factsheet      # Import only factsheet data
python -m data.fund_data_importer --returns        # Import only returns data
python -m data.fund_data_importer --portfolio      # Import only portfolio data
python -m data.fund_data_importer --nav            # Import only NAV data
```