import os
import pandas as pd
from datetime import datetime

class FundDataCleaner:
    def __init__(self, data_directory):
        self.data_directory = data_directory
        self.processed_amcs = []  # AMCs with at least one fund processed
        self.unprocessed_amcs = []  # AMCs where no fund was processed
        self.processed_funds = 0  # Count of funds successfully processed
        self.unprocessed_funds = 0  # Count of funds failed
        self.processed_funds_list = []  # List of (AMC, Fund Name, File Name)
        self.unprocessed_funds_list = []  # List of (AMC, File Name)
        self.clean_date = datetime.now().strftime("%Y-%m-%d")  # Store clean date
        self.output_directory=data_directory

    def clean_data(self):
        for amc_name in os.listdir(self.data_directory):
            amc_path = os.path.join(self.data_directory, amc_name)

            if os.path.isdir(amc_path):  # Ensure it's a directory
                funds_processed_for_amc = 0  # Track funds per AMC
                
                for file in os.listdir(amc_path):
                    if file.endswith(".xlsx") or file.endswith(".xls"):
                        file_path = os.path.join(amc_path, file)
                        try:
                            xls = pd.ExcelFile(file_path)
                            for sheet_name in xls.sheet_names:
                                df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

                                # Clean the dataframe
                                cleaned_df = self._clean_dataframe(df)

                                if cleaned_df is not None and not cleaned_df.empty:
                                    print(f"✅ Processed {sheet_name} from {file}")
                                    print(cleaned_df.head())  # Show preview
                                    self.processed_funds += 1
                                    funds_processed_for_amc += 1
                                    self.processed_funds_list.append((amc_name, sheet_name, file))
                                else:
                                    self.unprocessed_funds += 1
                                    self.unprocessed_funds_list.append((amc_name, file))

                        except Exception as e:
                            print(f"❌ Error processing {file}: {e}")
                            self.unprocessed_funds += 1
                            self.unprocessed_funds_list.append((amc_name, file))
                
                if funds_processed_for_amc > 0:
                    self.processed_amcs.append(amc_name)
                else:
                    self.unprocessed_amcs.append(amc_name)

        # Print final summary
        self._print_summary()

    def _clean_dataframe(self, df):
        """
        Cleans and formats the extracted fund portfolio data.
        """
        table_index = self._find_table_indices(df)
        cleaned_data = []

        for (start_idx, end_idx, category) in table_index:
            if start_idx is not None and end_idx is not None:
                table_df = df.iloc[start_idx:end_idx].reset_index(drop=True)

                # Ensure we have at least 6 columns before renaming
                if table_df.shape[1] >= 7:
                    df_selected = table_df.iloc[:, 1:7].copy()
                    df_selected.columns = ["Name of the Instrument", "ISIN", "Rating / Industry", 
                                           "Quantity", "Market Value (Rs. in Lakhs)", "% to NAV"]
                    df_selected["Category"] = category
                    cleaned_data.append(df_selected)

        return pd.concat(cleaned_data, ignore_index=True) if cleaned_data else None

    def _find_table_indices(self, df):
        table_index = []
        start_idx = None
        start = False
        current_category = None

        # List of possible categories
        category_keywords = [
            "Debt Instruments", "REIT/InvIT Instruments", "Equity & Equity related",
            "Commercial Paper", "Certificate of Deposit", "Treasury Bill"
        ]

        for idx, row in df.iterrows():
            if isinstance(row[1], str) and any(keyword in row[1] for keyword in category_keywords):
                current_category = row[1]
                for next_idx in range(idx + 1, len(df)):
                    if pd.notna(df.iloc[next_idx, 2]):  # Ensure ISIN column is not None
                        start_idx = next_idx
                        start = True
                        break

            if isinstance(row[1], str) and row[1] == "Sub Total" and start:
                table_index.append((start_idx, idx, current_category))
                start = False
                start_idx = None

        return table_index

    def _print_summary(self):
        """
        Prints the summary of the cleaning process.
        """
        print("\n=== Cleaning Summary ===")
        print(f"🗓️ Clean Date: {self.clean_date}")
        print(f"🏦 Total AMCs Processed: {len(self.processed_amcs)}")
        print(f"❌ Total AMCs Unprocessed: {len(self.unprocessed_amcs)}")
        print(f"✅ Total Funds Processed: {self.processed_funds}")
        print(f"⚠️ Total Funds Unprocessed: {self.unprocessed_funds}")

        print("\n📌 Processed AMCs:")
        for amc in self.processed_amcs:
            print(f"   - {amc}")

        print("\n📜 Unprocessed AMCs:")
        for amc in self.unprocessed_amcs:
            print(f"   - {amc}")

        print("\n✅ Processed Funds List:")
        for amc, fund, file in self.processed_funds_list:
            print(f"   - AMC: {amc}, Fund: {fund}, File: {file}")

        print("\n❌ Unprocessed Funds List:")
        for amc, file in self.unprocessed_funds_list:
            print(f"   - AMC: {amc}, File: {file}")



    def save_summary_to_excel(self):
        """
        Saves the cleaning summary to an Excel file.
        """
        summary_file = os.path.join(self.output_directory, f"cleaning_summary_{self.clean_date}.xlsx")
        with pd.ExcelWriter(summary_file, engine="xlsxwriter") as writer:
            # Processed Funds
            df_processed = pd.DataFrame(self.processed_funds_list, columns=["AMC", "Fund Name", "File Name"])
            df_processed.to_excel(writer, sheet_name="Processed Funds", index=False)

            # Unprocessed Funds
            df_unprocessed = pd.DataFrame(self.unprocessed_funds_list, columns=["AMC", "File Name"])
            df_unprocessed.to_excel(writer, sheet_name="Unprocessed Funds", index=False)

            # Processed AMCs
            df_amcs_processed = pd.DataFrame(self.processed_amcs, columns=["Processed AMCs"])
            df_amcs_processed.to_excel(writer, sheet_name="Processed AMCs", index=False)

            # Unprocessed AMCs
            df_amcs_unprocessed = pd.DataFrame(self.unprocessed_amcs, columns=["Unprocessed AMCs"])
            df_amcs_unprocessed.to_excel(writer, sheet_name="Unprocessed AMCs", index=False)

        print(f"\n📁 Summary saved to: {summary_file}")

import pandas as pd

# Column mappings
COLUMN_MAPPING = {
    "Name of the Instrument": ["Name of the Instrument", "Name Of the Instrument / Issuer", "Instrument"],
    "ISIN": ["ISIN"],
    "Rating / Industry": ["Rating / Industry", "Industry+ /Rating", "Rating"],
    "Quantity": ["Quantity", "Qty"],
    "Market Value": ["Market Value (Rs. in Lakhs)", "Market/ Fair Value (Rs. in Lacs.)", "Market Value"],
    "% to NAV": ["% to NAV", "% to AUM"]
}

# List of possible categories
CATEGORY_KEYWORDS = [
    "Debt Instruments", "REIT/InvIT Instruments", "EQUITY & EQUITY RELATED", 
    "Commercial Paper", "Certificate of Deposit", "Treasury Bill"
]

def extract_fund_data(file_path):
    try:
        df_raw = pd.read_excel(file_path, sheet_name=None, header=None, dtype=str)  # Read all sheets as strings

        for sheet_name, sheet_df in df_raw.items():
            print(f"\nProcessing sheet: {sheet_name}")

            # Step 1: Detect ISIN row (Header Row)
            header_row_idx = None
            for index, row in sheet_df.iterrows():
                if any("ISIN" in str(val) for val in row.dropna() if isinstance(val, str)):
                    header_row_idx = index
                    break
            
            if header_row_idx is None:
                print(f"Skipping {sheet_name} (No ISIN header found)")
                continue

            print(f"Header found at row {header_row_idx + 1}, skipping {header_row_idx - 1} rows")

            # Step 2: Read the file again with correct header
            df_clean = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=header_row_idx - 1, dtype=str)
            df_clean.columns = df_clean.iloc[0]  # First row after skip becomes column headers
            df_clean = df_clean[1:].reset_index(drop=True)  # Remove duplicate header row
            
            # Step 3: Identify required columns dynamically
            selected_columns = {}
            for key, possible_names in COLUMN_MAPPING.items():
                for col in df_clean.columns:
                    if any(name in str(col) for name in possible_names):
                        selected_columns[key] = col
                        break
            
            if len(selected_columns) < 6:
                print(f"Skipping {sheet_name} (Missing required columns)")
                continue
            
            # Step 4: Extract only required columns
            df_selected = df_clean[list(selected_columns.values())]
            df_selected.columns = selected_columns.keys()  # Rename to standard column names
            
            # Step 5: Detect categories dynamically and assign row-wise
            current_category = None
            df_selected["Category"] = None  # Initialize Category column
            
            for idx, row in df_selected.iterrows():
                row_values = row.astype(str).dropna().values  # Convert row values to string and drop NaNs
                
                new_category = next((val for val in row_values if any(keyword in val for keyword in CATEGORY_KEYWORDS)), None)
                if new_category:
                    current_category = new_category  # Update category if found                
                df_selected.at[idx, "Category"] = current_category  # Assign category directly
            
            # Step 6: **Now remove rows without ISIN**
            df_selected = df_selected[df_selected["Name of the Instrument"].notna()]

            print(f"Extracted {len(df_selected)} rows from {sheet_name}:\n", df_selected.head(100))  # Debug output
            
    except Exception as e:
        print(f"Error processing file: {e}")

# Example usage:
amc_directory1 = "/Users/njp60/Documents/code/mutualfundbackend/funddata/data/HDFC Mutual Fund/Monthly HDFC Multi Cap Fund - 31 January 2025.xlsx"
amc_directory1 = "/Users/njp60/Documents/code/mutualfundbackend/funddata/data/ICICI Prudential Mutual Fund/ICICI Prudential Retirement Fund - Hybrid Aggressive Plan.xlsx"


extract_fund_data(amc_directory1)


