# 000-LOG-Equipment-Automation
Google Gemini Chat: https://gemini.google.com/share/50686165d870

# Equipment Data Automator

## Description
This desktop application automates the reconciliation of UCSD equipment inventory data. It provides a graphical interface to ingest monthly equipment reports, match them against Master Part and PO records, and generate formatted Excel files for system import.

## Setup
1. **Install Dependencies:**
   `pip install pandas xlsxwriter openpyxl`
2. **Run the App:**
   `python equipment_automator.py`

## Input Requirements
The application requires three specific files. Green checkmarks in the GUI indicate a file has been loaded successfully.

* **00 Equipment File:** The raw monthly download (Excel or CSV). The script automatically scans for the header "ACCOUNT NO." and stops reading when it hits a blank row, ensuring headers and footers are ignored.
* **01 Master Parts:** Must contain "Part No." and "Equipment" columns.
* **02 Master POs:** Must contain "PO", "Account", "Quantity", "Part", and "Equipment" columns.

## Output Files
The script generates five Excel files in your selected output folder, dated with the current day:

1. **Equipment Import:** Contains unique records ready for import. This includes originally unique lines and duplicates that resolved to a single line after processing.
2. **STD Import:** A summary of Tab 2 from the source file, grouped by Account and Part Number.
3. **Duplicate Import (Pass):** Duplicate records where the calculated quantity matches the Master PO file.
4. **Duplicate ERRORS (Mismatch):** Duplicate records where quantities did not match the Master PO file. These require manual review.
5. **Credits:** Extracted records with negative rates.

## Key Logic Notes
* **Part Numbers:** The script enforces a 4-digit format (e.g., converts "123" to "0123") to prevent Excel from stripping leading zeros.
* **Processing:** Data transformation runs on a background thread to keep the application responsive.