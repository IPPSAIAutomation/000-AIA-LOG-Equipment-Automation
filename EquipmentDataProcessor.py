import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import threading
import queue
import os
import datetime
from typing import Tuple, Optional, List

# --------------------------------------------------------------------------------
# Business Logic Layer
# --------------------------------------------------------------------------------

class DataProcessor:
    """Handles all data ingestion, transformation, and business logic execution.
    
    Attributes:
        files (dict): Dictionary storing file paths for inputs.
        output_folder (str): Path to the destination folder.
        log_queue (queue.Queue): Queue to send status messages back to the GUI.
    """

    def __init__(self, files: dict, output_folder: str, log_queue: queue.Queue):
        self.files = files
        self.output_folder = output_folder
        self.log_queue = log_queue
        self.today_str = datetime.datetime.now().strftime("%Y%m%d")

    def log(self, message: str):
        """Helper to send messages to the GUI thread."""
        self.log_queue.put(("status", message))

    def error(self, message: str):
        """Helper to send error messages to the GUI thread."""
        self.log_queue.put(("error", message))

    def load_excel_with_stop_condition(self, file_path: str, sheet_index: int, 
                                     col_stop_trigger: str = "ACCOUNT NO.") -> pd.DataFrame:
        """Loads Excel data, finding a specific header and stopping at the first blank row.
        
        Why: The user requirement specifies that data doesn't start at row 0 and 
        must stop processing when a specific column becomes blank, ignoring footer data.
        """
        try:
            # Determine engine based on extension to support the CSVs provided in context 
            # while prioritizing the requested Excel logic.
            if file_path.lower().endswith('.csv'):
                self.log(f"Detected CSV for {os.path.basename(file_path)}, ignoring sheet index.")
                df_raw = pd.read_csv(file_path)
            else:
                # Read full sheet to find structure
                df_raw = pd.read_excel(file_path, sheet_name=sheet_index, header=None)

            # 1. Find the header row index looking for specific text
            header_row_idx = None
            for idx, row in df_raw.iterrows():
                # Convert row to string and check if trigger exists
                if row.astype(str).str.contains(col_stop_trigger, case=False, na=False).any():
                    header_row_idx = idx
                    break
            
            if header_row_idx is None:
                # Fallback: maybe the file is already clean?
                if col_stop_trigger in df_raw.columns:
                    df_clean = df_raw
                else:
                    raise ValueError(f"Could not find header row containing '{col_stop_trigger}'")
            else:
                # Set header
                df_raw.columns = df_raw.iloc[header_row_idx]
                df_clean = df_raw.iloc[header_row_idx + 1:].reset_index(drop=True)

            # 2. Stop at first blank row in the trigger column
            # Clean column name references to ensure we match 'ACCOUNT NO.' exactly if there's whitespace
            df_clean.columns = df_clean.columns.astype(str).str.strip()
            
            if col_stop_trigger not in df_clean.columns:
                 # Try finding it with case insensitivity if exact match failed
                 matches = [c for c in df_clean.columns if c.upper() == col_stop_trigger.upper()]
                 if matches: 
                     col_stop_trigger = matches[0]

            # Identify the index of the first null/empty value in the key column
            # We treat empty strings or NaNs as the stop signal
            is_null = df_clean[col_stop_trigger].isnull() | (df_clean[col_stop_trigger].astype(str).str.strip() == "")
            if is_null.any():
                first_blank_idx = is_null.idxmax() # idxmax returns the first True index
                df_clean = df_clean.loc[:first_blank_idx-1]
            
            return df_clean

        except Exception as e:
            raise RuntimeError(f"Error loading {os.path.basename(file_path)}: {str(e)}")

    def normalize_part_number(self, df: pd.DataFrame, col_name: str) -> pd.DataFrame:
        """Ensures part numbers are 4-digit strings with leading zeros.
        
        Why: Excel often strips leading zeros (e.g., 0123 -> 123). 
        This enforces '0123' format for accurate merging and output.
        """
        if col_name in df.columns:
            # Convert to numeric first to handle "123.0", then to int, then string zfill
            # Using apply ensures we handle mixed types gracefully
            def clean_part(x):
                try:
                    return str(int(float(x))).zfill(4)
                except (ValueError, TypeError):
                    return str(x).zfill(4)
            
            df[col_name] = df[col_name].apply(clean_part)
        return df

    def run(self):
        """Main execution flow for business logic."""
        try:
            self.log("Starting processing...")

            # ---------------------------------------------------------
            # 1. Load Reference Files
            # ---------------------------------------------------------
            self.log("Loading Master Part Numbers...")
            df_parts = pd.read_excel(self.files['01']) if self.files['01'].endswith('.xlsx') else pd.read_csv(self.files['01'])
            # Normalize part numbers in master file
            df_parts = self.normalize_part_number(df_parts, "Part No.")

            self.log("Loading PO Account Master...")
            df_po = pd.read_excel(self.files['02']) if self.files['02'].endswith('.xlsx') else pd.read_csv(self.files['02'])
            df_po = self.normalize_part_number(df_po, "Part")

            # ---------------------------------------------------------
            # 2. Initial Data Manipulation (df_start)
            # ---------------------------------------------------------
            self.log("Loading Equipment File (Tab 0)...")
            df_equip_raw = self.load_excel_with_stop_condition(self.files['00'], sheet_index=0)

            # Ensure numeric columns are actually numeric for filtering
            df_equip_raw['RATE'] = pd.to_numeric(df_equip_raw['RATE'], errors='coerce').fillna(0)
            
            # Logic: Remove negative rates (Save for Output 5)
            df_neg_rates = df_equip_raw[df_equip_raw['RATE'] < 0].copy()
            df_start = df_equip_raw[df_equip_raw['RATE'] >= 0].copy()

            # Logic: Remove specific equipment
            df_start = df_start[df_start['EQUIP'] != "H/C  Filtration -Countertop Unit"]

            # ---------------------------------------------------------
            # 3. Output 1 Logic: Equipment Import
            # ---------------------------------------------------------
            self.log("Processing Output 1 (Equipment Import)...")
            
            # Keep specific columns
            cols_req = ['ACCOUNT NO.', 'EQUIP', 'QTY']
            # Sanitize column names
            df_start.columns = [c.strip() for c in df_start.columns]
            df_out1_base = df_start[cols_req].copy()

            # Identify Unique vs Duplicate Accounts
            # We use keep=False to mark ALL duplicates as True
            is_dup = df_out1_base.duplicated(subset=['ACCOUNT NO.'], keep=False)
            df_01_unique = df_out1_base[~is_dup].copy()
            df_02_duplicate = df_out1_base[is_dup].copy()

            # Merge Part Numbers
            # Left merge using EQUIP (Main) and Equipment (Master)
            def apply_part_merge(target_df):
                merged = pd.merge(target_df, df_parts[['Equipment', 'Part No.']], 
                                  left_on='EQUIP', right_on='Equipment', how='left')
                merged = merged.drop(columns=['Equipment']) # Clean up key
                return self.normalize_part_number(merged, "Part No.")

            df_01_unique = apply_part_merge(df_01_unique)
            df_02_duplicate = apply_part_merge(df_02_duplicate)

            # Logic: "Look again" at duplicates. If they are now unique (single row per account), move to unique.
            # This happens if an account had 2 rows, but we are just looking at existence here.
            # The requirement: "If they have a single row, then append them to the 01_df_unique"
            dup_counts = df_02_duplicate['ACCOUNT NO.'].value_counts()
            single_entry_accounts = dup_counts[dup_counts == 1].index

            # Move singles to unique df
            rows_to_move = df_02_duplicate[df_02_duplicate['ACCOUNT NO.'].isin(single_entry_accounts)]
            df_01_unique = pd.concat([df_01_unique, rows_to_move], ignore_index=True)
            
            # Remove them from duplicate df
            df_02_duplicate = df_02_duplicate[~df_02_duplicate['ACCOUNT NO.'].isin(single_entry_accounts)]

            self.save_excel(df_01_unique, "UCSD EQUIPMENT Equipment Import")

            # ---------------------------------------------------------
            # 4. Output 2 Logic: STD Import
            # ---------------------------------------------------------
            self.log("Processing Output 2 (STD Import)...")
            
            df_tab2 = self.load_excel_with_stop_condition(self.files['00'], sheet_index=2)
            df_tab2.columns = [c.strip() for c in df_tab2.columns] # Clean headers
            df_tab2 = df_tab2[['ACCOUNT NO.', 'EQUIP', 'QTY']].copy()
            
            # Merge Parts
            df_tab2 = apply_part_merge(df_tab2)
            
            # Summarize: Group by Account, sum QTY
            # First ensure QTY is numeric
            df_tab2['QTY'] = pd.to_numeric(df_tab2['QTY'], errors='coerce').fillna(0)
            
            # Note: Grouping by Account means we might lose 'EQUIP' details if they vary. 
            # However, prompt says "Summarize... final output has columns ACCOUNT NO, EQUIP, QTY, Part No".
            # Usually grouping by Account + Part + Equip is safer to preserve data.
            df_out2 = df_tab2.groupby(['ACCOUNT NO.', 'EQUIP', 'Part No.'], as_index=False)['QTY'].sum()
            
            # Reorder columns
            df_out2 = df_out2[['ACCOUNT NO.', 'EQUIP', 'QTY', 'Part No.']]
            self.save_excel(df_out2, "UCSD EQUIPMENT STD Import")

            # ---------------------------------------------------------
            # 5. Output 3 & 4 Logic: Duplicate Processing
            # ---------------------------------------------------------
            self.log("Processing Output 3 & 4 (Duplicate Analysis)...")

            # Keep only specific columns from the Duplicate DF created in Step 1
            df_dup_proc = df_02_duplicate[['ACCOUNT NO.', 'EQUIP', 'QTY', 'Part No.']].copy()
            df_dup_proc['QTY'] = pd.to_numeric(df_dup_proc['QTY'], errors='coerce').fillna(0)

            # Summarize by Account and Part, Sum QTY
            df_dup_sum = df_dup_proc.groupby(['ACCOUNT NO.', 'Part No.'], as_index=False)['QTY'].sum()

            # Merge with [02 Master File PO Account]
            # Merge Keys: Account = Account No., Part = Part No.
            # PO File columns: PO, Account, Equipment, Quantity, Part
            
            # Prepare PO DF for merge (renaming for easier merge)
            df_po_merge = df_po[['PO', 'Account', 'Quantity', 'Part', 'Equipment']].copy()
            df_po_merge.rename(columns={
                'Quantity': 'Quantity_02_File',
                'Account': 'Account_Ref',
                'Part': 'Part_Ref',
                'Equipment': 'Equipment_Ref' # Renaming to avoid collision
            }, inplace=True)

            merged_dups = pd.merge(
                df_dup_sum,
                df_po_merge,
                left_on=['ACCOUNT NO.', 'Part No.'],
                right_on=['Account_Ref', 'Part_Ref'],
                how='left'
            )

            # Clean QTY for comparison
            merged_dups['Quantity_02_File'] = pd.to_numeric(merged_dups['Quantity_02_File'], errors='coerce').fillna(0)
            
            # Comparison Logic
            # "Pass" if QTY == Quantity_02_File, else "Mismatch"
            merged_dups['Status'] = merged_dups.apply(
                lambda row: 'Pass' if row['QTY'] == row['Quantity_02_File'] else 'Mismatch', axis=1
            )

            # --- Output 3: Pass ---
            df_pass = merged_dups[merged_dups['Status'] == 'Pass'].copy()
            # Cols: PO, Account, Equipment, Quantity, Part
            # Mapping back: Account -> ACCOUNT NO., Quantity -> QTY, Part -> Part No.
            # But prompt says "Keep only columns PO, Account, Equipment, Quantity, Part"
            # It implies taking the data structure roughly from the PO file side or the combined side.
            # I will construct the final DF based on the available data.
            
            df_out3 = pd.DataFrame()
            df_out3['PO'] = df_pass['PO']
            df_out3['Account'] = df_pass['ACCOUNT NO.']
            df_out3['Equipment'] = df_pass['Equipment_Ref'] # Using reference equipment name
            df_out3['Quantity'] = df_pass['QTY']
            df_out3['Part'] = df_pass['Part No.']
            
            self.save_excel(df_out3, "UCSD EQUIPMENT Duplicate Import")

            # --- Output 4: Mismatch ---
            df_fail = merged_dups[merged_dups['Status'] == 'Mismatch'].copy()
            # "Keep all columns" logic
            self.save_excel(df_fail, "UCSD EQUIPMENT Duplicate ERRORS")

            # ---------------------------------------------------------
            # 6. Output 5: Credits (Negative Rates)
            # ---------------------------------------------------------
            self.log("Processing Output 5 (Credits)...")
            self.save_excel(df_neg_rates, "UCSD EQUIPMENT Credits")

            self.log("Processing Complete!")
            self.log_queue.put(("done", "All files generated successfully."))

        except Exception as e:
            import traceback
            traceback.print_exc()
            self.error(f"Critical Error: {str(e)}")

    def save_excel(self, df: pd.DataFrame, file_prefix: str):
        """Saves dataframe to Excel using XlsxWriter to force text format for Parts."""
        filename = f"{file_prefix} {self.today_str}.xlsx"
        path = os.path.join(self.output_folder, filename)
        
        # We need to ensure Part numbers are stored as text to preserve leading zeros
        # Pandas to_excel with engine='xlsxwriter' allows column formatting
        writer = pd.ExcelWriter(path, engine='xlsxwriter')
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        # Get workbook and worksheet objects
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        # Add text format to prevent Excel from removing leading zeros if user edits
        text_format = workbook.add_format({'num_format': '@'})
        
        # Identify 'Part No.' or 'Part' column index to apply format
        for idx, col in enumerate(df.columns):
            if "part" in col.lower():
                # Apply text format to the whole column (adjusting for header row)
                # Note: xlsxwriter columns are 0-indexed, rows are 0-indexed
                worksheet.set_column(idx, idx, None, text_format)

        writer.close()
        self.log(f"Saved: {filename}")

# --------------------------------------------------------------------------------
# GUI Layer
# --------------------------------------------------------------------------------

class ModernApp(tk.Tk):
    """Main Application GUI using tkinter."""

    def __init__(self):
        super().__init__()
        self.title("Equipment Data Automator")
        self.geometry("700x550")
        self.resizable(False, False)
        
        # Set theme/style
        self.style = ttk.Style(self)
        self.style.theme_use('clam') 
        
        # Define Colors
        self.bg_color = "#f4f4f4"
        self.accent_color = "#2196F3" # Blue
        self.success_color = "#4CAF50" # Green
        self.configure(bg=self.bg_color)

        # State storage
        self.file_paths = {'00': None, '01': None, '02': None}
        self.output_path = None
        self.status_labels = {} # Store labels to update icons

        self.setup_ui()
        
        # Threading Queue
        self.msg_queue = queue.Queue()

    def setup_ui(self):
        """Builds the widget layout."""
        
        # Main Container
        main_frame = tk.Frame(self, bg=self.bg_color)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Header
        header = tk.Label(main_frame, text="Equipment Data Processor", 
                         font=("Helvetica", 18, "bold"), bg=self.bg_color, fg="#333")
        header.pack(pady=(0, 20))

        # Input Rows Frame
        input_frame = tk.Frame(main_frame, bg="white", padx=15, pady=15, relief=tk.RAISED, bd=1)
        input_frame.pack(fill=tk.X, pady=10)

        # Row 1: Equipment File
        self.create_file_row(input_frame, "00 Equipment File", '00')
        
        # Row 2: Master Parts
        self.create_file_row(input_frame, "01 Master File Part Numbers", '01')
        
        # Row 3: PO Accounts
        self.create_file_row(input_frame, "02 Master File PO Accounts", '02')

        # Output Selection
        out_frame = tk.Frame(main_frame, bg="white", padx=15, pady=15, relief=tk.RAISED, bd=1)
        out_frame.pack(fill=tk.X, pady=10)
        
        self.create_dir_row(out_frame, "Output Folder")

        # Log Area
        log_frame = tk.Frame(main_frame, bg=self.bg_color)
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        self.log_text = tk.Text(log_frame, height=8, state='disabled', font=("Consolas", 9), 
                               bg="#e8e8e8", relief=tk.FLAT)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Action Button
        self.run_btn = tk.Button(main_frame, text="Run Process", command=self.start_processing,
                                font=("Helvetica", 11, "bold"), bg="#ddd", fg="#888", 
                                state=tk.DISABLED, height=2, relief=tk.FLAT)
        self.run_btn.pack(fill=tk.X, pady=(10, 0))

    def create_file_row(self, parent, label_text, key):
        """Helper to create a unified file selection row."""
        row = tk.Frame(parent, bg="white")
        row.pack(fill=tk.X, pady=8)

        # Status Icon
        lbl_status = tk.Label(row, text="○", font=("Arial", 14), fg="#ccc", bg="white", width=3)
        lbl_status.pack(side=tk.LEFT)
        self.status_labels[key] = lbl_status

        # Label
        lbl_name = tk.Label(row, text=label_text, font=("Helvetica", 10), bg="white", width=30, anchor="w")
        lbl_name.pack(side=tk.LEFT, padx=5)

        # Button
        btn = tk.Button(row, text="Select File...", 
                       command=lambda: self.select_file(key),
                       bg="#eee", relief=tk.FLAT, padx=10)
        btn.pack(side=tk.RIGHT)

        # Display Path (Hidden initially, or truncated)
        lbl_path = tk.Label(row, text="No file selected", font=("Arial", 8, "italic"), fg="#999", bg="white")
        lbl_path.pack(side=tk.RIGHT, padx=10)
        
        # Store reference to path label to update it
        self.status_labels[f"{key}_path"] = lbl_path

    def create_dir_row(self, parent, label_text):
        """Helper to create the directory selection row."""
        row = tk.Frame(parent, bg="white")
        row.pack(fill=tk.X, pady=8)

        lbl_status = tk.Label(row, text="○", font=("Arial", 14), fg="#ccc", bg="white", width=3)
        lbl_status.pack(side=tk.LEFT)
        self.status_labels['output'] = lbl_status

        lbl_name = tk.Label(row, text=label_text, font=("Helvetica", 10), bg="white", width=30, anchor="w")
        lbl_name.pack(side=tk.LEFT, padx=5)

        btn = tk.Button(row, text="Select Folder...", command=self.select_output,
                       bg="#eee", relief=tk.FLAT, padx=10)
        btn.pack(side=tk.RIGHT)

        lbl_path = tk.Label(row, text="", font=("Arial", 8, "italic"), fg="#999", bg="white")
        lbl_path.pack(side=tk.RIGHT, padx=10)
        self.status_labels['output_path'] = lbl_path

    def select_file(self, key):
        filename = filedialog.askopenfilename(
            title="Select File",
            filetypes=(("Excel/CSV Files", "*.xlsx *.xls *.csv"), ("All files", "*.*"))
        )
        if filename:
            self.file_paths[key] = filename
            # Update UI
            self.status_labels[key].config(text="✓", fg=self.success_color)
            self.status_labels[f"{key}_path"].config(text=os.path.basename(filename))
            self.check_ready()

    def select_output(self):
        dirname = filedialog.askdirectory(title="Select Output Folder")
        if dirname:
            self.output_path = dirname
            self.status_labels['output'].config(text="✓", fg=self.success_color)
            self.status_labels['output_path'].config(text=dirname)
            self.check_ready()

    def check_ready(self):
        """Enables Run button only if all inputs are satisfied."""
        if all(self.file_paths.values()) and self.output_path:
            self.run_btn.config(state=tk.NORMAL, bg=self.accent_color, fg="white", cursor="hand2")
        else:
            self.run_btn.config(state=tk.DISABLED, bg="#ddd", fg="#888")

    def append_log(self, msg):
        """Updates log window safely."""
        self.log_text.config(state='normal')
        self.log_text.insert(tk.END, f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state='disabled')

    def start_processing(self):
        """Initiates the background thread."""
        self.run_btn.config(state=tk.DISABLED, text="Processing...")
        self.append_log("Starting background process...")
        
        processor = DataProcessor(self.file_paths, self.output_path, self.msg_queue)
        
        t = threading.Thread(target=processor.run, daemon=True)
        t.start()
        
        # Start polling the queue
        self.after(100, self.poll_queue)

    def poll_queue(self):
        """Checks for messages from the background thread."""
        try:
            while True:
                msg_type, msg_content = self.msg_queue.get_nowait()
                
                if msg_type == "status":
                    self.append_log(msg_content)
                elif msg_type == "error":
                    self.append_log(f"ERROR: {msg_content}")
                    messagebox.showerror("Error", msg_content)
                    self.reset_ui()
                    return # Stop polling on critical error
                elif msg_type == "done":
                    self.append_log(msg_content)
                    messagebox.showinfo("Success", msg_content)
                    self.reset_ui()
                    return

        except queue.Empty:
            # Continue polling if no messages
            self.after(100, self.poll_queue)

    def reset_ui(self):
        self.run_btn.config(state=tk.NORMAL, text="Run Process", bg=self.accent_color)

# --------------------------------------------------------------------------------
# Main Entry Point
# --------------------------------------------------------------------------------

if __name__ == "__main__":
    app = ModernApp()
    app.mainloop()