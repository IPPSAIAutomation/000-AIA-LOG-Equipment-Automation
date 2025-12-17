import pandas as pd
import os
from datetime import datetime

def run_reconciliation(source_path, master_path, export_folder):
    """
    Core logic for Equipment Reconciliation.
    Returns a success message string if successful.
    Raises an Exception if something fails.
    """
    
    # =========================================================
    # PART 1: Process Tab 1 (Index 0) - Reconciliation Logic
    # =========================================================
    try:
        # sheet_name=0 loads the first sheet, header=16 is Row 17
        df_source = pd.read_excel(source_path, sheet_name=0, header=16, dtype={'ACCOUNT NO.': str})
    except Exception as e:
        raise ValueError(f"Could not read the FIRST sheet of the Source File.\nDetails: {e}")
    
    df_master = pd.read_excel(master_path)

    # --- Validation ---
    if 'ACCOUNT NO.' not in df_source.columns:
            raise ValueError(f"Column 'ACCOUNT NO.' not found in Tab 1. Found: {df_source.columns.tolist()}")

    null_indices = df_source.index[df_source['ACCOUNT NO.'].isna()]
    if not null_indices.empty:
        df_source = df_source.iloc[:null_indices[0]]

    # --- Cleaning ---
    df_source['ACCOUNT NO.'] = df_source['ACCOUNT NO.'].fillna('').astype(str).str.strip().str.lstrip('0')
    df_source.drop_duplicates(inplace=True)

    # Prepare for Merge
    df_source['EQUIP_CLEAN'] = df_source['EQUIP'].astype(str).str.strip()
    df_master['Equipment_CLEAN'] = df_master['Equipment'].astype(str).str.strip()

    # --- Merge ---
    df_merged = pd.merge(
        df_source, 
        df_master[['Equipment_CLEAN', 'Part No.']], 
        left_on='EQUIP_CLEAN', 
        right_on='Equipment_CLEAN', 
        how='left'
    )

    # --- Formatting ---
    rename_map = {'Qty': 'Quantity', 'QTY': 'Quantity', 'qty': 'Quantity'}
    df_merged.rename(columns=rename_map, inplace=True)
    
    df_merged['Part No.'] = df_merged['Part No.'].fillna('NO MATCH FOUND')
    df_merged['Part No.'] = df_merged['Part No.'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(4)

    final_columns = ['ACCOUNT NO.', 'EQUIP', 'Quantity', 'Part No.']
    missing_cols = [c for c in final_columns if c not in df_merged.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in Tab 1 data: {missing_cols}")

    df_final = df_merged[final_columns].copy()

    # --- Logic Split (Unique vs Duplicates) ---
    account_counts = df_final['ACCOUNT NO.'].value_counts()
    df_final['Freq_Count'] = df_final['ACCOUNT NO.'].map(account_counts)
    
    df_unique = df_final[df_final['Freq_Count'] == 1].drop(columns=['Freq_Count']).copy()
    df_duplicates = df_final[df_final['Freq_Count'] >= 2].drop(columns=['Freq_Count']).copy()

    # --- SPECIAL LOGIC: Filter specific items from duplicates ---
    # Remove rows where EQUIP contains "H/C  Filtration -Countertop Unit"
    df_duplicates = df_duplicates[~df_duplicates['EQUIP'].astype(str).str.contains("H/C  Filtration -Countertop Unit", case=False, na=False)]

    # --- RE-EVALUATE Uniqueness after filter ---
    dup_counts_recalc = df_duplicates['ACCOUNT NO.'].value_counts()
    df_duplicates['New_Count'] = df_duplicates['ACCOUNT NO.'].map(dup_counts_recalc)

    # Identify rows that became unique
    rows_to_move = df_duplicates[df_duplicates['New_Count'] == 1].drop(columns=['New_Count'])
    
    # Identify rows that remain duplicates
    df_duplicates_final = df_duplicates[df_duplicates['New_Count'] >= 2].drop(columns=['New_Count'])

    # Move rows
    if not rows_to_move.empty:
        df_unique = pd.concat([df_unique, rows_to_move], ignore_index=True)

    df_duplicates = df_duplicates_final.sort_values(by='ACCOUNT NO.')

    # =========================================================
    # PART 2: Process Tab 3 (Index 2) - STD Import Logic
    # =========================================================
    try:
        # header=15 (Row 16) for Tab 3
        df_std = pd.read_excel(source_path, sheet_name=2, header=15, dtype={'ACCOUNT NO.': str})
    except Exception:
        # We silently fail here or log it, treating it as empty to continue process
        df_std = pd.DataFrame() 

    df_std_grouped = pd.DataFrame() # Default empty

    if not df_std.empty:
        if 'ACCOUNT NO.' in df_std.columns:
            null_indices_std = df_std.index[df_std['ACCOUNT NO.'].isna()]
            if not null_indices_std.empty:
                df_std = df_std.iloc[:null_indices_std[0]]

            df_std['ACCOUNT NO.'] = df_std['ACCOUNT NO.'].fillna('').astype(str).str.strip().str.lstrip('0')
            df_std.rename(columns=rename_map, inplace=True)

            if 'EQUIP' in df_std.columns:
                df_std['EQUIP_CLEAN'] = df_std['EQUIP'].astype(str).str.strip()
                
                df_std_merged = pd.merge(
                    df_std, 
                    df_master[['Equipment_CLEAN', 'Part No.']], 
                    left_on='EQUIP_CLEAN', 
                    right_on='Equipment_CLEAN', 
                    how='left'
                )
                
                df_std_merged['Part No.'] = df_std_merged['Part No.'].fillna('NO MATCH FOUND')
                df_std_merged['Part No.'] = df_std_merged['Part No.'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(4)
                df_std_merged.rename(columns={'Part No.': 'Part #'}, inplace=True)

                if 'Quantity' in df_std_merged.columns:
                    df_std_merged['Quantity'] = pd.to_numeric(df_std_merged['Quantity'], errors='coerce').fillna(0)
                    df_std_grouped = df_std_merged.groupby(['ACCOUNT NO.', 'Part #'])['Quantity'].sum().reset_index()
                else:
                    raise ValueError("Column 'Quantity' not found in 3rd Tab.")
            else:
                raise ValueError("Column 'EQUIP' not found in Tab 3.")
        else:
            raise ValueError(f"Column 'ACCOUNT NO.' not found in 3rd Tab.")

    # =========================================================
    # TODO: INSERT YOUR NEW FILE LOGIC HERE
    # =========================================================
    # df_new_logic = ...

    # =========================================================
    # EXPORT
    # =========================================================
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    
    # File 1: Standard Recon
    file_name_1 = f"UCSD EQUIPMENT Equipment Import {timestamp}.xlsx"
    full_path_1 = os.path.join(export_folder, file_name_1)

    with pd.ExcelWriter(full_path_1, engine='openpyxl') as writer:
        df_unique.to_excel(writer, sheet_name='Equipment Import', index=False)
        df_duplicates.to_excel(writer, sheet_name='Multiple Occurrences', index=False)

    msg = f"File 1 Created:\n{file_name_1}"

    # File 2: STD Import
    if not df_std_grouped.empty:
        file_name_2 = f"UCSD EQUIPMENT STD Import {timestamp}.xlsx"
        full_path_2 = os.path.join(export_folder, file_name_2)
        df_std_grouped.to_excel(full_path_2, index=False)
        msg += f"\n\nFile 2 Created:\n{file_name_2}"
    else:
        msg += "\n\n(Warning: 3rd Tab empty or skipped, File 2 not created.)"

    return msg