import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import logic  # Importing your new logic.py file

class EquipmentReconApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Equipment Reconciliation Tool")
        self.root.geometry("600x550")
        
        # Styles
        style = ttk.Style()
        style.configure("TButton", padding=6, relief="flat", background="#ccc")
        style.configure("TLabel", padding=5, font=("Helvetica", 10))

        # Variables
        self.source_path = tk.StringVar()
        self.master_path = tk.StringVar()
        self.po_master_path = tk.StringVar()
        self.export_folder = tk.StringVar()

        # --- GUI LAYOUT ---
        title_label = ttk.Label(root, text="Equipment Data Reconciliation", font=("Helvetica", 14, "bold"))
        title_label.pack(pady=15)

        form_frame = ttk.Frame(root, padding=20)
        form_frame.pack(fill='both', expand=True)

        # 1. Source File
        ttk.Label(form_frame, text="Source File (UCSD EQUIPMENT Summary):").grid(row=0, column=0, sticky='w')
        ttk.Entry(form_frame, textvariable=self.source_path, width=50).grid(row=1, column=0, padx=5, pady=5)
        ttk.Button(form_frame, text="Browse", command=self.select_source).grid(row=1, column=1, padx=5)

        # 2. Master File
        ttk.Label(form_frame, text="Master File (Part Numbers Map):").grid(row=2, column=0, sticky='w', pady=(10, 0))
        ttk.Entry(form_frame, textvariable=self.master_path, width=50).grid(row=3, column=0, padx=5, pady=5)
        ttk.Button(form_frame, text="Browse", command=self.select_master).grid(row=3, column=1, padx=5)

        # 3. Equipment PO Part Account Master File
        ttk.Label(form_frame, text="Equipment PO Part Account Master File:").grid(row=4, column=0, sticky='w', pady=(10, 0))
        ttk.Entry(form_frame, textvariable=self.po_master_path, width=50).grid(row=5, column=0, padx=5, pady=5)
        ttk.Button(form_frame, text="Browse", command=self.select_po_master).grid(row=5, column=1, padx=5)

        # 4. Export Folder
        ttk.Label(form_frame, text="Export Folder:").grid(row=6, column=0, sticky='w', pady=(10, 0))
        ttk.Entry(form_frame, textvariable=self.export_folder, width=50).grid(row=7, column=0, padx=5, pady=5)
        ttk.Button(form_frame, text="Browse", command=self.select_folder).grid(row=7, column=1, padx=5)

        # 5. Action Button
        btn_frame = ttk.Frame(root, padding=20)
        btn_frame.pack(fill='x')
        self.run_btn = tk.Button(btn_frame, text="RUN RECONCILIATION", bg="#4CAF50", fg="white", 
                                 font=("Helvetica", 10, "bold"), height=2, command=self.run_process)
        self.run_btn.pack(fill='x')

        # Status Bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = tk.Label(root, textvariable=self.status_var, bd=1, relief=tk.SUNKEN, anchor='w')
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    # --- HELPER FUNCTIONS ---
    def select_source(self):
        filename = filedialog.askopenfilename(title="Select Source File", filetypes=[("Excel Files", "*.xlsx *.xls")])
        if filename: self.source_path.set(filename)

    def select_master(self):
        filename = filedialog.askopenfilename(title="Select Master File", filetypes=[("Excel Files", "*.xlsx *.xls")])
        if filename: self.master_path.set(filename)

    def select_po_master(self):
        filename = filedialog.askopenfilename(title="Select PO Master File", filetypes=[("Excel Files", "*.xlsx *.xls")])
        if filename: self.po_master_path.set(filename)

    def select_folder(self):
        folder = filedialog.askdirectory(title="Select Export Folder")
        if folder: self.export_folder.set(folder)

    def run_process(self):
        # 1. Validation
        if not all([self.source_path.get(), self.master_path.get(), self.po_master_path.get(), self.export_folder.get()]):
            messagebox.showerror("Error", "Please select all input files and an export folder.")
            return

        # 2. Execution
        try:
            self.status_var.set("Processing data...")
            self.root.update_idletasks() # Update UI before freezing for logic

            # CALL THE LOGIC FILE
            success_msg = logic.run_reconciliation(
                self.source_path.get(),
                self.master_path.get(),
                self.po_master_path.get(),
                self.export_folder.get()
            )

            # Success
            self.status_var.set("Ready")
            messagebox.showinfo("Success", f"Process Complete!\n\n{success_msg}")

        except Exception as e:
            self.status_var.set("Error")
            messagebox.showerror("Execution Error", f"An error occurred:\n{str(e)}")
            print(e)