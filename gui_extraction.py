import os
import time
import ttkbootstrap as ttk  # ✅ Use ttkbootstrap for a modern UI
from ttkbootstrap import Style
from tkinter import filedialog, messagebox
from multiprocessing import Process, Queue, freeze_support
import baseline_extraction
import baseline_processing

# ✅ Function to Browse Database File
def browse_db():
    filename = filedialog.askopenfilename(filetypes=[("SQLite Database", "*.sqlite"), ("All Files", "*.*")])
    db_entry.delete(0, ttk.END)
    db_entry.insert(0, filename)

# ✅ Function to Browse Output Directory
def browse_output():
    folder = filedialog.askdirectory()
    output_entry.delete(0, ttk.END)
    output_entry.insert(0, folder)

# ✅ Create Indexes in Database
def create_indexes():
    """Creates indexes for the selected database."""
    db_file = db_entry.get()

    if not db_file or not os.path.exists(db_file):
        messagebox.showerror("Error", "Invalid Database File! Please select a valid .sqlite file.")
        return

    if messagebox.askyesno("Create Indexes", "Do you want to create indexes for the database?"):
        baseline_extraction.createIndexies(db_file)
        messagebox.showinfo("Success", "Indexes created successfully!")

# ✅ Start Extraction in a Separate Process
def run_extraction():
    """Starts extraction in a separate process to avoid blocking the GUI."""
    db_file = db_entry.get()
    countryCode = country_entry.get().strip()
    output_dir = output_entry.get()

    if not db_file or not os.path.exists(db_file):
        messagebox.showerror("Error", "Invalid Database File!")
        return
    if not countryCode:
        messagebox.showerror("Error", "Country Code is required!")
        return
    if not output_dir or not os.path.isdir(output_dir):
        messagebox.showerror("Error", "Invalid Output Directory!")
        return

    working_directory = os.path.join(output_dir, countryCode)

    log_text.insert(ttk.END, f"📂 Starting Extraction for {countryCode}...\n")
    log_text.update_idletasks()
    
    progress_bar["value"] = 0
    progress_bar.start(10)

    queue = Queue()
    process = Process(target=run_extraction_process, args=(db_file, countryCode, working_directory, queue))
    process.start()

    root.after(100, check_queue, queue)

# ✅ Run Extraction Process (Multiprocessing)
def run_extraction_process(db_file, countryCode, working_directory, queue):
    """Runs the extraction process and sends completion status."""
    start_time = time.time()
    baseline_processing.run_csv_generation_process_multiprocessing(db_file, [countryCode], working_directory)
    elapsed_time = time.time() - start_time

    queue.put((countryCode, elapsed_time))

# ✅ Check Queue for Extraction Completion
def check_queue(queue):
    """Monitors the queue for messages from the extraction process."""
    try:
        while not queue.empty():
            countryCode, elapsed_time = queue.get_nowait()
            log_completion(countryCode, elapsed_time)
            progress_bar.stop()
    except:
        pass

    root.after(100, check_queue, queue)

# ✅ Update Log After Extraction Completes
def log_completion(countryCode, elapsed_time):
    """Updates the GUI log when extraction is complete."""
    log_text.insert(ttk.END, f"✅ Extraction for {countryCode} completed in {elapsed_time:.2f} seconds.\n")
    log_text.update_idletasks()
    messagebox.showinfo("Completed", f"Extraction for {countryCode} is done!")
    progress_bar.stop()

# ✅ Exit Application
def exit_application():
    root.quit()
    root.destroy()

# ✅ GUI Setup
def create_gui():
    global root, db_entry, country_entry, output_entry, log_text, progress_bar

    root = ttk.Window(themename="lumen")  # ✅ Modern UI theme
    style = Style(theme="lumen")

    root.title("WISE Database Extraction Tool")
    root.geometry("600x900")

    font = ("Arial", 10, "bold")

    # ✅ Database File Selection
    ttk.Label(root, text="SQLite Database (.sqlite)", font=font).pack(pady=5)
    ttk.Label(root, text="Select the SQLite database file that contains the water data.", foreground="gray").pack()
    db_entry = ttk.Entry(root, width=50)
    db_entry.pack(pady=2)
    ttk.Button(root, text="Browse", bootstyle="primary", command=browse_db).pack(pady=5)

    # ✅ Country Code Entry
    ttk.Label(root, text="Country Code", font=font).pack(pady=5)
    ttk.Label(root, text="Enter the country code (e.g., 'DE' for Germany, 'FR' for France).", foreground="gray").pack()
    country_entry = ttk.Entry(root, width=10)
    country_entry.pack(pady=2)

    # ✅ Output Directory Selection
    ttk.Label(root, text="Output Directory", font=font).pack(pady=5)
    ttk.Label(root, text="Choose a folder where the generated CSV files will be saved.", foreground="gray").pack()
    output_entry = ttk.Entry(root, width=50)
    output_entry.pack(pady=2)
    ttk.Button(root, text="Browse", bootstyle="primary", command=browse_output).pack(pady=5)

    # ✅ Buttons
    ttk.Button(root, text="Create Indexes", bootstyle="info", command=create_indexes).pack(pady=10)
    ttk.Button(root, text="Start Extraction", bootstyle="success", command=run_extraction).pack(pady=10)

    # ✅ Log Output
    ttk.Label(root, text="Process Log", font=font).pack(pady=5)
    log_text = ttk.Text(root, height=10, width=70)
    log_text.pack(pady=5)

    # ✅ Progress Bar
    ttk.Label(root, text="Progress", font=font).pack(pady=5)
    progress_bar = ttk.Progressbar(root, length=400, mode='indeterminate', bootstyle="info")
    progress_bar.pack(pady=5)

    # ✅ Exit Button
    ttk.Button(root, text="Exit", bootstyle="danger", command=exit_application).pack(pady=5)

    root.mainloop()

# ✅ Ensure Multiprocessing Works on Windows
if __name__ == "__main__":
    freeze_support()
    create_gui()
