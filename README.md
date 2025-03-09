# 🌊 WISE Database Extraction Tool

## 📌 Project Overview
This project is a **Python-based tool** for extracting and processing **WISE water data** from an **SQLite database**. It includes:

✅ **Database Indexing & Cleaning** via `baseline_extraction.py`

✅ **Parallel Data Processing & CSV Export** via `baseline_processing.py`

✅ **User-Friendly GUI** using `ttkbootstrap` (`gui_extraction.py`)

📚 WISE Database Source: [European Environment Agency (EEA) WISE-WFD Database](https://www.eea.europa.eu/data-and-maps/data/wise-wfd-4/wise-wfd-database-1)

The tool processes water body information by country and generates CSV reports.

---

## 🛠️ Installation
### 1️⃣ **Clone the Repository**
```sh
git clone https://github.com/ThGoulis/1.1-WISE-Extraction-Tool.git
cd WISE-Extraction-Tool
```

### 2️⃣ **Set Up a Virtual Environment** *(Recommended)*
```sh
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3️⃣ **Install Dependencies**
```sh
pip install -r requirements.txt
```

---

## 🚀 Usage
### **1️⃣ Run the GUI (Recommended)**
```sh
python gui_extraction.py
```

- Browse for the **SQLite database file** (`.sqlite`).
- Enter the **country code** (e.g., `DE` for Germany, `FR` for France).
- Select an **output directory** for the generated CSVs.
- Click **"Start Extraction"**.

### **2️⃣ Run the Script in Terminal (CLI Mode)**
```sh
python baseline_processing.py database.sqlite DE output_folder/
```
This runs all extraction processes **in parallel** using **multiprocessing**.

---

## 📂 Project Structure
```
WISE-Extraction-Tool/
│── gui_extraction.py        # GUI Interface
│── baseline_processing.py   # Multiprocessing data extraction
│── baseline_extraction.py   # Database indexing & table updates
│── requirements.txt         # Required dependencies
│── README.md                # Project Documentation
```

---

## 📌 Dependencies
- Python 3.x
- `sqlite3`
- `ttkbootstrap` *(for the GUI)*
- `multiprocessing`, `argparse`, `tqdm`

Install all dependencies via:
```sh
pip install -r requirements.txt
```

---

## 🔥 Contributing
Pull requests are welcome! Feel free to submit issues and suggestions.

---

## 📝 License
This project is licensed under the GNU Affero General Public License v3.0. See the LICENSE file for details.
