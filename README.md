# 🚀 Universal SQL Data Warehouse Pipeline

This project is an automated **ETL Pipeline** (Extract, Transform, Load). It takes messy, raw data from CSV files and uses Python and SQL to clean it, organize it, and make it ready for business dashboards.

We use the **Medallion Architecture**, which organizes data into three stages:
* **Bronze 🥉**: Raw data straight from the source.
* **Silver 🥈**: Cleaned and filtered data.
* **Gold 🥇**: Business-ready data organized for reporting.

---

## 📁 Project Structure (Where Everything Lives)

Here is how our project folders are organized and what each part does:

* `datasets/` 📊: Put your raw CSV files here. (e.g., `source_crm/cust_info.csv`).
* `sql_scripts/` 🗄️: Contains our SQL code.
  * `01_silver_layer.sql`: Cleans the data (removes empty spaces, fixes dates).
  * `02_gold_layer.sql`: Connects the data together (creates business views).
* `pipeline_orchestrator.py` 🐍: The Python "boss" that runs everything in the right order.
* `.env.example` 🔐: A template to show how to securely connect to your database.

---

## 🛠️ Step-by-Step Setup Guide

Follow these steps carefully to run this project on your own computer!

### Step 1: Install the Tools
You need a few standard tools installed on your computer first:
1. **Python** (Version 3.8 or newer)
2. **A Database**: You can use Microsoft SQL Server, PostgreSQL, or MySQL. 

### Step 2: Install Python Libraries
Our Python script needs a few extra "translator" packages to talk to the database and read CSVs. Open your computer's **Terminal** (Mac) or **Command Prompt** (Windows) and type this:

```bash
pip install pandas sqlalchemy python-dotenv
```
(Depending on your database, you also need one of these: pip install pyodbc for SQL Server, OR pip install psycopg2 for Postgres).

### Step 3: Set Up Your Database Connection (The .env file)
We never put passwords or database names directly into our code because it isn't safe. Instead, we use a secret file.
  1. Find the .env.example file in this folder.
  2. Rename it to just .env.
  3. Open it in any text editor (like Notepad or VS Code).
  4. Type in your database connection link. It should look something like this:

```Code Snippet
# For Microsoft SQL Server:
DATABASE_URL=mssql+pyodbc://localhost/YourDatabaseName?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes
```

Note: Because of our .gitignore file, your .env file will stay hidden on your computer and won't upload to GitHub!

### Step 4: Run the Pipeline!
Now for the fun part. Open your Terminal, navigate to this project folder, and tell Python to run the orchestrator:

```bash
python pipeline_orchestrator.py
```

**✅ How to Check Your Work**
Open your database software (like SQL Server Management Studio or DBeaver) and run this simple query:

```SQL
SELECT * FROM gold.dim_customers;
```

⚠️ Common Beginner Errors (Troubleshooting)
**"ModuleNotFoundError":** 
Python doesn't recognize a library. **Solution:** Run the pip install commands from Step 2 again.

**"File Not Found":** 
Python can't find your CSV files. **Solution:** Make sure your datasets/ folder is exactly in the same main folder as your Python script.

**"Connection Refused":** 
Python can't talk to your database. **Solution:** Check your .env file. Is your database turned on? Is the name spelled perfectly?

--
**I utilized Google Gemini to assist in drafting this automation script as I am a very beginner in Python. Please feel free to point out any errors or areas for improvement; I always welcome the opportunity to learn from your feedback and ensure the script is optimized.**
--

**Original repository base:** [sql-data-warehouse-project](https://github.com/rgdeekshith/sql-data-warehouse-project)
Automation scripts developed with assistance from Google Gemini.
