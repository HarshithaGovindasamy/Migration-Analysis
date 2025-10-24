# Analyzing Migration Patterns from Census Data (SDG 10: Reduced Inequalities)

**Status:** Completed Mini Project
**Subject:** Big Data Analysis (PySpark Implementation)
**Author:** Harsha (Harshitha)
**Repo:** Analyzing-Migration-Patterns

---

## Executive Summary

This project implements a **Big Data analysis pipeline** using **PySpark** and **Pandas** to analyze migration patterns and quantify socio-economic inequality, directly addressing **SDG 10 (Reduced Inequalities)**. The core deliverable calculates and visualizes the **average income gap** between internal migrants and non-migrants using census-style microdata.

The solution is intentionally simple (`run_analysis.py`) to minimize environmental setup issues while proving proficiency in distributed computing concepts.

---

## Project Objectives & Key Deliverables

* **Big Data Analysis:** Implement core ETL (Extraction, Transformation, Load) and aggregation steps using **PySpark** for distributed processing.
* **SDG 10 Metric:** Calculate the **income disparity** stratified by migration status (Migrant vs. Non-Migrant).
* **Reproducibility:** Provide a single, executable script that handles data creation, analysis, and visualization.
* **Visualization:** Generate a bar chart illustrating the income inequality metric.

---

## Tech Stack & Setup

| Component | Purpose | Notes |
| :--- | :--- | :--- |
| **PySpark** | Core Big Data Framework | Used for distributed aggregation. |
| **Python 3.x** | Programming Language | Core logic and scripting. |
| **pandas/seaborn** | Data manipulation / Visualization | Used for final plotting of aggregated results. |
| **Java JDK** | Spark Dependency | **Crucial**—must be installed on the host machine. |

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repo-url>
    cd Simple-Migration-Analysis
    ```
2.  **Create and activate a virtual environment (venv):**
    ```bash
    python -m venv venv
    venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install pyspark findspark pandas matplotlib seaborn
    ```
4.  **CRITICAL Environment Step (For Windows):** The project requires Java to run Spark. If the script fails, you may need to manually set the `JAVA_HOME` environment variable in your terminal before running:
    ```bash
    set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_401
    ```
    *(Adjust the path to your actual Java installation.)*

---

## Quickstart: Running the Analysis

The single script `run_analysis.py` is designed to be fully self-contained. It will **automatically create a dummy data file** (`data/census_data.csv`) if one does not exist and then execute the full PySpark pipeline.

1.  **Run the primary analysis script:**

    ```bash
    (venv) PS C:\...\Simple-Migration-Analysis> python run_analysis.py
    ```

### Expected Output

Upon successful completion, the following files will be generated in the **`results/`** directory:

* **`sdg10_income_stats.csv`**: A CSV file containing the final aggregated average income for Migrants and Non-Migrants.
* **`sdg10_income_bar.png`**: A visualization that clearly displays the calculated income gap (the SDG 10 metric).

---

## Project Structure (Simple Scaffold)
