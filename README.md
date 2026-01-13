# PySpark Stock Analysis Application

[![Colab](https://img.shields.io/badge/Open%20in-Colab-blue?logo=googlecolab)](https://colab.research.google.com/github/theophile-bb/Pyspark-stock-analysis/blob/main/Pyspark_stock_analysis_app.ipynb)

This repository contains a PySpark project designed to perform scalable analysis of stock market data using distributed computing.  
The notebook and utilities demonstrate data ingestion, transformation, feature engineering, and visual insights using PySpark DataFrames and modular functions.

![image](https://github.com/user-attachments/assets/2b9c9dbe-e18f-4f16-b1cd-6b7f4a996294)

---

## Project Structure

Pyspark_stock_analysis_app/ <br>
├── Data/ <br>
│ ├── AMAZON.csv  <br>
│ ├── APPLE.csv  <br>
│ ├── FACEBOOK.csv  <br>
│ ├── GOOGLE.csv  <br>
│ ├── MICROSOFT.csv  <br>
│ ├── TESLA.csv  <br>
│ ├── ZOOM.csv  <br>
├── src/ <br>
│ ├── init.py <br>
│ └── utils.py # Reusable functions for data & model <br>
├── Pyspark_stock_analysis_app.ipynb # Main analysis notebook <br>
├── requirements.txt <br>
├── .gitignore <br>
└── README.md <br>

---


## 📋 Prerequisites

This project requires:

- Python 3.10+
- A working Python environment (venv, conda, etc.)

---

## 📋 Prerequisites

This project requires:

- Python 3.7+  
- Java 8 or higher (required by Spark)  
- PySpark   

Before running, make sure your Spark environment is set up correctly.

---

## ⚙️ Installation

1. Clone the repository:

```
$ git clone https://github.com/theophile-bb/Pyspark-stock-analysis.git
$ cd Pyspark-stock-analysis
$ pip install -r requirements.txt
```

## Data

The dataset used in this project includes historical stock prices and related financial metrics from major American companies. Key features typically are:

• Date: The trading date.

• Open: Opening price of the stock.

• High: Highest price during the trading session.

• Low: Lowest price during the trading session.

• Close: Closing price of the stock.

• Volume: Number of shares traded.

## Notebook

The main analysis is in: Pyspark_stock_analysis_app.ipynb


- Spark session initialization

- Distributed data loading

- Exploratory analysis & transformation

- Feature engineering (returns, rolling metrics)

- Visulization for performance monitoring

