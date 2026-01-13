# PySpark Stock Analysis Application

[![Colab](https://img.shields.io/badge/Open%20in-Colab-blue?logo=googlecolab)](https://colab.research.google.com/github/theophile-bb/Pyspark-stock-analysis/blob/main/Pyspark_stock_analysis_app.ipynb)

This repository contains a PySpark project designed to perform scalable analysis of stock market data using distributed computing.  
The notebook and utils provide modular functions to demonstrate data ingestion, transformation, feature engineering, and visual insights using PySpark.

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

---

## Data

The dataset used in this project includes historical stock prices and related financial metrics from major American companies. Key features typically are:

• Date: The trading date.

• Open: Opening price of the stock.

• High: Highest price during the trading session.

• Low: Lowest price during the trading session.

• Close: Closing price of the stock.

• Volume: Number of shares traded.

---

## Notebook

The main analysis is in: Pyspark_stock_analysis_app.ipynb


- Spark session initialization

- Distributed data loading

- Exploratory analysis & transformation

- Feature engineering (returns, rolling metrics)

- Visulization for performance monitoring

---

## Data Exploration function example 

```
Infos of the ZOOM.csv file 


► Schema of the dataset
root
 |-- Date: date (nullable = true)
 |-- High: double (nullable = true)
 |-- Low: double (nullable = true)
 |-- Open: double (nullable = true)
 |-- Close: double (nullable = true)
 |-- Volume: integer (nullable = true)
 |-- Adj Close: double (nullable = true)
 |-- company_name: string (nullable = true)

None

 ► Top rows of the dataset
+----------+-----------------+------------------+-----------------+-----------------+--------+-----------------+------------+
|      Date|             High|               Low|             Open|            Close|  Volume|        Adj Close|company_name|
+----------+-----------------+------------------+-----------------+-----------------+--------+-----------------+------------+
|2019-04-18|             66.0| 60.32099914550781|             65.0|             62.0|25764700|             62.0|        ZOOM|
|2019-04-22| 68.9000015258789|59.939998626708984|             61.0|65.69999694824219| 9949700|65.69999694824219|        ZOOM|
|2019-04-23|74.16899871826172| 65.55000305175781|66.87000274658203|             69.0| 6786500|             69.0|        ZOOM|
|2019-04-24|             71.5| 63.15999984741211| 71.4000015258789|63.20000076293945| 4973500|63.20000076293945|        ZOOM|
|2019-04-25| 66.8499984741211|62.599998474121094|64.73999786376953|             65.0| 3863300|             65.0|        ZOOM|
|2019-04-26|66.98999786376953|63.599998474121094|66.12000274658203|66.22000122070312| 1527400|66.22000122070312|        ZOOM|
|2019-04-29|             68.5|             64.75|66.52999877929688|68.16999816894531| 1822300|68.16999816894531|        ZOOM|
|2019-04-30| 72.5199966430664| 66.66999816894531| 68.4000015258789|72.47000122070312| 4113100|72.47000122070312|        ZOOM|
|2019-05-01|76.94999694824219| 70.81600189208984|72.72000122070312|72.76000213623047| 3301900|72.76000213623047|        ZOOM|
|2019-05-02|75.88999938964844| 69.69100189208984|            72.75|             75.5| 2525300|             75.5|        ZOOM|
+----------+-----------------+------------------+-----------------+-----------------+--------+-----------------+------------+
only showing top 10 rows

None

 ► Row count
411

 ► Time span
Minimum date 2019-04-18
Maximum date 2020-12-02
594 days, 0:00:00

 ► Columns info
Date
+-------+
|summary|
+-------+
|  count|
|   mean|
| stddev|
|    min|
|    max|
+-------+

None
High
+-------+------------------+
|summary|              High|
+-------+------------------+
|  count|               411|
|   mean|182.39606543585043|
| stddev|142.85245242428914|
|    min|  63.7400016784668|
|    max| 588.8400268554688|
+-------+------------------+

None
Low
+-------+------------------+
|summary|               Low|
+-------+------------------+
|  count|               411|
|   mean|172.02370037069576|
| stddev| 134.5095182837666|
|    min|59.939998626708984|
|    max| 562.5499877929688|
+-------+------------------+

None
Open
+-------+-----------------+
|summary|             Open|
+-------+-----------------+
|  count|              411|
|   mean|177.1945766625323|
| stddev|138.9019224307191|
|    min|             61.0|
|    max|            572.5|
+-------+-----------------+

None
Close
+-------+------------------+
|summary|             Close|
+-------+------------------+
|  count|               411|
|   mean|177.46828020750172|
| stddev|138.66359007242428|
|    min|              62.0|
|    max| 568.3400268554688|
+-------+------------------+

None
Volume
+-------+-----------------+
|summary|           Volume|
+-------+-----------------+
|  count|              411|
|   mean|7060712.613138686|
| stddev|6905543.039975025|
|    min|           512600|
|    max|         53346800|
+-------+-----------------+

None
Adj Close
+-------+------------------+
|summary|         Adj Close|
+-------+------------------+
|  count|               411|
|   mean|177.46828020750172|
| stddev|138.66359007242428|
|    min|              62.0|
|    max| 568.3400268554688|
+-------+------------------+

None
company_name
+-------+------------+
|summary|company_name|
+-------+------------+
|  count|         411|
|   mean|        NULL|
| stddev|        NULL|
|    min|        ZOOM|
|    max|        ZOOM|
+-------+------------+

None

 ► Missing values for each column
Date 	 number of null values:  0
High 	 number of null values:  0
Low 	 number of null values:  0
Open 	 number of null values:  0
Close 	 number of null values:  0
Volume 	 number of null values:  0
Adj Close 	 number of null values:  0
company_name 	 number of null values:  0

 ► Correlation Matrix
[[1.         0.99913202 0.99929984 0.99921986 0.41942667 0.99921986]
 [0.99913202 1.         0.99914703 0.99926417 0.39512838 0.99926417]
 [0.99929984 0.99914703 1.         0.99824163 0.40555376 0.99824163]
 [0.99921986 0.99926417 0.99824163 1.         0.4103528  1.        ]
 [0.41942667 0.39512838 0.40555376 0.4103528  1.         0.4103528 ]
 [0.99921986 0.99926417 0.99824163 1.         0.4103528  1.        ]]
```
