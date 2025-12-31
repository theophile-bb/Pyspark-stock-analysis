#--------------------- processing ------------------------

"""import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()"""

from warnings import filters
import os

import numpy as np

from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, year, month, weekofyear, avg, to_date
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

import plotly.express as px
import plotly.graph_objects as go


"""Create  a spark session"""
def create_spark_session(
    app_name: str = "StockAnalysis",
    master: str = "local[*]"
) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .getOrCreate()
    )

"""
Function : read_file(folder,file)

Parameters :
folder : path of the folder with the csv files
file : name of the file

Return : spark dataframe

Usage :
This function allow us to load the file into a spark dataframe.
"""

def read_file(folder,file):
    spark = create_spark_session()
    name = os.path.join(folder, file)
    df = spark.read.csv(name,inferSchema=True, header =True)
    return df

"""
Function : calculate_time_span(dataset)

Parameters :
dataset : spark dataframe

Return : earliest day, latest day, number of day in the span

Usage :
This function gives the minimum and maximum day, as well as the number of days in a dataframe column with datetimes.
"""

def calculate_time_span(dataset):
  dataset2 = dataset.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
  min = dataset2.agg({"Date": "min"}).collect()[0][0]
  max = dataset2.agg({"Date": "max"}).collect()[0][0]

  day_difference = max - min

  return min, max, day_difference

"""
Function : calcultate_corr(dataset)

Parameters :
dataset : spark dataframe

Return : a correlation matrix

Usage :
This function gives the correlation matrix for a dataset.
"""

def calcultate_corr(dataset):
  # convert to vector
  vector_col = "correlations"
  numeric = dataset.drop(*["Date", "company_name"])

  assembler = VectorAssembler(inputCols=numeric.columns, outputCol=vector_col)
  output = assembler.transform(numeric)

  matrix = Correlation.corr(output, vector_col)
  matrix2 = matrix.collect()[0]['pearson({})'.format(vector_col)].values

  num_columns = len(numeric.columns)
  correlation_matrix = np.array(matrix2).reshape((num_columns, num_columns))

  return correlation_matrix

"""
Function : get_info_df(dataset)

Parameters :
dataset : spark dataframe
file : name of the file

Return : null

Usage :
Gives many insights about a dataframe: schema, top 10 rows, number of values, time span the data is about, data type for each column, missing values, correlation matrix.
"""

def get_info_df(dataset,file):
  print(f"Infos of the {file} file \n\n")

  print("► Schema of the dataset")
  print(dataset.printSchema())

  print("\n ► Top rows of the dataset")
  print(dataset.show(10))

  print("\n ► Row count")
  print(dataset.count())

  print("\n ► Time span")
  min_date, max_date, span = calculate_time_span(dataset)
  print(f"Minimum date {min_date}")
  print((f"Maximum date {max_date}"))
  print(span)

  print("\n ► Columns info")
  for name, col in zip(dataset.schema.names, dataset.columns):
    print(name)
    print(dataset.describe([col]).show())

  print("\n ► Missing values for each column")
  for col in dataset.columns:
    print(col, "\t", "number of null values: ", dataset.filter(dataset[col].isNull()).count())

  print("\n ► Correlation Matrix")
  correlation = calcultate_corr(dataset)
  print(correlation)

"""
Function : get_average_year(dataset, column)

Parameters :
dataset : spark dataframe
column : a column of the dataframe

Return : a dataframe column grouped by year

Usage :
Gives the average value of a column per year.
"""

def get_average_year(dataset, column):
  poulet = dataset.withColumn("Date", col("Date").cast("Date"))

  toAverage = poulet.groupBy(year("date").alias("year")).agg(avg(column).alias(f"average_{column}"))
  toAverage = toAverage.orderBy(year("date"))

  return toAverage

"""
Function : get_average_month(dataset, column)

Parameters :
dataset : spark dataframe
column : a column of the dataframe

Return : a dataframe column grouped by month

Usage :
Gives the average value of a column per month.
"""

def get_average_month(dataset, column):
  poulet = dataset.withColumn("Date", col("Date").cast("Date"))

  toAverage = poulet.groupBy(year("date").alias("year"), month("date").alias("month")).agg(avg(column).alias(f"average_{column}"))
  toAverage = toAverage.orderBy(year("date"), month("date"))

  return toAverage

"""
Function : get_average_week(dataset, column)

Parameters :
dataset : spark dataframe
column : a column of the dataframe

Return : a dataframe column grouped by week

Usage :
Gives the average value of a column per week.
"""

def get_average_week(dataset, column):
  poulet = dataset.withColumn("Date", col("Date").cast("Date"))

  toAverage = poulet.groupBy(year("date").alias("year"), weekofyear("date").alias("week")).agg(avg(column).alias(f"average_{column}"))
  toAverage = toAverage.orderBy(year("date"), weekofyear("date"))

  return toAverage

"""
Function : daily_difference(dataset)

Parameters :
dataset : spark dataframe

Return : spark dataframe

Usage :
Returns a new dataframe with the column "stock_difference" being the difference on closing values over 2 consecutive days.
"""

def daily_difference(dataset):
  windowSpec = Window().orderBy("date")

  dataset_day_diff = dataset.withColumn("stock_difference", F.col("Close") - F.lag("Close").over(windowSpec))

  return dataset_day_diff

"""
Function : monthly_difference(dataset)

Parameters :
dataset : spark dataframe

Return : spark dataframe

Usage :
Returns a new dataframe with the column "stock_difference" being the difference on closing values over 2 consecutive months.
"""

def monthly_difference(dataset):
  window_spec = Window.partitionBy(F.year("date"), F.month("date")).orderBy("date")

  df_with_row_number = dataset.withColumn("row_number", F.row_number().over(window_spec))
  result_df = df_with_row_number.filter(F.col("row_number") == 1).drop("row_number")

  windowSpec2 = Window().orderBy("date")

  dataset_month_diff = result_df.withColumn("stock_difference", F.col("Close") - F.lag("Close").over(windowSpec2))

  return dataset_month_diff

"""
Function : calculate_daily_return(dataset)

Parameters :
dataset : spark dataframe

Return : spark dataframe

Usage :
Returns a new dataframe with the column "Daily_return" being the daily return of a stock (difference of closing and opening values).
"""

def calculate_daily_return(dataset):
  newDataset = dataset.withColumn("Daily_return",  col("Close") - col("Open"))

  return newDataset

"""
Function : get_highest_dr(dataset, start_date=None, end_date=None)

Parameters :
dataset : spark dataframe
start_date (optional): give the start date for a time span to look after
end_date (optional): give the end date for a time span to look after

Return : spark dataframe

Usage :
Function giving the highest daily returns over a time period. If no start and end date are inputted as parameters, the function will simply take the whole dataset by default.
"""

def get_highest_dr(dataset, start_date=None, end_date=None):
    if "Daily_return" in dataset.columns:
        if start_date is not None and end_date is not None:
            filteredDataset = dataset.filter((col("Date") >= start_date) & (col("Date") <= end_date))
            sortedAndFilteredDataset = filteredDataset.orderBy(col("Daily_return").desc())
            return sortedAndFilteredDataset
        else:
            sortedDataset = dataset.orderBy(col("Daily_return").desc())
            return sortedDataset
    else:
        print("Error: 'Daily_return' column not found.")
        return None

"""
Function : calculate_moving_average(df, column_name, n, start_date=None)

Parameters :
df : spark dataframe
column_name : name of the column to use the rolling average technique on
n : number of points to use for the moving average
start_date (optional): give the reference date as a starting point for the moving average

Return : spark dataframe

Usage :
This function calculates the moving average starting a defined date and with a certain number of n points. It then displays the results in a new column.
"""

def calculate_moving_average(df, column_name, n, start_date=None):

    window_spec = Window.orderBy("date").rowsBetween(-n + 1, 0)

    moving_avg_col = f"{column_name}_moving_avg"
    df_with_moving_avg = df.withColumn(moving_avg_col, F.avg(column_name).over(window_spec))

    if start_date is not None:
        result_df = df_with_moving_avg.filter(F.col("date") >= start_date)
    else:
        result_df = df_with_moving_avg

    return result_df

"""
Function : get_ror_daytoday(dataset, reference_date, period)

Parameters :
dataset : spark dataframe
reference_date : reference date to calculate the rate of return
period : integer, number of day to calculate the RoR on

Return : spark dataframe

Usage :
This function calculates the return rate on a whole period based on the value of the reference date.
"""

def get_ror_period(dataset, reference_date, period):
    spark = SparkSession.builder.getOrCreate()

    dataset = dataset.withColumn("date", F.to_date("date"))

    end_date = reference_date + F.expr(f"INTERVAL {period} DAYS")

    filtered_data = dataset.filter((F.col("date") >= reference_date) & (F.col("date") <= end_date))

    return_rate_column = "return_rate"
    initial_close_price = filtered_data.filter(F.col("date") == reference_date).select("close").collect()[0][0]

    dataset = (filtered_data
               .withColumn(return_rate_column,
                           (F.col("close") - F.lit(initial_close_price)) / F.lit(initial_close_price))
               .orderBy("date"))

    return dataset

"""
Function : calculate_stock_correlation(stock1, stock2, start_date=None, end_date=None)

Parameters :
stock1 : spark dataframe
stock2 : spark dataframe
start_date (optional): give the start date for a time span to look after
end_date (optional): give the end date for a time span to look after

Return : correlation coefficient

Usage :
This function returns the correlation coefficient for 2 stocks on a given time span.
"""

def calculate_stock_correlation(stock1, stock2, start_date=None, end_date=None):

    stock1 = stock1.select("date", "close").withColumnRenamed("close", "close1")
    stock2 = stock2.select("date", "close").withColumnRenamed("close", "close2")

    if start_date and end_date:
        stock1 = stock1.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
        stock2 = stock2.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))

    joined_data = stock1.join(stock2, "date", "inner")

    correlation = joined_data.corr("close1", "close2")

    return correlation

"""
Function : get_trading_value(dataset)

Parameters :
dataset : spark dataframe

Return : spark dataframe

Usage :
Returns a new dataframe with the column "trading_value" being the actual total trading value for the day (closing value for one stock * volume of stocks).
"""

def get_trading_value(dataset):

  df = dataset.withColumn("trading_value", col("Close") * col("Volume"))
  return df

"""
Function : plot_stock()

Parameters :
None

Return : plotly plot

Usage :
Generate a plotly graph object.
"""

def plot_stock():
  fig = go.Figure()
  fig.update_layout(title='Stocks over time span', xaxis_title='Date', yaxis_title='Close')

  return fig

"""
Function : add_trace_plot(dataset, fig)

Parameters :
dataset : spark dataframe
column : column from the dataframe
fig : plotly graph object
name : name to give to the trace

Return : updated plotly GO

Usage :
Adds a line trace to a plotly graph object. Useful to plot the stock evolution for each company and on the same plot.
"""

def add_trace_plot(dataset, column, fig, name):
  df_pandas = dataset.toPandas()
  fig.add_trace(go.Scatter(x=df_pandas['Date'], y=df_pandas[column], mode='lines', name=name))
  return fig





