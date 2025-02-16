# Workshop Data-Ingestion-with-Data-Load-Tool Homework

Dataset & API
We’ll use NYC Taxi data via the same custom API from the workshop:

🔹 Base API URL:
```
https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
```
🔹 Data format: Paginated JSON (1,000 records per page).
🔹 API Pagination: Stop when an empty page is returned.

Question 1: dlt Version
``` python
# Step 1 - Intsall dlt with duckdb or any datawarehouse as your destination for the assignment we used duck db
!pip install dlt[duckdb]

#Step 2 - Check for version
import dlt
print("dlt version:", dlt.__version__)
```
ANSWER : dlt version: 1.6.1

Question 2: Define & Run the Pipeline (NYC Taxi API)
Use dlt to extract all pages of data from the API and How many tables were created?

Steps:

1️⃣ Use the @dlt.resource decorator to define the API source.

2️⃣ Implement automatic pagination using dlt's built-in REST client.

3️⃣ Load the extracted data into DuckDB for querying.

``` python

# Step 1 
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Step 2
# Define the API resource for NYC taxi data
@dlt.resource(name="rides")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory

# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi"
)

# Step 3
Load the data into DuckDB to test:
load_info = pipeline.run(ny_taxi)
print(load_info)

# Start a connection to your database using native duckdb connection and look what tables were generated:
import duckdb
from google.colab import data_table
data_table.enable_dataframe_formatter()

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()

```
ANSWER : 4


Question 3: Explore the loaded data, What is the total number of records extracted?

``` python
# Inspect the table ride:
df = pipeline.dataset(dataset_type="default").rides.df()
df.info()

```

ANSWER: 10000

Question 4: Trip Duration Analysis , What is the average trip duration?

``` python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)

```
ANSWER: 12.3049
