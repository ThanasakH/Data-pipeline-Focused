# Data-pipeline Focused

Create Data pipeline (DAG) in Python Language which runs once a week with the following steps:

- Retrieve all recipes for current week menu using API call (Review [Mock API](https://hellofresh-au.free.beeceptor.com/menus/2021-W10/classic-box))
- For each recipe extract following information from API resposen: `name`, `headline`, `prepTime`, `ratingsCount`, `favoritesCount`, `nutrition - Energy (kJ)`, etc.
- Data must be flattened into a CSV file. CSV Filename: `YYYY_WW_menu.csv`.
- Identify top 10 recipes based on `ratingsCount`, `favoritesCount` and export them a file `YYYY_WW_TOP_10.csv`. 
- Result CSV must be uploaded to the following s3 location `s3_bucket/YYYY_WW_menu.csv` and `s3_bucket/YYYY_WW_TOP_10.csv`.

# Overall System Design

This project has been designed to demonstrate a Data pipeline solution on the Apache Airflow and the goal is to retrieve recipe data from various data sources, transform data to a proper format, and load data to AWS S3 bucket.

<p align="center"><img src="https://user-images.githubusercontent.com/34445145/147894558-3a1c7c80-0d28-4a6e-8530-ca093c20c613.png"></p>
<p align="center">Figure 1. Overall Architecture.</p>

**Data Source**
- `Web API`: this API provides weekly-recipes data in a JSON format via this [URL](https://hellofresh-au.free.beeceptor.com/menus/2021-W10/classic-box).
- `Website`: the nutritions in recipes can be found on different WebsiteURLs from the Web API ([example](https://www.hellofresh.com/recipes/chicken-sausage-spaghetti-bolognese-611d139a3db57e6fd7172855)) and a web scraping technique is used to extract those data.

**Apache Airflow**
- `Recipes_pipeline(DAG)`: it manipulates the entire workflow. For instance, gathering data from data sources, transforming data, and transferring data to AWS S3 Bucket.

**Destination**
- `AWS S3 Bucket`: the S3 service will store the results in CSV format via the boto3 library.

# How to run this project locally

## Installation and Start Service
Please make sure a Docker has been installed on your machine before moving on next steps.

1. Clone this repository.
```
git clone https://github.com/ThanasakH/Data-pipeline-Focused.git
```

2. Initialise environment file and AWS credentials. In this step, you'll need to enter AWS_ACCESS_KEY, SECRET_KEY, and S3_BUCKET_NAME
```
make init
```

3. Initialise Apache Airflow and build a docker image.
```
make build
```

4. Create containers and starts service.
```
make up
```

Note: Once you would like to stop service and remove containers.
```
make down
```

## Execution

**To manually trigger the task:**
1. Navigate to http://localhost:8080
2. Enter Username: `airflow`
3. Enter Password: `airflow`
4. Click enable `Recipes_pipeline` DAG.
5. Click `Trigger DAG` on the Actions column.

**To rerun the task on specific week:**
1. Navigate to http://localhost:8080
2. Enter Username: `airflow`
3. Enter Password: `airflow`
4. Click enable `Recipes_pipeline` DAG.
5. Click `Trigger DAG w/ config` on the Actions column.
6. Enter {"execution_year":"_YYYY_", "execution_week":"_WW_"}. For example {"execution_year":"2020", "execution_week":"01"}
7. Click `Trigger`

**To run the test:**
1. Navigate to CLI in the container.
2. Enter `pytest`
