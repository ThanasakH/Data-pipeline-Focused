from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import os
import datetime
import pandas as pd
import requests
import json
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError


# initialise constant varibles
class AWS_Config:
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

MOCK_API = True


# For PythonOperator
def get_recipes_from_api(execution_year, execution_week, recipes_output_path):
    # assign current year & week for the process if there is no argument passing
    if execution_year == "" or execution_week == "":
        execution_year = datetime.datetime.today().strftime("%Y")
        execution_week = datetime.datetime.today().strftime("%W")
    recipes_output_path = recipes_output_path.format(execution_year, execution_week)
    GET_RECIPIES_URL = "https://hellofresh-au.free.beeceptor.com/menus/{0}-W{1}/classic-box".format(2021 if MOCK_API else EXECUTION_YEAR,
                                                                                                    10 if MOCK_API else EXECUTION_WEEK)
    print("Execution process for {0}-{1}.".format(execution_year, execution_week))

    # retrieve data using Web API
    r = requests.get(GET_RECIPIES_URL)
    result_api = r.json()

    # convert result to dataframe
    df = pd.DataFrame(result_api.get("items")[0].get("courses"))

    # flatten recipe data and convert to new dataframe
    json_struct = json.loads(df.recipe.to_json(orient="records"))
    df_recipe = pd.io.json.json_normalize(json_struct) 

    # select focusing columns
    cols = ['name', 'headline', 'prepTime', 'ratingsCount', 'favoritesCount', 'websiteUrl']
    df_recipe = df_recipe[cols]

    # web scraping nutrition via websiteUrl
    df_recipe["nutrition"] = df_recipe.apply(lambda x: get_nutrition(x.websiteUrl), axis=1)

    # flatten nutrition data into dataframe
    json_struct = json.loads(df_recipe.to_json(orient="records"))
    df_recipe = pd.io.json.json_normalize(json_struct) 

    # transform columns into proper format
    cols = ["prepTime", "nutrition.Calories", "nutrition.Fat", "nutrition.Saturated Fat", "nutrition.Carbohydrate", "nutrition.Sugar", "nutrition.Dietary Fiber", "nutrition.Protein", "nutrition.Cholesterol", "nutrition.Sodium"]
    new_cols = ["prepTime (M)", "nutrition - Energy (kJ)", "nutrition - Fat (g)", "nutrition - Saturated Fat (g)", "nutrition - Carbohydrate (g)", "nutrition - Sugar (g)", "nutrition - Dietary Fiber (g)", "nutrition - Protein (g)", "nutrition - Cholesterol (mg)", "nutrition - Sodium (mg)"]
    df_recipe = format_columns(df_recipe, cols, new_cols)

    # assign datatypes for better display
    df_recipe = df_recipe.astype({"ratingsCount":pd.Int64Dtype(),
                                "favoritesCount":pd.Int64Dtype()
                                })

    # save dataframe to csv file
    df_recipe.drop(columns=["websiteUrl"]).sort_values("name", ascending=True).to_csv(recipes_output_path, index=False)

def get_nutrition(url):
    print("get_nutrition is processing.. [{0}]".format(url))

    # retrieve html data
    r = requests.get(url)
    soup = BeautifulSoup(r.content, "html.parser")

    # seek for Nutrition Values tag
    divs = soup.findAll("div", attrs = {"class": "fela-_1qmjd6x"})
    
    # extract focusing information eg. Energy, Fat, Saturated Fat, etc.
    if len(divs) > 0:
        nutrition_divs = divs[0].findChild().find_all("div")[0:-1]
        #convert into dictionary and return
        result = {div.find_all("span")[0].get_text(): div.find_all("span")[1].get_text() for div in nutrition_divs}
        return result
    return {}

def format_columns(df, col_names, new_col_names):
    # map column names with new column names and convert into dict
    col_dict = dict(zip(col_names, new_col_names))
    
    # assign datatypes to string
    df = df.astype({col:str for col in col_dict.keys()})
    
    # extract values from string
    for col in col_dict.keys():
        if col == "nutrition.Calories":
            # convert kilojoules to kilocalories as the requirement indicated
            df[col] = df.apply(lambda x: float(x[col].split()[0]) * 4.184 if x[col]!="nan" else pd.np.NaN , axis=1)
        elif col == "prepTime":
            df[col] = df.apply(lambda x: x[col][2:4] if len(x[col])==5 else pd.np.NaN , axis=1)
        else:
            df[col] = df.apply(lambda x: x[col].split()[0] if x[col]!="nan" else pd.np.NaN , axis=1)
            
    # rename columns and return
    return df.rename(columns=col_dict)
    

def get_top10_recipes(execution_year, execution_week, recipes_output_path, top10_recipes_output_path):
    # assign current year & week for the process if there is no argument passing
    if execution_year == "" or execution_week == "":
        execution_year = datetime.datetime.today().strftime("%Y")
        execution_week = datetime.datetime.today().strftime("%W")
    recipes_output_path = recipes_output_path.format(execution_year, execution_week)
    top10_recipes_output_path = top10_recipes_output_path.format(execution_year, execution_week)
    print("Execution process for {0}-{1}.".format(execution_year, execution_week))

    # load data from csv
    df_recipe = pd.read_csv(recipes_output_path)

    # rank dataframe base on ratingsCount
    df_base_ratingsCount = df_recipe.sort_values("ratingsCount", ascending=False)
    df_base_ratingsCount['rank'] = df_base_ratingsCount['ratingsCount'].rank(ascending=False, method='min')
    df_base_ratingsCount = df_base_ratingsCount.head(10)
    df_base_ratingsCount["rankBaseOn"] = "ratingsCount"
    df_base_ratingsCount.reset_index(inplace=True, drop=True)

    # rank dataframe base on favoritesCount
    df_base_favoritesCount = df_recipe.sort_values("favoritesCount", ascending=False)
    df_base_favoritesCount['rank'] = df_base_favoritesCount['favoritesCount'].rank(ascending=False, method='min')
    df_base_favoritesCount = df_base_favoritesCount.head(10)
    df_base_favoritesCount["rankBaseOn"] = "favoritesCount"
    df_base_favoritesCount.reset_index(inplace=True, drop=True)

    # union the ranking results
    df_recipe = pd.concat([df_base_ratingsCount, df_base_favoritesCount])

    # assign datatypes
    df_recipe = df_recipe.astype({"rank":pd.Int64Dtype(),
                                "ratingsCount":pd.Int64Dtype(),
                                "favoritesCount":pd.Int64Dtype()
                                })
    # reindex columns
    cols = ["rank", "rankBaseOn"] + list(df_recipe.columns[:-2])
    df_recipe = df_recipe.reindex(columns=cols)

    # save dataframe to csv file
    df_recipe.to_csv(top10_recipes_output_path, index=False)

def upload_to_s3(execution_year, execution_week, local_file, s3_file):
    # assign current year & week for the process if there is no argument passing
    if execution_year == "" or execution_week == "":
        execution_year = datetime.datetime.today().strftime("%Y")
        execution_week = datetime.datetime.today().strftime("%W")
    local_file = local_file.format(execution_year, execution_week)
    s3_file = s3_file.format(execution_year, execution_week)
    print("Execution process for {0}-{1}.".format(execution_year, execution_week))

    # create instance with AWS access_key & secret_key
    s3 = boto3.client('s3', 
                      aws_access_key_id=AWS_Config.ACCESS_KEY, 
                      aws_secret_access_key=AWS_Config.SECRET_KEY
                      )
    try:
        # upload file to s3 bucket
        s3.upload_file(local_file, AWS_Config.BUCKET_NAME, s3_file)
        print("Upload Successful [{0}]".format(s3_file))
    except FileNotFoundError:
        # print("The file was not found [{0}]".format(local_file))
        raise("The file was not found [{0}]".format(local_file))
    except NoCredentialsError:
        raise("Credentials not available")



# Default Args
default_args = {
    'owner': 'thanasak.ha',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime.datetime(2022, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    dag_id='Recipes_pipeline',
    default_args=default_args,
    description='Pipeline for ETL recipes and top10 recipes data',
    schedule_interval="@weekly",
    start_date=datetime.datetime(2022, 1, 1),
    catchup=False,
)

# Tasks
get_recipes_from_api = PythonOperator(
    task_id='get_recipes_from_api',
    python_callable=get_recipes_from_api,
    op_kwargs={ 
                "execution_year": '{{ dag_run.conf.get("execution_year", "") }}',
                "execution_week": '{{ dag_run.conf.get("execution_week", "") }}',
                "recipes_output_path": "/home/airflow/data/{0}_{1}_menu.csv"
                },
    dag=dag,
)

get_top10_recipes = PythonOperator(
    task_id='get_top10_recipes',
    python_callable=get_top10_recipes,
    op_kwargs={ 
                "execution_year": '{{ dag_run.conf.get("execution_year", "") }}',
                "execution_week": '{{ dag_run.conf.get("execution_week", "") }}',
                "recipes_output_path": "/home/airflow/data/{0}_{1}_menu.csv",
                "top10_recipes_output_path": "/home/airflow/data/{0}_{1}_TOP_10.csv"
                },
    dag=dag,
)

upload_recipes_to_s3 = PythonOperator(
    task_id='upload_recipes_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={ 
                "execution_year": '{{ dag_run.conf.get("execution_year", "") }}',
                "execution_week": '{{ dag_run.conf.get("execution_week", "") }}',
                "local_file": "/home/airflow/data/{0}_{1}_menu.csv",
                "s3_file": "{0}_{1}_menu.csv",
                },
    dag=dag,
)

upload_top10_recipes_to_s3 = PythonOperator(
    task_id='upload_top10_recipes_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={ 
                "execution_year": '{{ dag_run.conf.get("execution_year", "") }}',
                "execution_week": '{{ dag_run.conf.get("execution_week", "") }}',
                "local_file": "/home/airflow/data/{0}_{1}_TOP_10.csv",
                "s3_file": "{0}_{1}_TOP_10.csv",
                },
    dag=dag,
)

# Dependencies
get_recipes_from_api >> get_top10_recipes >> [upload_recipes_to_s3, upload_top10_recipes_to_s3]
