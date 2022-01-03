# Data-pipeline-Focused

Create Data pipeline (DAG) in Python Language which runs once a week with following steps:

- Retrieve all recipes for current week menu using API call (Review [Mock API](https://hellofresh-au.free.beeceptor.com/menus/2021-W10/classic-box))
- For each recipe extract following information from API resposen: `name`, `headline`, `prepTime`, `ratingsCount`, `favoritesCount`, `nutrition - Energy (kJ)`, etc.
- Data must be flattened into a CSV file. CSV Filename: `YYYY_WW_menu.csv`.
- Identify top 10 recipes based on `ratingsCount`, `favoritesCount` and export them a file `YYYY_WW_TOP_10.csv`. 
- Result CSV must be uploaded to the following s3 location `s3_bucket/YYYY_WW_menu.csv` and `s3_bucket/YYYY_WW_TOP_10.csv`.

# Overall System Design

![Prototype - diagram sieci AWS](https://user-images.githubusercontent.com/34445145/147894558-3a1c7c80-0d28-4a6e-8530-ca093c20c613.png)


# Installation



# Execution


