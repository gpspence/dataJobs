from pathlib import Path
from pyspark.sql import types as T, functions as F, SparkSession

CWD = Path.cwd()
DATA_DIR: Path = CWD / "data"
SURVEY_PATH: Path = DATA_DIR / "survey_results_public.csv"
SURVEY_SCHEMA: Path = DATA_DIR / "survey_results_schema.csv"
JOBAD_PATH: Path = DATA_DIR / "train-00000-of-00001.parquet"


spark = SparkSession \
    .builder \
    .appName("Main") \
    .getOrCreate()
sc = spark.sparkContext

# TODO
# Key questions:
# 1. What are the skills commonly used by data engineers/ scientists?
# 2. What is the salary distribution for data engineers/ scientists?
# 3. Which skills are commonly required in job ads for these roles?
# 4. What are the advertised salaries for these roles?
# 5. Where are the skill gaps (i.e. differences between industry vs ads)?
# Once done, see if there are any other interesting insights, export data as
# json? and create TypeScript D3 application to interactively explore the data


survey = (
    spark
    .read
    .option("header", True)
    .csv(str(SURVEY_PATH))  # stackoverflow survey 2024
)
survey_schema = (
    spark
    .read
    .option("header", True)
    .csv(str(SURVEY_SCHEMA))  # stackoverflow survey schema
)

jobs = (
    spark
    .read
    .option("header", True)
    .parquet(str(JOBAD_PATH))
)
print(survey_schema.show(survey_schema.count()))
