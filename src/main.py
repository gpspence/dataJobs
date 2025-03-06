from pathlib import Path
import pickle
import polars as pl
from typing import Tuple, List
from functools import reduce

CWD = Path.cwd()
DATA_DIR: Path = CWD / "data"
SURVEY_PATH: Path = DATA_DIR / "survey"
PICKLE_PATH: Path = DATA_DIR / "columnnames.pickle"

# TODO
# Key questions:
# 1. What are the skills commonly used by data engineers/ scientists?
# 2. What is the salary distribution for data engineers/ scientists?
# 3. Which skills are commonly required in job ads for these roles?
# 4. What are the advertised salaries for these roles?
# 5. Where are the skill gaps (i.e. differences between industry vs ads)?
# Once done, see if there are any other interesting insights, export data as
# json? and create TypeScript D3 application to interactively explore the data


def attempt_read_file(path: Path) -> pl.DataFrame:
    return pl.read_csv(path)


def encode_columns(df: pl.DataFrame, separator: str = ";") -> pl.DataFrame:
    def transform_column(df: pl.DataFrame, col: pl.Expr, col_name: str) -> pl.DataFrame:
        # Split the column by separator
        unique_values = df.select(
            col
            .str
            .split(separator)
            .explode()
            .unique()
        )
        unique_values = unique_values.to_series().to_list()
        # Create new dummy columns for each unique value
        for val in unique_values:
            # Create a new column with 1 if the value is present, else 0
            df = df.with_columns(
                pl.col(col_name)
                .str
                .contains(val)
                .cast(pl.Int8)
                .alias(f"{col_name}_{val}")
            )
        return df

    original_cols = df.clone().columns
    # Apply the transformation to each column in the DataFrame
    for col_name in df.columns:
        df = transform_column(df, pl.col(col_name), col_name)
    df = df.drop(original_cols)
    return df


def clean_df(df: pl.DataFrame) -> Tuple[pl.DataFrame]:
    # Drop rows with no ConvertedCompYearly
    df = (
        df
        .filter(pl.col("ConvertedCompYearly") != "NA")
        .cast({"ConvertedCompYearly": pl.Float32})
    )
    # Filter out rows which: not developers, data jobs
    filter_values: dict = {
        "MainBranch": ["I am a developer by profession"],
        "Employment": ["Employed, full-time"],
        "DevType": [
            "Data or business analyst",
            "Data scientist or machine learning specialist",
            "Data or business analyst",
            "Developer, AI"
            ]
    }
    for k, v in filter_values.items():
        df = df.filter(pl.col(k).is_in(v))
    # Read in list of columns to keep and filter out all others
    with open(PICKLE_PATH, "rb") as f:
        data_eng_skills = pickle.load(f)
    data_eng_skills = [x for x in data_eng_skills if x not in [
        "JobSat", "EmbeddedHaveWorkedWith"]]
    df = df.select(data_eng_skills)
    # Split out labels and get skills into individual columns
    label_columns = ["ConvertedCompYearly"]
    labels = df.select(label_columns)
    drop_label_columns = [x for x in df.columns if x not in label_columns]
    df = encode_columns(
        df
        .select(drop_label_columns)
        .drop_nans()
    )
    return (df, labels)


def get_column_intersection(df_list: List[pl.DataFrame]) -> List[str]:
    columns = [set(df.columns) for df in df_list]
    return reduce(lambda x, y: x & y, columns)


def main():
    all_dataframes = []
    all_labels = []
    for filename in SURVEY_PATH.iterdir():
        if "survey_results_public" in str(filename):
            # Ingest
            df = attempt_read_file(filename)

            # Transform
            df, labels = clean_df(df)
            all_dataframes.append(df)
            all_labels.append(labels)

    # Union together dataframes
    combined_columns = get_column_intersection(all_dataframes)
    all_dataframes = [df.select(combined_columns) for df in all_dataframes]
    combined_df = pl.concat(all_dataframes)
    print(combined_df)
    combined_labels = pl.concat(all_labels)
    print(pl.concat([combined_df, combined_labels], how="horizontal"))



if __name__ == "__main__":
    main()
