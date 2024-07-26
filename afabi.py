import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from io import StringIO
import os


# Define config as a global variable
config = {}


def load_config(config_path):
    global config
    with open(config_path) as file:
        config = json.load(file)


def process_date(df):
    print("Processing date...")
    for idx, row in df.iterrows():
        try:
            df.at[idx, "日期"] = pd.to_datetime(
                row["日期"], format="%Y/%m/%d"
            ).strftime("%Y/%m/%d")

        except ValueError:
            print(f"Failed to convert row {index}: {row['日期']}\n")
            df.drop(idx, inplace=True)

    print("Processed date")
    return df


def process_03(df, start_date, end_date):
    # 1. 日期 2024/7/1 -> 2024/07/01
    df = process_date(df)

    # 2. 選取日期符合startdate~enddate者 (爬蟲api會select整月資料)
    #    e.g. 原本startdate=2024-06-10, enddate=2024-07-20, 會輸出2024年6月和7月整月的資料
    df = df[
        (df["日期"] >= start_date.replace("-", "/"))
        & (df["日期"] <= end_date.replace("-", "/"))
    ]

    # 3. Unpivot
    print("Unpivoting...")
    cols = ["日期", "作物", "日/旬", "產地平均價格(元/公斤)"]
    df2 = pd.DataFrame(columns=cols)

    # Iterate through each row of the original DataFrame
    for index, row in df.iterrows():
        date = row["日期"]
        # Iterate through all columns of each row (excluding the first column, which is the date)
        for col in df.columns[1:]:
            value = row[col]
            if pd.notna(value):
                # Split the column names (產地平均價格(元/公斤)-旬-甘藍)
                parts = col.split("-")
                if len(parts) >= 3:
                    category = parts[0].strip()  # 產地平均價格(元/公斤)
                    period = parts[1].strip()  # 日or旬
                    crop = "-".join(
                        part.strip() for part in parts[2:]
                    )  # 作物名稱 (產地平均價格(元/公斤)-旬-結球白菜-成功白)

                    # Create a new DataFrame from the dictionary
                    new_row = pd.DataFrame(
                        [
                            {
                                "日期": date,
                                "作物": crop,
                                "日/旬": period,
                                "產地平均價格(元/公斤)": value,
                            }
                        ]
                    )

                    # Add the values to the new DataFrame
                    df2 = pd.concat([df2, new_row], ignore_index=True)

    print("Unpivoted")
    return df2


def crawler(
    config_path,
    function,
    start_date,
    end_date,
    important_code=None,
):
    load_config(config_path)

    # Extract configuration values
    base_url = config["base_url"]

    if not important_code:
        important_code = ",".join(config["important_code"].values())

    # Construct the full URL
    url = f"{base_url}?function={function}&startdate={start_date}&enddate={end_date}&importantcode={important_code}"
    print("URL: ", url, "\n")

    # Make the HTTP request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Assuming the response is a CSV, load it into a pandas DataFrame
        # 刪除備註
        df = pd.read_csv(StringIO(response.text.split("備註,")[0]))
        print("Data downloaded")

        if function == "01":
            df = process_date(df)

        elif function == "02":
            # header最後多一個逗號,使其多一列
            df = df.iloc[:, :-1]

        elif function == "03":
            df = process_03(df, start_date, end_date)

        # Save the data to a CSV file
        path = os.path.join(config["dir"][function], f"afabi_{function}_{end_date}.csv")
        df.to_csv(path, index=False)
        print(f"Data saved to {path}\n")

    else:
        print(f"Failed to retrieve data: {response.status_code}")
