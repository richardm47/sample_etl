import requests
import pandas as pd
import numpy as np
import sqlite3

from config import URL

relabel = {
    "Country/Region": "Country_Region",
    "Province/State": "State",
}

##fetch urls
def get_urls():
    urls = []
    response = requests.get(URL)

    for data in response.json():
        if data["name"].endswith(".csv"):
            urls.append(data["download_url"])
    return urls

##Tranform pandas df
def transform_data(data, filename):
    ##rename labels
    for label in data:
        if label in relabel:
            data = data.rename(columns={label: relabel[label]})

    ##return a dataframe with these parameters
    labels = ["Province_State", "Country_Region", "Last_Update", "Confirmed", "Deaths", "Recovered"]
    ##filename is datetime
    if "Last_Update" not in data:
        data["Last_Update"] = pd.to_datetime(filename)

    ##replace columns not in dataframe with nan
    for label in labels:
        if label not in data:
            data[label] = np.nan

    return data[labels]


##Extract data as pandas df
def extract_data(urls):
    all_data = []
    for i, url in list(enumerate(urls)): ##loop through the URLs
        data = pd.read_csv(url) ##EXTRACT csv from url
        filename = url.split("/")[-1].split(".")[0] ##extract date
        data = transform_data(data, filename)
        all_data.append(data)
    return all_data


##load df to sqlite
def load_data(files, db_name):
    conn = sqlite3.connect(f"{db_name}.db") ##create sqlite connection

    flag = True
    for file in files:
        if flag:
            file.to_sql(db_name, con=conn, index=False, if_exists="replace")  ##replace if first entry and table name already exist
            flag = False
        else:
            file.to_sql(db_name, con=conn, index=False, if_exists="append") ##append to table


if __name__ == "__main__":
    urls = get_urls()[:10] ##using only the first 10 urls, can be changed to more urls
    ext_data = extract_data(urls) ##extract & transform data
    load_data(ext_data, "DataWarehouse") ##load data
