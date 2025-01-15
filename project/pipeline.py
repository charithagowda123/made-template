import pandas as pd
import os
import requests

print(
    "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau."
)

urls = {
    "registration_records": "https://data.ny.gov/resource/vw9z-y4t7.csv",
    "rural": "https://api.census.gov//data/2020/dec/dhc?get=group(H2)&ucgid=pseudo(0400000US36$0500000)",
    "education": "https://api.census.gov//data/2023/acs/acsse?get=group(K201501)&ucgid=pseudo(0400000US36$0500000)",
    "age_sex": "https://api.census.gov//data/2022/acs/acs5/subject?get=group(S0101)&ucgid=pseudo(0400000US36$0500000)",
    "race": "https://api.census.gov//data/2022/acs/acs5?get=group(B02001)&ucgid=pseudo(0400000US36$0500000)",
    "income": "https://api.census.gov//data/2022/acs/acs5/subject?get=group(S1901)&ucgid=pseudo(0400000US36$0500000)",
}

base_path = "."


def normalise_county_name(s):
    s = s.str.replace(" County, New York", "").str.upper()
    translation_table = str.maketrans({".": None, ",": None, " ": None})
    s = s.str.translate(translation_table)
    return s


def download_registration_records(url):
    # Download the registration records
    try:
        if not os.path.exists(base_path + "/data/raw/registration_records.csv"):
            print("Downloading registration records...")
            r = requests.get(url)
            with open(base_path + "/data/raw/registration_records.csv", "wb") as f:
                f.write(r.content)
    except:
        raise Exception("Failed to download registration records")
    try:
        df = pd.read_csv(base_path + "/data/raw/registration_records.csv")
        df = df[df["record_type"] == "VEH"]
        # Count Electric Vehicles and other vehicles by County
        df = df.groupby("county").apply(
            lambda x: pd.Series(
                {
                    "Electric Vehicles": x["registration_class"][
                        x["fuel_type"] == "ELECTRIC"
                    ].sum(),
                    "Other Vehicles": x["registration_class"][
                        x["fuel_type"] != "ELECTRIC"
                    ].sum(),
                }
            ),
            include_groups=False,
        )
        df["Total Vehicles"] = df["Electric Vehicles"] + df["Other Vehicles"]
        df["EV_Percent"] = df["Electric Vehicles"] / df["Total Vehicles"]
        df = df.drop("OUT-OF-STATE")
        df.index = normalise_county_name(df.index)
        df = df["EV_Percent"]
        # print(df)
        # print((df["EV_Percent"]*df["Total Vehicles"]).sum()/df["Total Vehicles"].sum())
        print("Saving processed data...")
        df.to_csv(base_path + "/data/processed/registration_records.csv")
        return df
    except:
        raise Exception("Failed to parse registration records")


def download_from_census(url, filename):
    try:
        if not os.path.exists(base_path + "/data/raw/" + filename):
            print(f"Downloading {filename} data from census...")
            r = requests.get(url)
            with open(base_path + "/data/raw/" + filename + ".json", "wb") as f:
                f.write(r.content)
    except:
        raise Exception(f"Failed to download {filename} data from census")
    try:
        df = pd.read_json(base_path + "/data/raw/" + filename + ".json")
        df.columns = df.iloc[0]
        df = df.drop(0)
        df = df.rename(columns={"NAME": "county"})
        df = df.set_index("county")
        df.index = normalise_county_name(df.index)
    except:
        raise Exception(f"Failed to parse {filename} data from census")
    return df


def download_rural(url):
    # Download the rural data
    df = download_from_census(url, "rural")
    try:
        df = df.rename(
            columns={
                "H2_001N": "Total Households",
                "H2_002N": "Urban Households",
                "H2_003N": "Rural Households",
            }
        )
        df = df.drop(
            columns=[
                "GEO_ID",
                "H2_001NA",
                "H2_002NA",
                "H2_003NA",
                "H2_004N",
                "H2_004NA",
                "ucgid",
            ]
        )
        df = df.apply(pd.to_numeric)
        df["Rural Percent"] = df["Rural Households"] / df["Total Households"]
        df = df["Rural Percent"]
        df.to_csv(base_path + "/data/processed/rural.csv")
        print(df)
        return df
    except:
        raise Exception("Failed to parse rural data")


def download_education(url):
    # Download the education data
    df = download_from_census(url, "education")
    try:
        df = df[
            [
                "K201501_001E",
                "K201501_002E",
                "K201501_003E",
                "K201501_004E",
                "K201501_005E",
                "K201501_006E",
                "K201501_007E",
                "K201501_008E",
            ]
        ]
        df = df.rename(
            columns={
                "K201501_001E": "Total",
                "K201501_002E": "Less than 9th Grade",
                "K201501_003E": "9th to 12th Grade, No Diploma",
                "K201501_004E": "High School Graduate (Includes Equivalency)",
                "K201501_005E": "Some College, No Degree",
                "K201501_006E": "Associate's Degree",
                "K201501_007E": "Bachelor's Degree",
                "K201501_008E": "Graduate or Professional Degree",
            }
        )
        df = df.apply(pd.to_numeric)
        df["Average Education Level"] = (
            1 * df["Less than 9th Grade"]
            + 2 * df["9th to 12th Grade, No Diploma"]
            + 3 * df["High School Graduate (Includes Equivalency)"]
            + 4 * df["Some College, No Degree"]
            + 5 * df["Associate's Degree"]
            + 6 * df["Bachelor's Degree"]
            + 7 * df["Graduate or Professional Degree"]
        ) / df["Total"]
        df = df["Average Education Level"]
        df.to_csv(base_path + "/data/processed/education.csv")
        print(df)
        return df
    except:
        raise Exception("Failed to parse education data")


def download_age_sex(url):
    df = download_from_census(url, "age_sex")
    try:
        df = df[["S0101_C01_032E", "S0101_C01_033E"]]
        df = df.rename(
            columns={
                "S0101_C01_032E": "Median Age",
                "S0101_C01_033E": "Females per 100 Males",
            }
        )
        df.to_csv(base_path + "/data/processed/age_sex.csv")
        print(df)
        return df
    except:
        raise Exception("Failed to parse age_sex data")


def download_race(url):
    df = download_from_census(url, "race")
    try:
        df = df[
            ["B02001_002E", "B02001_003E", "B02001_005E", "B02001_007E", "B02001_008E"]
        ]
        df = df.apply(pd.to_numeric)
        df = df.div(df.sum(axis=1), axis=0)
        df = df.rename(
            columns={
                "B02001_002E": "White (%)",
                "B02001_003E": "Black or African American (%)",
                "B02001_005E": "Asian (%)",
                "B02001_007E": "Other (%)",
                "B02001_008E": "Mixed (%)",
            }
        )
        df.to_csv(base_path + "/data/processed/race.csv")
        print(df)
        return df
    except:
        raise Exception("Failed to parse race data")


def download_income(url):
    df = download_from_census(url, "income")
    try:
        df = df["S1901_C01_012E"]
        df.name = "Median Household Income"
        df.to_csv(base_path + "/data/processed/income.csv")
        print(df)
        return df
    except:
        raise Exception("Failed to parse income data")


def create_directories():
    if not os.path.exists(base_path + "/data/raw"):
        os.makedirs(base_path + "/data/raw")
    if not os.path.exists(base_path + "/data/processed"):
        os.makedirs(base_path + "/data/processed")


def main():
    print(
        "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau."
    )
    create_directories()
    df = pd.DataFrame()
    df = df.join(
        download_registration_records(urls["registration_records"]),
        how="outer",
    )
    df = df.join(download_rural(urls["rural"]))
    df = df.join(download_education(urls["education"]))
    df = df.join(download_age_sex(urls["age_sex"]))
    df = df.join(download_race(urls["race"]))
    df = df.join(download_income(urls["income"]))
    df.to_csv(base_path + "/data/results.csv")


if __name__ == "__main__":
    main()
