import pandas as pd
import numpy as np
import os

def load_with_real_header(path, skiprows=3):
    raw = pd.read_csv(path, skiprows=skiprows, header=None)

    # first row is header
    header = raw.iloc[0].tolist()
    df = raw[1:].copy()
    df.columns = header

    # drop empty columns
    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")

    # reset index just to keep things tidy
    df = df.reset_index(drop=True)
    return df

def get_last_numeric_per_row(df, system_col_name_guess_list):
    # 1. Find the column that holds the readable system/policy name
    system_col = None
    for guess in system_col_name_guess_list:
        if guess in df.columns:
            system_col = guess
            break
    if system_col is None:
        # if not found, just take the first column
        system_col = df.columns[0]

    # 2. All other columns: try to convert to numeric to detect numeric history columns
    numeric_part = df.drop(columns=[system_col]).apply(pd.to_numeric, errors="coerce")

    # 3. "last known value" = take the last non-null value across each row
    last_values = numeric_part.apply(lambda row: row.dropna().iloc[-1] if row.dropna().shape[0] > 0 else np.nan, axis=1)

    # return a frame with system name and that last numeric snapshot
    out = pd.DataFrame({
        "System": df[system_col],
        "LatestValue": last_values
    })
    return out

# ---- Load each dataset ----

emissions_df = load_with_real_header("data/data_08_2025_Compliance_Emissions.csv", skiprows=3)
price_df     = load_with_real_header("data/data_08_2025_Compliance_Price.csv",     skiprows=3)
revenue_df   = load_with_real_header("data/data_08_2025_Compliance_Revenue.csv",   skiprows=3)

# ---- Extract most recent snapshot per policy/system ----

emissions_latest = get_last_numeric_per_row(
    emissions_df,
    system_col_name_guess_list=[
        "System", "ETS_CA_Alberta", "Austria ETS", "Alberta TIER"
    ]
)
emissions_latest = emissions_latest.rename(columns={"LatestValue": "co2_ppm"})

price_latest = get_last_numeric_per_row(
    price_df,
    system_col_name_guess_list=[
        "Alberta TIER", "System", "ETS_CA_Alberta"
    ]
)
price_latest = price_latest.rename(columns={"LatestValue": "carbon_tax"})

revenue_latest = get_last_numeric_per_row(
    revenue_df,
    system_col_name_guess_list=[
        "Alberta TIER", "System"
    ]
)
revenue_latest = revenue_latest.rename(columns={"LatestValue": "gdp"})

# ---- Merge them by System ----
merged = emissions_latest.merge(price_latest, on="System", how="outer")
merged = merged.merge(revenue_latest, on="System", how="outer")

# Add a static "year" column for now since we’re treating these as latest (2025 snapshot)
merged["year"] = 2025

# Clean up: drop rows where System is missing
merged = merged.dropna(subset=["System"])

# OPTIONAL: round floats for cleanliness
merged["co2_ppm"] = merged["co2_ppm"].astype(float).round(6)
merged["carbon_tax"] = merged["carbon_tax"].astype(float).round(6)
merged["gdp"] = merged["gdp"].astype(float).round(6)

# Reorder columns to match your FastAPI `/upload-dataset` expectation
final_cols = ["year", "co2_ppm", "gdp", "carbon_tax", "System"]
merged = merged[final_cols]

# Save
os.makedirs("data", exist_ok=True)
output_path = "data/cleaned_dataset.csv"
merged.to_csv(output_path, index=False)

print("✅ DONE. Saved cleaned dataset →", output_path)
print("Total rows:", len(merged))
print("\nPreview:")
print(merged.head(15))
