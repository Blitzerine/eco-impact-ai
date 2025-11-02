from fastapi import FastAPI, UploadFile, File
import pandas as pd
import io
from src.db.connection import get_db
import math

app = FastAPI(title="EcoImpactAI API", version="1.0")

@app.post("/upload-dataset")
async def upload_dataset(file: UploadFile = File(...)):
    try:
        # Read uploaded CSV into pandas
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        print("✅ Received dataset:")
        print(df.head())

        # Connect to DB once
        conn = get_db()
        cur = conn.cursor()

        inserted_count = 0

        for _, row in df.iterrows():
            # safely pull values from row, convert NaN -> None
            year_val = int(row["year"]) if not math.isnan(row["year"]) else None

            co2_val = (
                float(row["co2_ppm"])
                if not (pd.isna(row["co2_ppm"]))
                else None
            )

            gdp_val = (
                float(row["gdp"])
                if not (pd.isna(row["gdp"]))
                else None
            )

            tax_val = (
                float(row["carbon_tax"])
                if not (pd.isna(row["carbon_tax"]))
                else None
            )

            system_val = str(row["System"]) if not pd.isna(row["System"]) else None

            cur.execute(
                """
                INSERT INTO climate_data (year, co2_ppm, gdp, carbon_tax, system)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (year_val, co2_val, gdp_val, tax_val, system_val),
            )

            inserted_count += 1

        conn.commit()
        conn.close()

        return {"status": "ok", "inserted": inserted_count}

    except Exception as e:
        print("❌ Upload error:", e)
        return {"status": "error", "detail": str(e)}
