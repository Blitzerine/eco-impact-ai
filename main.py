from fastapi import FastAPI
from fastapi.responses import JSONResponse
from src.db.connection import get_db

app = FastAPI(
    title="Eco-Impact AI API",
    description="Climate policy simulation backend (Supabase + ML)",
    version="0.1.0"
)

@app.get("/health")
def health():
    conn = get_db()
    if conn is None:
        return JSONResponse({"status": "DB not connected"}, status_code=500)
    return {"status": "ok"}

@app.post("/simulate")
def simulate():
    # Week 4+:
    # return model prediction for a given policy scenario
    return {"message": "simulate endpoint placeholder"}

@app.post("/upload-dataset")
def upload_dataset():
    # Week 2:
    # you'll accept cleaned dataset rows and insert them into Supabase
    return {"message": "dataset upload placeholder"}
