from fastapi import FastAPI
import joblib
import pandas as pd
import os

app = FastAPI()
model_path = '../models/model.pkl'
model = joblib.load(model_path) if os.path.exists(model_path) else None

@app.post("/predict")
def predict(data: dict):
    if model is None:
        return {"error": "Modelo no disponible"}
    df = pd.DataFrame([data])
    pred = model.predict(df)
    return {"prediction": pred.tolist()}
