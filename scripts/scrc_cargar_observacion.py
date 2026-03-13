from fastapi import FastAPI, UploadFile, File
from sqlalchemy import create_engine
from io import BytesIO

import pandas as pd

app = FastAPI()
db = create_engine("postgresql://postgres:qEeSQnleCVRvntMi7MLgXvC9C9IRk6tq@187.124.80.69:5432/scrc_db")

@app.post("/import-excel")
async def file_load(file:UploadFile=File(...)):
    file_content = await file.read()
    file_buffer = BytesIO(file_content)

    df = pd.read_excel(file_buffer, engine="openpyxl", keep_default_na=True)
    df = df.where(pd.notnull(df), None)

    map_columns = {
        "NIC": "nic",
        "ORDEN": "orden",
        "CONTRATA": "contrata",
        "TIPO SUSPENSION SOLICITADA": "tipo_suspension_solicitada",
        "TIPO BRIGADA": "tipo_brigada",
        "ACCION": "accion",
        "SUBACCION/SUBANOMALIA": "subaccion_subanomalia",
        "OBS_FECHA": "obs_fecha",
        "OBS_ACTA": "obs_acta",
        "OBS_TECNICO": "obs_tecnico",
        "OBS_PREDIO": "obs_predio",
        "OBS_ATENDIO": "obs_atendio",
        "OBS_LECTURA": "obs_lectura",
        "OBS_SS": "obs_ss",
        "OBS_RI": "obs_ri",
        "OBS_PAS_COMENTARIO": "obs_pas_comentario",
        "OBS_MEDIDOR": "obs_medidor",
        "TL_ESTANDARIZADO": "tl_estandarizado"
    }

    df = df.rename(columns=map_columns)
    df["origen"] = "centro"

    df.to_sql(name="observacion", con=db, if_exists="append", index=False)

    return {
        "status": "Datos insertados."
    }