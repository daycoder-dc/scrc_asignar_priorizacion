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
        "Num_Lote": "num_lote",
        "Técnico": "tecnico",
        "Orden": "orden",
        "Tip_Orden": "tip_orden",
        "NIC": "nic",
        "Zona": "zona",
        "Departamento": "departamento",
        "Municipio": "municipio",
        "Corregimiento": "corregimiento",
        "Localidad": "localidad",
        "Tip_Via": "tip_via",
        "Nom_Calle": "nom_calle",
        "Duplicador": "duplicador",
        "Num_Puerta": "num_puerta",
        "Direccion": "direccion",
        "Dir_Referencia": "dir_referencia",
        "Nombre Cliente": "nombre_cliente",
        "Tarifa": "tarifa",
        "Deuda Vencida": "deuda_vencida",
        "Facturas Vencidas": "facturas_vencidas",
        "N° Medidor": "num_medidor",
        "Marca Medidor": "marca_medidor",
        "Antiguedad": "antiguedad",
        "Estado": "estado",
        "CT Gestor": "ct_gestor",
        "Comentario": "comentario",
        "Tipo_Suspension(SCR)": "tipo_suspension_scr",
        "Estado del Servicio": "estado_del_servicio"
    }

    df = df.rename(columns=map_columns)
    df["origen"] = "centro"

    df.to_sql(name="asignacion", con=db, if_exists="append", index=False)

    return {
        "status": "Datos insertado"
    }

    
