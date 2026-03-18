from fastapi import FastAPI, UploadFile, File
from sqlalchemy import create_engine
from io import BytesIO

import pandas as pd

app = FastAPI()
db = create_engine("postgresql://postgres:qEeSQnleCVRvntMi7MLgXvC9C9IRk6tq@187.124.80.69:5432/scrc_db")

@app.post("/import-excel")
async def file_load(file: UploadFile = File(...)):
    file_contet = await file.read()
    file_buffer = BytesIO(file_contet)
    
    df = pd.read_excel(file_buffer, engine="openpyxl", keep_default_na=True)
    df = df.where(pd.notnull(df), None)

    map_columns = {
        "CUENTA": "cuenta",
        "PRODUCTO": "producto",
        "NOMBRE_CLIENTE": "nombre_cliente",
        "Ejecutivo_cuenta": "ejecutivo_cuenta",
        "DIRECCION_CLIENTE": "direccion_cliente",
        "TERRITORIAL.": "territorial",
        "MUNICIPIO": "municipio",
        "CATEGORIA": "categoria",
        "SUBCATEGORIA": "subcategoria",
        "Fecha Facturación": "fecha_facturacion",
        "Fecha_vencimiento": "fecha_vencimiento",
        "Facturado Energia": "facturado_energia",
        "Facturado irregularidades": "facturado_irregularidades",
        "Facturado sin Irr": "facturado_sin_irr",
        "Facturado Terceros": "facturado_terceros",
        "Con_pago": "con_pago",
        "Fecha_pago": "fecha_pago",
        "Recaudo Energia": "recaudo_energia",
        "Recaudo Corriente": "recaudo_corriente",
        "Recaudo Cartera": "recaudo_cartera",
        "Recaudo 0-30": "recaudo_0_30",
        "Recaudo Energia Irr": "recaudo_energia_irr",
        "Recaudo sin irr": "recaudo_sin_irr",
        "Recaudo Terceros": "recaudo_terceros",
        "Suma de Deuda_Energia (BDEF)": "suma_de_deuda_energia_bdef",
        "Suma de Deuda Total": "suma_de_deuda_total",
        "Suma de Facturas": "suma_de_facturas"
    }

    df = df.rename(columns=map_columns)
    df["origen"] = "centro"

    df.to_sql(name="balanza", con=db, if_exists="append", index=False)

    return {
        "status": "Datos insertados"
    }