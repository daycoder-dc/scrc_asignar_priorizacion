from fastapi import FastAPI, UploadFile, status, Form
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from typing import Annotated
from io import BytesIO

import polars as pl

uri = "postgresql://postgres:qEeSQnleCVRvntMi7MLgXvC9C9IRk6tq@187.124.80.69:5432/scrc_db"

app = FastAPI()

@app.post("/")
async def root(file:Annotated[UploadFile, Form()], origen:Annotated[str, Form()]):
    file_content = await file.read()
    file_buffer = BytesIO(file_content)
    df = pl.read_excel(file_buffer)

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
        "Estado del Servicio": "estado_del_servicio",
        "Deuda_Hoy": "deuda_hoy",
        "Fecha_de_Vencimiento": "fecha_de_vencimiento",
        "Facturas_Vencidas": "facturas_vencidas_2",
        "Nombre_del_Cliente": "nombre_del_cliente_2",
        "Tarifa_1": "tarifa_2",
        "Marca_Medidor": "marca_medidor_2",
        "Numero de medidor": "numero_de_medidor",
        "Municipio_1": "municipio_2",
        "Corregimiento_1": "corregimiento_2",
        "Barrio": "barrio",
        "Dirección": "direccion_2",
        "Multifamiliar": "multifamiliar",
        "Material_en_Bodega": "material_en_bodega",
        "Maestro_Zona": "maestro_zona",
        "Priorización": "priorizacion",
        "Exclusiones Temporal": "exclusiones_temporal",
        "Tipo_Brigada": "tipo_brigada",
        "Cluster": "cluster"
    }

    df = df.rename(map_columns)

    df = df.with_columns(
        pl.lit(origen).alias("origen")
    )
    
    df.write_database(
        table_name="maestro_db",
        connection=uri,
        engine="adbc",
        if_table_exists="append"
    )

    return JSONResponse(
        content=jsonable_encoder(
            {"status": "procesado"}
        ),
        status_code=status.HTTP_202_ACCEPTED
    )