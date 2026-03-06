import pandas as pd

FILE_PATH = "schemas/Maestro_BD_Norte 1.xlsx"

df = pd.read_excel(FILE_PATH)

print(df.head())