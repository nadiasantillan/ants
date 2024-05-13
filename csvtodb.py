import os
import re
import sqlite3
import pandas as pd

class DBPop:
    def __init__(self, db_file_name, csv_path, csv_pattern):
        self.db_file_name = db_file_name
        self.csv_path = csv_path
        self.csv_pattern = re.compile(csv_pattern)

    def db_status(self):
        tables = []
        conn = sqlite3.connect(self.db_file_name)
        cur = conn.cursor()
        res = cur.execute("SELECT name FROM sqlite_master")
        row = res.fetchone()
        while row:
            tables.append(row[0])
            row = res.fetchone()
        conn.close()        
        return tables == ["transferencias"]
    
    def records_from(self, filename):
        df = pd.read_csv(
               filename,
               sep=",",
               skiprows=0,
               quotechar="\"",
               dtype=str,
               keep_default_na=False)
        return df.rename(columns={"titular_domicilio_provincia_id": "titular_domicilio_provincia_indec_id", "titular_pais_nacimiento_id": "titular_pais_nacimiento_indec_id"})
    
    def files_from_path(self):
        for filename in os.listdir(self.csv_path):
            if self.csv_pattern.search(filename):
                yield "{}/{}".format(self.csv_path, filename)

    def import_csvs(self):
        conn = sqlite3.connect(self.db_file_name)
        cur = conn.cursor()
        if not self.db_status():
            cur.execute("""
                        CREATE TABLE transferencias(
                        tramite_tipo, 
                        tramite_fecha,
                        fecha_inscripcion_inicial,
                        registro_seccional_codigo,
                        registro_seccional_descripcion,
                        registro_seccional_provincia,
                        automotor_origen,
                        automotor_anio_modelo,
                        automotor_tipo_codigo,
                        automotor_tipo_descripcion,
                        automotor_marca_codigo,
                        automotor_marca_descripcion,
                        automotor_modelo_codigo,
                        automotor_modelo_descripcion,
                        automotor_uso_codigo,
                        automotor_uso_descripcion,
                        titular_tipo_persona,
                        titular_domicilio_localidad,
                        titular_domicilio_provincia,
                        titular_genero,
                        titular_anio_nacimiento,
                        titular_pais_nacimiento,
                        titular_porcentaje_titularidad,
                        titular_domicilio_provincia_indec_id,
                        titular_pais_nacimiento_indec_id
                        )""")
            conn.commit()
        query = "insert into transferencias values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        for filename in self.files_from_path():
            self.records_from(filename).to_sql("transferencias", con=conn, index=False, if_exists="append")
            print(filename)
        conn.commit()            
        conn.close()

populator = DBPop("transferencias.db", "/home/nadia/unsl/labodatos/data", "transferencias")
# populator.records_from("/home/nadia/unsl/labodatos/data/dnrpa-transferencias-autos-202001.csv")
populator.import_csvs()
# print(list(populator.files_from_path()))