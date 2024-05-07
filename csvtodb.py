import sqlite3

class DBPop:
    def __init__(self, db_file_name, csv_path):
        self.db_file_name = db_file_name
        self.csv_path = csv_path

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

    def import_csvs(self):
        conn = sqlite3.connect(self.db_file_name)
        cur = conn.cursor()
        if not self.db_status():
            cur.execute("CREATE TABLE transferencias(tramite_tipo, tramite_fecha)")
            conn.commit()
        conn.close()

populator = DBPop("transferencias.db", "~/pipi")
populator.import_csvs()