import numpy as np
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from config import USER, DB, PORTA, PASSWORD


class CarregaMySQL:
    def __init__(self, caminho_dataset) -> None:
        self.conexao, self.conc_alch = self.conect_mysql()
        self.csv_name = caminho_dataset
        self.le_csv()

    def conect_mysql(self):
        conexao, conexao_alchemy = None, None
        try:
            conexao = mysql.connector.connect(
                host=PORTA,
                database=DB,
                user=USER,
                password=PASSWORD)
            conexao_alchemy = create_engine(
                url = f"mysql+mysqlconnector://{USER}:{PASSWORD}@{PORTA}/{DB}"
            )
            print("conexão estabelecida com sucesso")

        except Exception as e: 
            print("erro ao conectar ao Data base ", e)
        return conexao, conexao_alchemy
    
    def execute_query(self,sql):
        try:
            with self.conexao as cur:
                cursor = cur.cursor() 
                cursor.execute(sql)
                print("comando executado com sucesso")            
        except Exception as e:
            print("erro ao executar a query ", e)

    def insert(self,df):
        try:
            df.to_sql(self.tb_name,con=self.conc_alch,if_exists="replace",index=False)
            print("dados inseridos com sucesso")
        except Exception as e: 
            print("erro ao realizar a inserção ", e)
    
    def create_table(self, colunas):
        colunas_fomatadas = self.trata_formatacao_coluna(colunas=colunas)
        sql = f"""CREATE TABLE IF NOT EXISTS {self.tb_name} ({colunas_fomatadas})""" 
        self.execute_query(sql)

    def trata_formatacao_coluna(self, colunas):
        string_colunas = ""
        for col in colunas:
            col2 = (col.replace(" ","_")).lower()
            col2 = col2.replace(".", "")
            col2 = col2.replace("(", "")
            col2 = col2.replace(")", "")
            col2 = col2.replace("-", "_")
            col2 = f"{col2} VARCHAR(100)," 
            string_colunas += col2
        string_colunas = string_colunas[:-2] 
        string_colunas += ')'
        return string_colunas

    
    def le_csv(self):
        self.tb_name = ((self.csv_name.split("\/")[2]).split(".")[0]).lower()
        df = pd.read_csv(self.csv_name)
        #Removendo colunas duplicadas do df
        df = df.T.drop_duplicates().T
        print(df)
        colunas = df.columns.to_list()
        self.create_table(colunas= colunas)  
        self.insert(df=df)
        
    