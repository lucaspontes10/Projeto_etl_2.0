from extracao import Extracao   
from carrega_mysql import CarregaMySQL

def main():
    print("inciando extração")
    extracao = Extracao(data_name="euro-2024-matches",repo_name = "thamersekhri",pasta_name= "dados") 
    extracao.get_dataset()
    print("iniciando carregamento para o Mysql")
    carrega_mysql = CarregaMySQL (caminho_dataset = "dados\/arquivos_extraidos\/Euro_2024_Matches.csv")
    print("processo finalizado !!")
if __name__ == "__main__" :
    main()
