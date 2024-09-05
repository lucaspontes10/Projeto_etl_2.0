from config import *
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi 
import zipfile

class Extracao:
    def __init__(self,data_name,repo_name,pasta_name) -> None:
        self.api = KaggleApi()
        self.api.authenticate()
        self.dataset_name = data_name
        self.repo_name = repo_name
        self.pasta_name = pasta_name

    def get_dataset(self):
        '''
            meteodo responsavel, por 
            realizar a conexão com o kaggle
            e extrair o dataset.
        ''' 
        dataset_name = f'{self.repo_name}/{self.dataset_name}'
        
        kaggle.api.dataset_download_files(dataset = dataset_name ,path= self.pasta_name,unzip=False)
        self.__extrai_arquivos()
        
    def __extrai_arquivos(self):
        try: 
            with zipfile.ZipFile(f"{self.pasta_name}/{self.dataset_name}.zip","r") as zip_read:
                for arquivo in zip_read.namelist():
                    if arquivo.endswith(".csv"):
                        zip_read.extract(arquivo,f"{self.pasta_name}/arquivos_extraidos")
            print("arquivo csv extraidos com sucesso")
        except Exception as e:
            print(f"erro ao extrair arquivos:{e}")    