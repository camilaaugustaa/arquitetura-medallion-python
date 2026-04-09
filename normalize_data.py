import os
import pandas as pd


# Criamos a classe NormalizeData para normalizar os dados, convertendo arquivos CSV e JSON para Parquet, 
# tratando listas e removendo duplicatas.

class NormalizeData: 
    def __init__(self, input_dir, output_dir): 
        self.input_dir = input_dir
        self.output_dir = output_dir    
        os.makedirs(self.output_dir, exist_ok=True)

# O método normalize_data percorre os arquivos na pasta de entrada, lê os dados, trata as colunas que contêm listas, 
# remove duplicatas e salva os dados normalizados em formato Parquet na pasta de saída.

    def normalize_data(self):
        for file in os.listdir(self.input_dir):
            input_path = os.path.join(self.input_dir, file)
            name, ext = os.path.splitext(file)
            output_path = os.path.join(self.output_dir, f"{name}.parquet")

            if ext.lower() == '.csv':
                df = pd.read_csv(input_path, encoding="latin-1")
            elif ext.lower() == '.json':
                try:
                    df = pd.read_json(input_path)
                except ValueError:
                    df = pd.read_json(input_path, lines=True)
            else:
                print(f"Arquivo{file} ignorado (formato não suportado)")
                continue
            

            for col in df.columns:
                    if df[col].apply(lambda x: isinstance(x,list)).any():
                        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)



            df = df.drop_duplicates().reset_index(drop=True)

            df.to_parquet(output_path, index=False)
            print(f"Arquivo {file} normalizado e salvo como {output_path}")

# O bloco if __name__ == "__main__": é usado para executar o processo de normalização dos dados quando o script é executado diretamente. 
# Ele cria uma instância da classe NormalizeData, especificando as pastas de entrada e saída, e chama o método normalize_data para iniciar 
# o processo de normalização.

if __name__ == "__main__":
    normalize_data = NormalizeData(input_dir='01-bronze-raw', output_dir='02-silver-validated')
    normalize_data.normalize_data()

