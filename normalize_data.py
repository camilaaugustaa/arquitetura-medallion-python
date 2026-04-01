import os
import pandas as pd

input_dir = '01-bronze-raw'#arquivo de entrada
output_dir = '02-silver-validated'#arquivo de saída

os.makedirs(output_dir, exist_ok=True)#cria o diretório de saída se ele não existir

for file in os.listdir(input_dir):
    input_path = os.path.join(input_dir, file)