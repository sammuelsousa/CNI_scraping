# Importando libs necessárias 
import json 
import pandas as pd 
import requests
import pyspark
from bs4 import BeautifulSoup
from fastparquet import write, ParquetFile
import os
from datetime import datetime
import time

#Criando uma rotina de execução e capturando o momento da raspagem
while(True): 
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(f'At time : {current_time} IST')

#função que recebe a url e faz um request
    def req_func(link):
        return requests.get(link)
        print('O link é:', link)
#função que transforma o objeto em um documento BeautfulSoup
    def soup_fun(dados):
        return BeautifulSoup(dados.text,'html.parser')
#função converte o documento BeautfulSoup em string
    def convert_str(conteudo):
        if type(conteudo)!="str":
            return str(conteudo)

#    Fazendo requisição à página web e salvando o conteúdo no formato BeautfulSoap, que depois é convertido
#    para string a fim de viabilizar a transformação em objeto json

    url="https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"
    dados_html = req_func(url)
    dados_soup = soup_fun(dados_html)
    dados_str = convert_str(dados_soup)
    dados_json = json.loads(dados_str)


#    Percorrendo o objeto dados_json e mapeando as posições dos arrays

#Tentei de várias formas converter o json em um dataframe para facilitar a exportação para parquet, porém
#devido ao pouco tempo, preferi fazer o mapeamento de forma manual, tendo em vista que o json tinha colunas
#com tamanhos diferentes. Não trouxe todas as colunas, mas estão comentadas no final do array
    dados_col = {}
    dados_col['Id'] = dados_json['Id']
    dados_col['Nome'] = dados_json['Nome']
    dados_col['Fonte'] = dados_json['Fonte']
    dados_col['Notas'] = dados_json['Notas'][0]
    dados_col['Nome'] = dados_json['Periodos']['Nome']
    dados_col['Periodicidade'] = dados_json['Periodos']['Periodicidade']
    dados_col['Periodos'] = dados_json['Periodos']['Periodos']
    dados_col['Periodos - Id'] = dados_json['Periodos']['Periodos'][0]['Id']
    dados_col['Periodos - Codigo'] = dados_json['Periodos']['Periodos'][0]['Codigo']
    dados_col['Periodos - Nome'] = dados_json['Periodos']['Periodos'][0]['Nome']
    dados_col['Periodos - Disponivel'] = dados_json['Periodos']['Periodos'][0]['Disponivel']
    dados_col['Periodos - DataLiberacao'] = dados_json['Periodos']['Periodos'][0]['DataLiberacao']
    dados_col['Conjuntos - Id'] = dados_json['Periodos']['Conjuntos'][0]['Id']
    dados_col['Conjuntos - Nome'] = dados_json['Periodos']['Conjuntos'][0]['Nome']
    dados_col['Conjuntos - Periodos'] = dados_json['Periodos']['Conjuntos'][0]['Periodos'][0]
    dados_col['Unidades de Medida - Id'] = dados_json['UnidadesDeMedida'][0]['Id']
    dados_col['Unidades de Medida - Nome'] = dados_json['UnidadesDeMedida'][0]['Nome']
    dados_col['Variaveis - id'] = dados_json['Variaveis'][0]['Id']
    dados_col['Variaveis - Nome'] =dados_json['Variaveis'][0]['Nome']
    dados_col['Variaveis - Desidentificacao'] = dados_json['Variaveis'][0]['Desidentificacao']
    dados_col['Variaveis - DecimaisApresentacao'] = dados_json['Variaveis'][0]['DecimaisApresentacao']
    dados_col['Variaveis - DecimaisArmazenamento'] = dados_json['Variaveis'][0]['DecimaisArmazenamento']
    dados_col['Variaveis - Descricao'] = dados_json['Variaveis'][0]['Descricao']
    #dados_col['Variaveis - VariaveisDerivadas'] = dados_json['Variaveis'][0]['VariaveisDerivadas'][0]
    dados_col['Variaveis - UnidadeDeMedida - Periodo'] = dados_json['Variaveis'][0]['UnidadeDeMedida'][0]['Periodo']
    dados_col['Variaveis - UnidadeDeMedida - Unidade'] = dados_json['Variaveis'][0]['UnidadeDeMedida'][0]['Unidade']
    dados_col['Variaveis - Nome'] =dados_json['Variaveis'][0]['Nome']
    #dados_col[''] = dados_json['Variaveis']
    dados_col['Territorios - Nome'] =dados_json['Territorios']['Nome']
    dados_col['Territorios - DicionarioNiveis - Id'] =dados_json['Territorios']['DicionarioNiveis']['Ids'][0]
    dados_col['Territorios - DicionarioNiveis - Nomes'] =dados_json['Territorios']['DicionarioNiveis']['Nomes'][0]
    dados_col['Territorios - DicionarioNiveis - DatasExtincao'] =dados_json['Territorios']['DicionarioNiveis']['DatasExtincao'][0]
    dados_col['Territorios - DicionarioUnidades - Id'] =dados_json['Territorios']['DicionarioUnidades']['Ids'][0]
    dados_col['Territorios - DicionarioUnidades - Nomes'] =dados_json['Territorios']['DicionarioUnidades']['Nomes'][0]
    dados_col['Territorios - DicionarioUnidades - Niveis'] =dados_json['Territorios']['DicionarioUnidades']['Niveis'][0]
    dados_col['Territorios - DicionarioUnidades - Niveis'] =dados_json['Territorios']['DicionarioUnidades']['Niveis'][0]
    dados_col['Territorios - DicionarioUnidades - SiglasUF'] =dados_json['Territorios']['DicionarioUnidades']['SiglasUF'][0]
    dados_col['Territorios - DicionarioUnidades - CodigosIBGE'] =dados_json['Territorios']['DicionarioUnidades']['CodigosIBGE'][0]
    dados_col['Territorios - DicionarioUnidades - Complementos1'] =dados_json['Territorios']['DicionarioUnidades']['Complementos1'][0]
    dados_col['Territorios - DicionarioUnidades - Complementos2'] =dados_json['Territorios']['DicionarioUnidades']['Complementos2'][0]
    dados_col['Territorios - DicionarioUnidades - DatasExtincao'] =dados_json['Territorios']['DicionarioUnidades']['DatasExtincao'][0]
    #dados_col['NiveisTabela - Id'] =dados_json['NiveisTabela'][0]['Id']
    #dados_col['PossuiMalhaTerritorial'] =dados_json['PossuiMalhaTerritorial']
    #dados_col['AbrangenciasOmitidas'] =dados_json['AbrangenciasOmitidas']
    #dados_col['NiveisAbrangentes'] =dados_json['NiveisAbrangentes']
    #dados_col['VisoesTerritoriais']


#    Convertendo o objeto dados_col em DataFrame e armazenando em arq para posteriormente gerar um arquivo csv.

    arq = pd.DataFrame(dados_col)
    arq.to_csv('dados.csv')
    dados_csv = pd.read_csv('dados.csv')

#    Convertendo os dados do arquivo csv em parquet e gerando um arquivo .parquet

    dados_csv.to_parquet('teste.parquet', compression = "GZIP")
    #dados_parquet = ParquetFile('teste.parquet').to_pandas()
    
    time.sleep(600)