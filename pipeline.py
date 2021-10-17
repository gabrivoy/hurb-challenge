# Imports para o Apache Beam
import apache_beam as beam
from apache_beam.transforms.deduplicate import DeduplicatePerKey
from apache_beam.options.pipeline_options import PipelineOptions

# Imports das bibliotecas auxiliares: 
# - Pandas para auxílio com documentos .csv
# - Json para realizar a conversão do pandas.DataFrame para um output json
import pandas as pd
import json

# Criação de dois pipelines
# 'pipe' = pipeline de extração de dados e criação de arquivos intermediários
# 'pipe_second' = pipeline de leitura dos arquivos intermediários para gerar o output final
options = PipelineOptions()
pipe = beam.Pipeline(options=options)
pipe_second = beam.Pipeline(options=options)

# Funções de formatação auxiliares
def formatOutput(element):
    return str(element[0]) + ',' + str(element[1])

def formatUF(element):
    if element[0] == '76':
        return str(element[0] + ',BR') 
    else:
        return str(element[0] + ',' + element[1][1])

def formatUnion(element):
    # Ordem dos elementos na tupla e dicionário:
    # - element[0] = '76'
    # - element[1] = dict
    # - element[1]['1'] = casos
    # - element[1]['2'] = obitos
    # - element[1]['3'] = regiao
    # - element[1]['4'] = estado
    # - element[1]['5'] = governador
    # - element[1]['6'] = uf

    if element[0] == '76':
        # Para adicionar o ID do UF, adicionar: 'element[0] + ',' + ' no começo da string de retorno
        return element[1]['3'][0] + ',' + "--" + ',' + element[1]['6'][0] + ',' + "--" + ',' + element[1]['1'][0] + ',' + element[1]['2'][0]
    else:
        # Para adicionar o ID do UF, adicionar: 'element[0] + ',' + ' no começo da string de retorno
        return element[1]['3'][0] + ',' + element[1]['4'][0] + ',' + element[1]['6'][0] + ',' + element[1]['5'][0] + ',' + element[1]['1'][0] + ',' + element[1]['2'][0]

# 'casosporuf' = Faz a leitura e extração em tupla do número de casos de COVID19 por código de UF
# gera um .csv intermediário com esses dados
casosporuf = ( 
    pipe 
    | 'Lê o arquivo HIST_PAINEL_COVIDBR_28set2020.csv' >> beam.io.ReadFromText(".\data\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines=1) 
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas' >> beam.Map(lambda value: value.split(';'))
    | 'Faz um filtro retirando todos os elementos preenchidos na coluna codmun' >> beam.Filter(lambda value: value[4] == '')
    | 'Cria uma tupla onde a chave primária é o UF do estado e o valor é o número de novos casos por dia' >> beam.Map(lambda value: (value[3], int(value[11])))
    | 'Faz a soma de cada um dos casos novos pela chave de UF' >> beam.CombinePerKey(sum)
    | 'Mapeia a saida' >> beam.Map(formatOutput)
    | 'OutCasos' >> beam.io.WriteToText(".\processing\outputcasos.csv")
)

# 'obitosporuf' = Faz a leitura e extração em tupla do número de óbitos de COVID19 por código de UF.
# gera um .csv intermediário com esses dados
obitosporuf = ( 
    pipe 
    | 'Lê o arquivo HIST_PAINEL_COVIDBR_28set2020.csv (2)' >> beam.io.ReadFromText(".\data\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines=1) 
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas (2)' >> beam.Map(lambda value: value.split(';'))
    | 'Faz um filtro retirando todos os elementos preenchidos na coluna codmun (2)' >> beam.Filter(lambda value: value[4] == '')
    | 'Cria uma tupla onde a chave primária é o UF do estado e o valor é o número de novos óbitos por dia' >> beam.Map(lambda value: (value[3], int(value[13])))
    | 'Faz a soma de cada um dos óbitos novos pela chave de UF' >> beam.CombinePerKey(sum)
    | 'Mapeia a saida (2)' >> beam.Map(formatOutput)
    | 'OutObitos' >> beam.io.WriteToText(".\processing\outputobitos.csv")
)

# 'regioesporuf' = Faz a leitura e extração em tupla das regiões do país por código de UF.
# gera um .csv intermediário com esses dados
regioesporuf = (
    pipe
    | 'Lê o arquivo HIST_PAINEL_COVIDBR_28set2020.csv (3)' >> beam.io.ReadFromText(".\data\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines=1) 
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas (3)' >> beam.Map(lambda value: value.split(';'))
    | 'Faz um filtro retirando todos os elementos preenchidos na coluna codmun (3)' >> beam.Filter(lambda value: value[4] == '')
    | 'Cria uma tupla onde a chave primária é o UF do estado e o valor é a região' >> beam.Map(lambda value: (value[3], value[0]))
    | 'Junta as regiões pela chave de UF' >> beam.GroupByKey()
    | 'Mapeia a saida (3)' >> beam.Map(formatUF)
    | 'OutRegioes' >> beam.io.WriteToText(".\processing\outputregioes.csv")
)

# 'uf' = Faz a leitura e extração em tupla das siglas de UF do estado por código de UF.
# gera um .csv intermediário com esses dados
uf = (
    pipe
    | 'Lê o arquivo  HIST_PAINEL_COVIDBR_28set2020.csv (4)' >> beam.io.ReadFromText(".\data\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines=1)
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas (6)' >> beam.Map(lambda value: value.split(';'))
    | 'Retira as informações sobre UF' >> beam.Map(lambda value: (value[3], value[1]))
    | 'Junta as regiões pela chave de UF (6)' >> beam.GroupByKey()
    | 'Mapeia a saida (6)' >> beam.Map(formatUF)
    | 'Out4' >> beam.io.WriteToText(".\processing\outputuf.csv")
)

# 'ufnome' = Faz a leitura e extração em tupla dos nomes por extenso de cada estado por código de UF.
# gera um .csv intermediário com esses dados
ufnome = (
    pipe
    | 'Lê o arquivo EstadosIBGE.csv' >> beam.io.ReadFromText(".\data\EstadosIBGE.csv", skip_header_lines=1)
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas (4)' >> beam.Map(lambda value: value.split(';'))
    | 'Retira as informações sobre UF e código' >> beam.Map(lambda value: (value[1], value[0]))
    | 'Mapeia a saida (4)' >> beam.Map(formatOutput)
    | 'Out2' >> beam.io.WriteToText(".\processing\outputufnome.csv")
)

# 'ufnome' = Faz a leitura e extração em tupla dos nomes dos governadores de cada estado por código de UF.
# gera um .csv intermediário com esses dados
ufgov = (
    pipe
    | 'Lê o arquivo EstadosIBGE.csv (2)' >> beam.io.ReadFromText(".\data\EstadosIBGE.csv", skip_header_lines=1)
    | 'Faz a separação linha-a-linha entre ponto-e-víruglas (5)' >> beam.Map(lambda value: value.split(';'))
    | 'Retira as informações sobre UF e nome de governador' >> beam.Map(lambda value: (value[1], value[3]))
    | 'Mapeia a saida (5)' >> beam.Map(formatOutput)
    | 'Out3' >> beam.io.WriteToText(".\processing\outputufgov.csv")
)

# execução do primeiro pipe, que irá gerar os arquivos intermediários
pipe.run()

# formatação dos arquivos intermediários gerados em um só
casos = (
    pipe_second
    | 'Lê o arquivo outputcasos.csv' >> beam.io.ReadFromText('.\processing\outputcasos.csv-00000-of-00001')
    | 'Separar casos' >> beam.Map(lambda value: value.split(','))
    | 'Pareia os UF com os casos' >> beam.Map(lambda value: value)
)

obitos = (
    pipe_second
    | 'Lê o arquivo outputobitos.csv' >> beam.io.ReadFromText('.\processing\outputobitos.csv-00000-of-00001')
    | 'Separar obitos' >> beam.Map(lambda value: value.split(','))
    | 'Pareia os UF com os obitos' >> beam.Map(lambda value: value)
)

regioes = (
    pipe_second
    | 'Lê o arquivo outputregioes.csv' >> beam.io.ReadFromText('.\processing\outputregioes.csv-00000-of-00001')
    | 'Separar regioes' >> beam.Map(lambda value: value.split(','))
    | 'Pareia os UF com as regioes' >> beam.Map(lambda value: value)
    )

nome = (
    pipe_second
    | 'Lê o arquivo outputnome.csv' >> beam.io.ReadFromText('.\processing\outputufnome.csv-00000-of-00001')
    | 'Separar UFs e nomes' >> beam.Map(lambda value: value.split(','))
    | 'Pareia os UF com os nomes' >> beam.Map(lambda value: value)
)

governador = (
    pipe_second
    | 'Lê o arquivo outputgov.csv' >> beam.io.ReadFromText('.\processing\outputufgov.csv-00000-of-00001')
    | 'Separar UFs e governadores' >> beam.Map(lambda value: value.split(','))
    | 'Pareia os UF com os governadores' >> beam.Map(lambda value: value)    
)

uf = (
    pipe_second
    | 'Lê o arquivo outputuf.csv' >> beam.io.ReadFromText('.\processing\outputuf.csv-00000-of-00001')
    | 'Separar UFs' >> beam.Map(lambda value: value.split(','))
    | 'Pareia com os UFs' >> beam.Map(lambda value: value)    

)

casoseobitos = (
    {'1': casos, '2': obitos, '3': regioes, '4': nome, '5': governador, '6': uf}
    | beam.CoGroupByKey()
    | beam.Map(formatUnion)
    | 'Casos e óbitos por UF' >> beam.io.WriteToText('.\processing\casoseobitos.csv',
    header = 'Regiao,Estado,UF,Governador,TotalCasos,TotalObitos')
)

# execução do segundo pipe para gerar o arquivo intermediário com as informações completas
pipe_second.run()

# formatação do arquivo intermediário completo para as saídas desejadas
# faz a leitura do arquivo intermediário completo em um pandas.DataFrame
df = pd.read_csv('./processing/casoseobitos.csv-00000-of-00001')

# gera um output no formato .json do pandas.DataFrame
result = df.to_json(orient='records')
parsed = json.loads(result)

with open('outputjson.json', 'w', encoding='utf8') as outfile:
    json.dump(parsed, outfile, ensure_ascii=False, indent = 4)

# gera um output no formato .csv do pandas.DataFrame
df.to_csv('output.csv', index=True) 