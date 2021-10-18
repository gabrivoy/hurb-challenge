# Hurb Challenge - Data Engineer

Desenvolvido por Gabriel Ribeiro Gomes, GitHub: [@gabrivoy](https://github.com/gabrivoy). *ribeiroggabriel@gmail.com* | *ribeirogabriel@poli.ufrj.br*.


## Introdução


Este repositório tem como objetivo apresentar a resolução do desafio para a posição de Cientista de Dados/Engenheiro de Dados da Hurb, que consiste na montagem de um pipeline de dados usando Apache Beam. Caso você não esteja familiarizado com o desafio, pode vê-lo aqui: [Desafio Data Engineer Hurb](https://github.com/gabrivoy/hurb-challenge/blob/main/Desafio%20Apache%20Beam%20-%20Data%20Engineer.pdf) .

As tecnologias e conhecimentos aplicados na resolução desse desafio foram:
* Python
* Framework Apache Beam 
* Git
* VirtualEnvironments

## Instalação

O desafio requer [**Python**](https://www.python.org/) **v3.5+** e o **pip** para ser executado. Após a instalação, o usuário pode ir até sua pasta de preferência e realizar o git clone.

```
$ git clone https://github.com/gabrivoy/hurb-challenge.git
```

É recomendado rodar o sistema dentro de um ambiente virtual de execução (virtualenv), para isso, o usuário pode executar dentro do terminal na pasta os seguintes comandos:

```
$ virtualenv venv
$ .\venv\Scripts\activate
```

Para instalar os pacotes necessários para a aplicação, instalamos os requirements dentro do ambiente virtual.

```
$ pip install -r requirements.txt
```

Após a instalação, executar o arquivo **pipeline.py** irá rodar a aplicação.

```
$ python3 pipeline.py
```

## Funcionamento


A aplicação funciona através da leitura de dois arquivos CSV da pasta ./data, os arquivos:

* EstadosIBGE.csv
* HIST_PAINEL_COVIDBR_28set2020.csv

Foi feita uma análise exploratória inicial para entender melhor o conteúdo desses arquivos. Essa análise se encontra disponível nesse [Notebook](https://github.com/gabrivoy/hurb-challenge/blob/main/exploratory-analysis.ipynb). Para criar um pipeline de formatação desses dados dentro do pedido pelo desafio, foi usado o framework [Apache Beam](https://beam.apache.org/), que é um modelo de programação unificado avançado que permite a implementação de trabalhos de processamento de dados em lote e streaming que podem executados em qualquer mecanismo de execução (ex.: Google Dataflow, Spark, Apache Airflow, entre outros).

A figura abaixo mostra uma pequena diagramação do funcionamento do sistema, dividido em 2 *pipelines*, onde cada *pipeline* executa pelo menos 3 etapas em cada arquivo.

![image](https://drive.google.com/uc?export=view&id=1aAL2EQaywNHdm-q2NIzt9J_7JANjpqkF)

No primeiro *pipe*, temos a leitura dos arquivos pelo Apache Beam, e a formatação deles em seis arquivos intermediários de tuplas, usando o código de referência de cada UF como chave primária. Esses arquivos então são alocados dentro de uma pasta chamada ./processing. O segundo *pipe* acessa esses arquivos e faz a união deles em um só, seguindo o formato:

```
arquivo = (id,{dicionario})
```

Onde:
```
dicionario = {
    '1': número de casos
    '2': número de obitos
    '3': nome das regioes
    '4': nome dos municipios
    '5': nome dos governadores
    '6': sigla dos ufs
    }
```

Por fim, esses arquivos são alocados formatados já como texto separado por vírgulas dentro do arquivo 'casoseobitos.csv', que também fica na pasta ./processing.

A etapa final da aplicação utiliza as bibliotecas pandas e json para transformar o CSV em uma variável do tipo pandas.DataFrame, e dessa variável utilizar as funções 'to_csv()' e 'json.dump()' para gerar os arquivos finais: 'output.csv' e 'outputjson.json.'


