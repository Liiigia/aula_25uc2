import polars as pl  
from datetime import datetime

ENDERECO_DADOS = './../DADOS/PARQUET/NovoBolsaFamilia/'

try:
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()

    # Método Lazy "scan_parquet" cria um plano de execução, não carregando TODOS os dados 
    # diretamente na memória, porém, o plano é implementado                           # SCAN PARQUET, quando faz a leitura de tdo, ele faz um apanhado e cria plano de execução
    lazy_plan = (                                                                     #  e no final ele não traz todos os dados, ele mostra quantas colunas e etc
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
        .select(['NOME MUNICÍPIO', 'VALOR PARCELA'])                                  # Poderia ter colocado o .select tudo na msm linha tbm. SELECT, uso para pegar somente algumas informaçes, para não precisar trazer todas. ESTAMOS USANDO O POLARS
        .group_by('NOME MUNICÍPIO')   
        .agg(pl.col('VALOR PARCELA').sum())                                            # agg = agregar
        .sort('VALOR PARCELA', descending=True)                                                                         
    )

    # print(lazy_plan)
    # collect() executa o plano de execução. Ele realmente traz os dados, traz somente o que o plano diz, organizar, agrupar etc
    df_bolsa_familia = lazy_plan.collect()

    print(df_bolsa_familia.head(10))

    fim = datetime.now()
    print(f'Tempo de execução: {fim - inicio} ')
    print('Leitura do arquivo parquet')


except Exception as e:
    print('Ero ao obter dados')