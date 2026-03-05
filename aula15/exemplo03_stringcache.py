import polars as pl  
from datetime import datetime

ENDERECO_DADOS = './../DADOS/PARQUET/NovoBolsaFamilia/'

try:
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()

    with pl.StringCache():
        # Método Lazy "scan_parquet" cria um plano de execução, não carregando TODOS os dados 
        # diretamente na memória, porém, o plano é implementado                           
        lazy_plan = (                                                                     
            pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
            .select(['NOME MUNICÍPIO', 'VALOR PARCELA'])          # TROUXEMOS SOMENTE DUAS COLUNAS
            .with_columns([
                pl.col('NOME MUNICÍPIO').cast(pl.Categorical)     # O NOME DOS MUNICIPIOS SERÃO TRANSFORMADOS EM CATEGORIAS, POIS OS NOMES SE REPETEM
            ])                                  
            .group_by('NOME MUNICÍPIO')   
            .agg(pl.col('VALOR PARCELA').sum())                                            
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