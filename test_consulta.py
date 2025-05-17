import duckdb

con = duckdb.connect()

# Caminho do diretório onde os dados particionados estão armazenados
output_dir = 'output_particionado'

# Dimensões do Usuário (Demográficas e de Perfil)
query_dim_demo_perf_users = """
SELECT *
FROM 'output_sem_funnel/**/*.parquet'
"""

df_dim_users = duckdb.query(query_dim_demo_perf_users).to_df()



df_dim_users.to_csv('test.csv', index=False)

# Fechar a conexão
con.close()