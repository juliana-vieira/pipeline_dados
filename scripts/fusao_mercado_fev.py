from processamento_dados import Dados

path_empresa_A = 'data_raw/dados_empresaA.json'
path_empresa_B = 'data_raw/dados_empresaB.csv'

# Extract

print("\nDados extraídos: Empresa A")
print("-------------------")
dados_empresaA = Dados.leitura_dados(path_empresa_A, 'json')
print(f"Nome das colunas: {dados_empresaA.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaA.qtd_linhas}")

print("\nDados extraídos: Empresa B")
print("-------------------")
dados_empresaB = Dados.leitura_dados(path_empresa_B, 'csv')
print(f"Nome das colunas: {dados_empresaB.nome_colunas}")
print(f"Quantidade de linhas: {dados_empresaB.qtd_linhas}")

# Transform

key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print("\nDados transformados")
print("-------------------")
print(dados_empresaB.nome_colunas)

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(dados_fusao.nome_colunas)
print(dados_fusao.qtd_linhas)

# Load

path_dados_combinados = 'data_processed/dados_combinados.csv'
dados_fusao.salvar_dados(path_dados_combinados)
print(path_dados_combinados)








