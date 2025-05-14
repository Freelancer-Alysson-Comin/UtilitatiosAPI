import csv
import time
import requests
import chardet  # type: ignore
import logging

# Log para rastrear execução
print("Script iniciado.")

# Configuração de logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL do endpoint
url = "http://raffinato.flyerp.com.br/APIs/ImportarPessoa"

# Bearer Token
bearer_token = "08228007-9d86-4446-8718-e948131a3464"

# Cabeçalhos
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho dos arquivos
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/clientes_falhados_raffinato.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_pessoa_CADASTRO_CLIENTES_RAFFINATO_V3.csv"

# Detectar codificação do CSV
print("Detectando codificação do CSV...")
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result['encoding']
print(f"Codificação detectada: {encoding}")
logging.info(f"Codificacao detectada: {encoding}")

# Função de limpeza
def limpar_texto(texto):
    return texto.replace("\xa0", " ").strip() if texto else None

# Início da leitura e processamento
print("Tentando abrir o CSV...")

try:
    with open(csv_file_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')

        # Mostrar colunas detectadas
        if reader.fieldnames:
            print(f"Colunas detectadas no CSV: {reader.fieldnames}")
        else:
            print("⚠️ Nenhuma coluna detectada no CSV.")
            raise ValueError("CSV sem cabeçalho válido.")

        reader.fieldnames = [field.strip() for field in reader.fieldnames]

        with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
            erro_writer = csv.writer(erro_file)

            # Correção: separação correta entre "nome_vendedor" e "Id"
            erro_writer.writerow(["cnpj_cpf", "inscricao_estadual_RG", "razao_social_nome", "nome_fantasia_apelido",
                                  "endereco", "numero", "complemento", "bairro", "cidade", "estado",
                                  "cep", "telefone1", "telefone2", "contato_nome", "contato_telefone",
                                  "email", "observacao", "codigo_ramo_atividade",
                                  "somente_criacao", "exterior", "exterior_nome_cidade", "id_importacao",
                                  "referencia_busca", "Mensagem", "nome_vendedor", "Id"])

            linha = 0
            print("Iniciando processamento das linhas...")

            for row in reader:
                linha += 1
                print(f"Processando linha {linha}: {row}")

                pessoa = {
                    "cnpj_cpf": limpar_texto(row.get("cnpj_cpf")),
                    "inscricao_estadual_RG": limpar_texto(row.get("inscricao_estadual_RG")),
                    "razao_social_nome": limpar_texto(row.get("razao_social_nome")),
                    "nome_fantasia_apelido": limpar_texto(row.get("nome_fantasia_apelido")),
                    "endereco": limpar_texto(row.get("endereco")),
                    "numero": limpar_texto(row.get("numero")),
                    "complemento": limpar_texto(row.get("complemento")),
                    "bairro": limpar_texto(row.get("bairro")),
                    "cidade": limpar_texto(row.get("cidade")),
                    "estado": limpar_texto(row.get("estado")),
                    "cep": limpar_texto(row.get("cep")),
                    "telefone1": limpar_texto(row.get("telefone1")),
                    "telefone2": limpar_texto(row.get("telefone2")),
                    "contato_nome": limpar_texto(row.get("contato_nome")),
                    "contato_telefone": limpar_texto(row.get("contato_telefone")),
                    "email": limpar_texto(row.get("email")),
                    "observacao": limpar_texto(row.get("observacao")),                                       
                    "codigo_ramo_atividade": int(row.get("codigo_ramo_atividade", 5)),
                    "somente_criacao": row.get("somente_criacao", "false").lower() == "true",
                    "exterior": row.get("exterior", "false").lower() == "true",
                    "exterior_nome_cidade": limpar_texto(row.get("exterior_nome_cidade")),
                    "id_importacao": limpar_texto(row.get("id_importacao")),
                    "referencia_busca": int(row.get("referencia_busca", 0)),
                    "nome_vendedor": limpar_texto(row.get("nome_vendedor", "PADRÃO"))
                }

                # Log do payload
                print(f"Enviando requisição: {pessoa}")
                logging.info(f"Enviando requisição: {pessoa}")

                try:
                    response = requests.post(url, headers=headers, json=pessoa)
                    response.raise_for_status()
                    response_data = response.json()

                    if not response_data.get("Sucesso"):
                        print(f"❌ Falha no envio da linha {linha}: {response_data.get('Mensagem')}")
                        erro_writer.writerow(list(pessoa.values()) + [response_data.get("Mensagem", "Erro"), response_data.get("Id", "N/A")])

                except Exception as e:
                    logging.error(f"Erro ao enviar dados na linha {linha}: {e}")
                    print(f"⚠️ Erro na requisição da linha {linha}: {e}")
                    erro_writer.writerow(list(pessoa.values()) + ["Erro na requisição", "N/A"])

                # Aguardar 200ms entre requisições
                time.sleep(0.2)

    print("✅ Processamento finalizado.")

except Exception as e:
    logging.error(f"Erro inesperado: {e}")
    print(f"❌ Erro inesperado: {e}")
