import csv
import time
import requests
import chardet  # type: ignore
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "https://redrivesandbox.flyerp.com.br/apis/CadastrarContrato"

# Bearer Token para autenticação
bearer_token = "0af5dff3-7f63-422e-b6d9-7b48258a28ba"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/Contratos_redrive_completo.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_Contratos_redrive.csv"

# Função para converter o valor em decimal formatado
def format_decimal(value):
    try:
        return value.replace('.', ',') if isinstance(value, str) else str(value)
    except AttributeError:
        return '0'  # Retorna '0' em caso de erro

# Detectar a codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result.get('encoding', 'utf-8-sig')
logging.info(f"Codificação detectada: {encoding}")

# Criar e escrever cabeçalho no arquivo de erro antes do loop
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(['cnpj_cpf_cliente', 'referencia', 'referencia_busca_cliente', 'data_primeiro_vencimento', 'dia_vencimento',
                          'descricao', 'numero', 'codigo_vendedor', 'data_vigencia_inicial', 'data_vigencia_final',
                          'situacao', 'recorrencia', 'tipo_faturamento', 'indice_reajuste', 'codigo_banco_conta',
                          'codigo_forma_pagamento', 'codigo_plano_contas', 'codigo_condicao_pagamento',
                          'observacao_fiscal', 'observacao_nao_fiscal', 'data_proximo_reajuste', 'servicos.referencia',
                          'servicos.quantidade', 'servicos.valor_unitario', 'servicos.complemento',
                          'tipo_desconto_boleto_ate_vencimento', 'valor_desconto_boleto_ate_vencimento', 'id_importacao_pessoa', 'Mensagem', 'Id'])

# Ler arquivo CSV original e processar cada linha
try:
    with open(csv_file_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')
        reader.fieldnames = [field.strip() for field in reader.fieldnames]

        # Abrir o arquivo de erro no modo append para adicionar registros falhados
        with open(erro_csv_file_path, mode='a', newline='', encoding='utf-8-sig') as erro_file:
            erro_writer = csv.writer(erro_file)

            for row in reader:
                try:
                    logging.info(f"Processando linha: {row}")

                    # Montagem do payload
                    contrato_a_receber = {
                        "cnpj_cpf_cliente": row.get('cnpj_cpf_cliente', ''),
                        "referencia": row.get('referencia', ''),
                        "referencia_busca_cliente": row.get('referencia_busca_cliente', '0'),
                        "data_primeiro_vencimento": row.get('data_primeiro_vencimento', ''),
                        "dia_vencimento": row.get('dia_vencimento', ''),
                        "descricao": row.get('descricao', ''),
                        "numero": row.get('numero', ''),
                        "codigo_vendedor": int(row.get('codigo_vendedor', 0)),
                        "data_vigencia_inicial": row.get('data_vigencia_inicial', ''),
                        "data_vigencia_final": row.get('data_vigencia_final', ''),
                        "situacao": row.get('situacao', ''),
                        "recorrencia": row.get('recorrencia', ''),
                        "tipo_faturamento": row.get('tipo_faturamento', ''),
                        "indice_reajuste": row.get('indice_reajuste', ''),
                        "codigo_banco_conta": int(row.get('codigo_banco_conta', 0)),
                        "codigo_forma_pagamento": int(row.get('codigo_forma_pagamento', 0)),
                        "codigo_plano_contas": int(row.get('codigo_plano_contas', 0)),
                        "codigo_condicao_pagamento": row.get('codigo_condicao_pagamento', ''),
                        "observacao_fiscal": row.get('observacao_fiscal', ''),
                        "observacao_nao_fiscal": row.get('observacao_nao_fiscal', ''),
                        "data_proximo_reajuste": row.get('data_proximo_reajuste', ''),
                        "tipo_desconto_boleto_ate_vencimento": row.get('tipo_desconto_boleto_ate_vencimento', '0'),
                        "valor_desconto_boleto_ate_vencimento": format_decimal(row.get('valor_desconto_boleto_ate_vencimento', '0')),
                        "id_importacao_pessoa": row.get('id_importacao_pessoa', ''),
                        "servicos": [
                            {
                                "referencia": row.get('servicos.referencia', ''),
                                "quantidade": int(row.get('servicos.quantidade', 1)),
                                "valor_unitario": format_decimal(row.get('servicos.valor_unitario', '0')),
                                "complemento": row.get('servicos.complemento', '')
                            }
                        ]
                    }

                    # Envio do payload
                    response = requests.post(url, headers=headers, json=contrato_a_receber)
                    response_data = response.json()

                    if response.status_code != 200 or not response_data.get("Sucesso", False):
                        # Escrever no arquivo de erro
                        erro_writer.writerow([
                            contrato_a_receber["cnpj_cpf_cliente"],
                            contrato_a_receber["referencia"],
                            contrato_a_receber["referencia_busca_cliente"],
                            contrato_a_receber["data_primeiro_vencimento"],
                            contrato_a_receber["dia_vencimento"],
                            contrato_a_receber["descricao"],
                            contrato_a_receber["numero"],
                            contrato_a_receber["codigo_vendedor"],
                            contrato_a_receber["data_vigencia_inicial"],
                            contrato_a_receber["data_vigencia_final"],
                            contrato_a_receber["situacao"],
                            contrato_a_receber["recorrencia"],
                            contrato_a_receber["tipo_faturamento"],
                            contrato_a_receber["indice_reajuste"],
                            contrato_a_receber["codigo_banco_conta"],
                            contrato_a_receber["codigo_forma_pagamento"],
                            contrato_a_receber["codigo_plano_contas"],
                            contrato_a_receber["codigo_condicao_pagamento"],
                            contrato_a_receber["observacao_fiscal"],
                            contrato_a_receber["observacao_nao_fiscal"],
                            contrato_a_receber["data_proximo_reajuste"],
                            contrato_a_receber["servicos"][0]["referencia"],
                            contrato_a_receber["servicos"][0]["quantidade"],
                            contrato_a_receber["servicos"][0]["valor_unitario"],
                            contrato_a_receber["servicos"][0]["complemento"],
                            contrato_a_receber["tipo_desconto_boleto_ate_vencimento"],
                            contrato_a_receber["valor_desconto_boleto_ate_vencimento"],
                            contrato_a_receber["id_importacao_pessoa"],
                            response_data.get("Mensagem", "Erro desconhecido"),
                            response_data.get("Id", "N/A")
                        ])
                        logging.error(f"Erro ao cadastrar contrato {contrato_a_receber['referencia']}: {response_data.get('Mensagem', 'Erro desconhecido')}")
                    else:
                        logging.info(f"Contrato {contrato_a_receber['referencia']} cadastrado com sucesso! ID: {response_data.get('Id')}")

                    time.sleep(0.2)

                except Exception as e:
                    logging.error(f"Erro ao processar linha: {row}. Erro: {str(e)}")
                    # Escrever a linha com erro no arquivo de erro
                    erro_writer.writerow(list(row.values()) + [f"Erro de processamento: {str(e)}", "N/A"])
                    continue  # Continuar para a próxima linha

except Exception as e:
    logging.error(f"Erro inesperado ao processar arquivo: {e}")