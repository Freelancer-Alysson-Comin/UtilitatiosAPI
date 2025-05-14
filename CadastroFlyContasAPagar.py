import csv
import time
import requests
import chardet # type: ignore
import logging

# Configuração de logging
logging.basicConfig(filename='app_contas_pagar.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice para Contas a Pagar
url = "https://simsaude.flyerp.com.br/APIs/ImportarContasAPagar"

# Bearer Token para autenticação
bearer_token = "662680e7-b405-45d5-9f14-31208f318a9f"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/NOVOSPAGAMENTOS2.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_contas_pagar.csv"

# Função para converter o valor em decimal formatado
def format_decimal(value):
    try:
        return value.replace('.', ',') if isinstance(value, str) else str(value)
    except AttributeError:
        return '0'  # Retorna '0' em caso de erro

# Detectar a codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result['encoding']
logging.info(f"Codificação detectada: {encoding}")

# Abrir o arquivo CSV para registros falhados
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow(['nome_cliente', 'cnpj_cliente', 'numero', 'parcela', 'emissao', 'vencimento',
                          'ultimo_pagamento', 'valor', 'valor_pago', 'juros', 'desconto', 'numero_nf',
                          'serie_nf', 'banco', 'plano_contas', 'observacao', 'id', 'forma_pagamento',
                          'nome_centro_resultados_1', 'percentual_centro_resultados_1', 'Mensagem', 'Id'])

    # Ler arquivo CSV original
    try:
        with open(csv_file_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=';')
            reader.fieldnames = [field.strip() for field in reader.fieldnames]

            for row in reader:
                nome_cliente = row.get('nome_cliente')
                cnpj_cliente = row.get('cnpj_cliente')
                numero = row.get('numero')
                parcela = row.get('parcela')
                emissao = row.get('emissao')
                vencimento = row.get('vencimento')
                ultimo_pagamento = row.get('ultimo_pagamento')
                valor = format_decimal(row.get('valor', '0'))
                valor_pago = format_decimal(row.get('valor_pago', '0'))
                juros = format_decimal(row.get('juros', '0'))
                desconto = format_decimal(row.get('desconto', '0'))
                numero_nf = row.get('numero_nf')
                serie_nf = row.get('serie_nf')
                banco = row.get('banco')
                plano_contas = row.get('plano_contas')
                observacao = row.get('observacao')
                id_importacao = row.get('id')
                forma_pagamento = row.get('forma_pagamento')
                nome_centro_resultados_1 = row.get('nome_centro_resultados_1')
                percentual_centro_resultados_1 = format_decimal(row.get('percentual_centro_resultados_1', '0'))

                if cnpj_cliente is None:
                    logging.warning("Coluna 'cnpj_cliente' não encontrada no CSV. Ignorando registro.")
                    continue

                try:
                    contas_a_pagar = {
                        "nome_cliente": nome_cliente,
                        "cnpj_cliente": cnpj_cliente,
                        "numero": numero,
                        "parcela": int(parcela) if parcela else 1,
                        "emissao": emissao,
                        "vencimento": vencimento,
                        "ultimo_pagamento": ultimo_pagamento,
                        "valor": valor,
                        "valor_pago": valor_pago,
                        "juros": juros,
                        "desconto": desconto,
                        "numero_nf": int(numero_nf) if numero_nf else 0,
                        "serie_nf": serie_nf,
                        "banco": banco,
                        "plano_contas": plano_contas,
                        "observacao": observacao,
                        "id": id_importacao,
                        "forma_pagamento": forma_pagamento,
                        "nome_centro_resultados_1": nome_centro_resultados_1,
                        "percentual_centro_resultados_1": percentual_centro_resultados_1
                    }
                except ValueError as ve:
                    logging.error(f"Erro de conversão de valor no registro: {row}. Erro: {ve}")
                    erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                         ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                         serie_nf, banco, plano_contas, observacao, id_importacao, forma_pagamento,
                                         nome_centro_resultados_1, percentual_centro_resultados_1,
                                         f"Erro de conversão: {ve}", "N/A"])
                    continue

                try:
                    response = requests.post(url, headers=headers, json=contas_a_pagar)
                   # response.raise_for_status()

                    response_data = response.json()

                    if response_data.get("Sucesso"):
                        logging.info(f"Sucesso: {response_data['Mensagem']}, ID: {response_data['Id']}")
                    else:
                        logging.error(f"Erro: {response_data['Mensagem']}, ID: {response_data.get('Id', 'N/A')}")
                        erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                             ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                             serie_nf, banco, plano_contas, observacao, id_importacao,
                                             forma_pagamento, nome_centro_resultados_1, percentual_centro_resultados_1,
                                             response_data.get("Mensagem", ""), response_data.get("Id", "N/A")])

                except requests.exceptions.HTTPError as errh:
                    logging.error(f"Erro HTTP: {errh} - Registro {numero} não foi enviado.")
                    erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                         ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                         serie_nf, banco, plano_contas, observacao, id_importacao, forma_pagamento,
                                         nome_centro_resultados_1, percentual_centro_resultados_1, f"Erro HTTP: {errh}", "N/A"])
                except requests.exceptions.ConnectionError as errc:
                    logging.error(f"Erro de Conexão: {errc}")
                    erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                         ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                         serie_nf, banco, plano_contas, observacao, id_importacao, forma_pagamento,
                                         nome_centro_resultados_1, percentual_centro_resultados_1, f"Erro de Conexão: {errc}", "N/A"])
                except requests.exceptions.Timeout as errt:
                    logging.error(f"Timeout: {errt}")
                    erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                         ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                         serie_nf, banco, plano_contas, observacao, id_importacao, forma_pagamento,
                                         nome_centro_resultados_1, percentual_centro_resultados_1, f"Timeout: {errt}", "N/A"])
                except requests.exceptions.RequestException as err:
                    logging.error(f"Erro: {err}")
                    erro_writer.writerow([nome_cliente, cnpj_cliente, numero, parcela, emissao, vencimento,
                                         ultimo_pagamento, valor, valor_pago, juros, desconto, numero_nf,
                                         serie_nf, banco, plano_contas, observacao, id_importacao, forma_pagamento,
                                         nome_centro_resultados_1, percentual_centro_resultados_1, f"Erro: {err}", "N/A"])
    finally:
        print("Bloco 'finally' executado.")
    time.sleep(4)
