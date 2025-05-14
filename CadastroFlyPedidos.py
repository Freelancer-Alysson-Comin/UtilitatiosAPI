import csv
import time
import requests
import chardet  # type: ignore
import logging

# Configuração de logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "http://recon.flyerp.com.br/apis/CadastrarPedidoVenda"

# Bearer Token para autenticação
bearer_token = "1543c620-5f03-4f4f-9734-1b16144030fc"

# Cabeçalhos da requisição incluindo o Bearer Token
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminho do arquivo CSV original e de erro
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/pedidos_prontos_recon.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_pedido.csv"

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
    erro_writer.writerow(['cnpj_cpf_cliente', 'data_emissao', 'data_entrega', 'observacao_fiscal',
                          'itens.codigo_servico_produto', 'itens.valor_unitario', 'itens.quantidade',
                          'itens.codigo_lote_serie', 'parcelas.valor', 'parcelas.vencimento', 'parcelas.pagamento_a_vista',
                          'codigo_operacao_fiscal', 'codigo_banco_conta', 'codigo_forma_pagamento',
                          'codigo_condicao_pagamento', 'codigo_vendedor', 'codigo_situacao', 'valor_frete',
                          'tipo_frete', 'codigo_transportador', 'Mensagem', 'Id'])

    # Ler arquivo CSV original
    try:
        with open(csv_file_path, mode='r', encoding=encoding) as file:
            reader = csv.DictReader(file, delimiter=';')
            reader.fieldnames = [field.strip() for field in reader.fieldnames]

            for row in reader:
                # Mapear os campos conforme especificado
                cnpj_cpf_cliente = row.get('cnpj_cpf_cliente')
                data_emissao = row.get('data_emissao')
                data_entrega = row.get('data_entrega')
                observacao_fiscal = row.get('observacao_fiscal')
                itens_codigo_servico_produto = int(row.get('itens.codigo_servico_produto', '0'))
                itens_valor_unitario = format_decimal(row.get('itens.valor_unitario', '0'))
                itens_quantidade = int(row.get('itens.quantidade', '1'))
                itens_codigo_lote_serie = int(row.get('itens.codigo_lote_serie', '0'))
                parcelas_valor = format_decimal(row.get('parcelas.valor', '0'))
                parcelas_vencimento = row.get('parcelas.vencimento')
                parcelas_pagamento_a_vista = row.get('parcelas.pagamento_a_vista') == 'true'
                codigo_operacao_fiscal = int(row.get('codigo_operacao_fiscal', '0'))
                codigo_banco_conta = int(row.get('codigo_banco_conta', '0'))
                codigo_forma_pagamento = int(row.get('codigo_forma_pagamento', '0'))
                codigo_condicao_pagamento = int(row.get('codigo_condicao_pagamento', '0'))
                codigo_vendedor = int(row.get('codigo_vendedor', '0'))
                codigo_situacao = int(row.get('codigo_situacao', '0'))
                valor_frete = format_decimal(row.get('valor_frete', '0'))
                tipo_frete = row.get('tipo_frete')
                codigo_transportador = int(row.get('codigo_transportador', '0'))

                # Criar o dicionário para o pedido de venda
                pedido_venda = {
                    "cnpj_cpf_cliente": cnpj_cpf_cliente,
                    "data_emissao": data_emissao,
                    "data_entrega": data_entrega,
                    "observacao_fiscal": observacao_fiscal,
                    "itens": [
                        {
                            "codigo_servico_produto": itens_codigo_servico_produto,
                            "valor_unitario": itens_valor_unitario,
                            "quantidade": itens_quantidade,
                            "codigo_lote_serie": itens_codigo_lote_serie
                        }
                    ],
                    "parcelas": [
                        {
                            "valor": parcelas_valor,
                            "vencimento": parcelas_vencimento,
                            "pagamento_a_vista": parcelas_pagamento_a_vista
                        }
                    ],
                    "codigo_operacao_fiscal": codigo_operacao_fiscal,
                    "codigo_banco_conta": codigo_banco_conta,
                    "codigo_forma_pagamento": codigo_forma_pagamento,
                    "codigo_condicao_pagamento": codigo_condicao_pagamento,
                    "codigo_vendedor": codigo_vendedor,
                    "codigo_situacao": codigo_situacao,
                    "valor_frete": valor_frete,
                    "tipo_frete": tipo_frete,
                    "codigo_transportador": codigo_transportador
                }

                try:
                    response = requests.post(url, headers=headers, json=pedido_venda)
                    response.raise_for_status()
                    response_data = response.json()

                    if response_data.get("Sucesso"):
                        logging.info(f"Sucesso: {response_data.get('Mensagem', 'Mensagem não disponível')}, ID: {response_data.get('Id', 'N/A')}")
                    else:
                        logging.error(f"Erro: {response_data.get('Mensagem', 'Mensagem não disponível')}, ID: {response_data.get('Id', 'N/A')}")
                        erro_writer.writerow([cnpj_cpf_cliente, data_emissao, data_entrega, observacao_fiscal,
                                             itens_codigo_servico_produto, itens_valor_unitario, itens_quantidade,
                                             itens_codigo_lote_serie, parcelas_valor, parcelas_vencimento, parcelas_pagamento_a_vista,
                                             codigo_operacao_fiscal, codigo_banco_conta, codigo_forma_pagamento,
                                             codigo_condicao_pagamento, codigo_vendedor, codigo_situacao, valor_frete,
                                             tipo_frete, codigo_transportador, response_data.get("Mensagem", "Mensagem não disponível"),
                                             response_data.get("Id", "N/A")])

                except requests.exceptions.RequestException as err:
                    logging.error(f"Erro de Requisição: {err}")
                    erro_writer.writerow([cnpj_cpf_cliente, data_emissao, data_entrega, observacao_fiscal,
                                         itens_codigo_servico_produto, itens_valor_unitario, itens_quantidade,
                                         itens_codigo_lote_serie, parcelas_valor, parcelas_vencimento, parcelas_pagamento_a_vista,
                                         codigo_operacao_fiscal, codigo_banco_conta, codigo_forma_pagamento,
                                         codigo_condicao_pagamento, codigo_vendedor, codigo_situacao, valor_frete,
                                         tipo_frete, codigo_transportador, f"Erro de Requisição: {err}", "N/A"])

                # Adiciona um tempo de espera para não sobrecarregar o servidor
                time.sleep(0.2)

    except FileNotFoundError as e:
        logging.error(f"Erro ao tentar abrir o arquivo CSV: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado: {e}")
