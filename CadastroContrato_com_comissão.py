import csv
import time
import requests
import chardet
import logging
from collections import defaultdict

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# URL do webservice
url = "https://datapay.flyerp.com.br/apis/CadastrarContrato"

# Bearer Token para autenticação
bearer_token = "1b7caa65-ce95-44bf-9c96-b33f2f7edf83"

# Cabeçalhos da requisição
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}

# Caminhos dos arquivos
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/testedatapay.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/registros_falhados_testedatapay.csv"

def format_decimal(value):
    try:
        return float(str(value).replace('.', '').replace(',', '.')) if value else 0.0
    except:
        return 0.0

# Detectar codificação do arquivo CSV
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result.get('encoding', 'utf-8-sig')
logging.info(f"Codificação detectada: {encoding}")

# Criar arquivo de erro com cabeçalho
with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
    erro_writer = csv.writer(erro_file)
    erro_writer.writerow([
        'cnpj_cpf_cliente', 'referencia', 'referencia_busca_cliente', 'data_primeiro_vencimento',
        'dia_vencimento', 'descricao', 'numero', 'codigo_vendedor', 'data_vigencia_inicial',
        'data_vigencia_final', 'situacao', 'recorrencia', 'tipo_faturamento', 'indice_reajuste',
        'codigo_banco_conta', 'codigo_forma_pagamento', 'codigo_plano_contas',
        'codigo_condicao_pagamento', 'observacao_fiscal', 'observacao_nao_fiscal',
        'data_proximo_reajuste', 'servicos.referencia', 'servicos.quantidade',
        'servicos.valor_unitario', 'servicos.complemento', 'tipo_desconto_boleto_ate_vencimento',
        'valor_desconto_boleto_ate_vencimento', 'id_importacao_pessoa', 'Mensagem', 'Id'
    ])

try:
    with open(csv_file_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')
        reader.fieldnames = [field.strip() for field in reader.fieldnames]

        contratos = defaultdict(list)
        for row in reader:
            referencia = row.get('referencia', '')
            if referencia:
                contratos[referencia].append(row)

        with open(erro_csv_file_path, mode='a', newline='', encoding='utf-8-sig') as erro_file:
            erro_writer = csv.writer(erro_file)

            for referencia, linhas in contratos.items():
                try:
                    if not linhas:
                        continue

                    contrato_data = linhas[0]

                    servicos = []
                    for linha in linhas:
                        servico = {
                            "referencia": linha.get('servicos.referencia', ''),
                            "quantidade": int(linha.get('servicos.quantidade', 1)),
                            "valor_unitario": format_decimal(linha.get('servicos.valor_unitario', '0')),
                            "complemento": linha.get('servicos.complemento', ''),
                            "nao_faturavel": linha.get('servicos.nao_faturavel', '').lower() == 'true',
                            "data_inicio_servico": linha.get('servicos.data_inicio_servico', ''),
                            "data_reajuste": linha.get('servicos.data_reajuste', '')
                        }

                        # Adiciona comissão se existir
                        if linha.get('comissao.codigo_politica_comissao'):
                            servico["comissao"] = {
                                "codigo_politica_comissao": int(linha.get('comissao.codigo_politica_comissao', 0)),
                                "codigo_vendedor_1": int(linha.get('comissao.codigo_vendedor_1', 0)) if linha.get('comissao.codigo_vendedor_1') else None,
                                "codigo_vendedor_2": int(linha.get('comissao.codigo_vendedor_2', 0)) if linha.get('comissao.codigo_vendedor_2') else None,
                                "codigo_vendedor_3": int(linha.get('comissao.codigo_vendedor_3', 0)) if linha.get('comissao.codigo_vendedor_3') else None
                            }

                        servicos.append(servico)

                    payload = {
                        "cnpj_cpf_cliente": contrato_data.get('cnpj_cpf_cliente', ''),
                        "referencia": referencia,
                        "referencia_busca_cliente": contrato_data.get('referencia_busca_cliente', '0'),
                        "data_primeiro_vencimento": contrato_data.get('data_primeiro_vencimento', ''),
                        "dia_vencimento": int(contrato_data.get('dia_vencimento', 1)),
                        "descricao": contrato_data.get('descricao', ''),
                        "numero": contrato_data.get('numero', ''),
                        "codigo_vendedor": int(contrato_data.get('codigo_vendedor', 0)),
                        "data_vigencia_inicial": contrato_data.get('data_vigencia_inicial', ''),
                        "data_vigencia_final": contrato_data.get('data_vigencia_final', ''),
                        "situacao": contrato_data.get('situacao', ''),
                        "recorrencia": int(contrato_data.get('recorrencia', 1)),
                        "tipo_faturamento": int(contrato_data.get('tipo_faturamento', 0)),
                        "indice_reajuste": int(contrato_data.get('indice_reajuste', 0)),
                        "codigo_banco_conta": int(contrato_data.get('codigo_banco_conta', 0)),
                        "codigo_forma_pagamento": int(contrato_data.get('codigo_forma_pagamento', 0)),
                        "codigo_plano_contas": int(contrato_data.get('codigo_plano_contas', 0)),
                        "codigo_condicao_pagamento": int(contrato_data.get('codigo_condicao_pagamento', 0)),
                        "observacao_fiscal": contrato_data.get('observacao_fiscal', ''),
                        "observacao_nao_fiscal": contrato_data.get('observacao_nao_fiscal', ''),
                        "data_proximo_reajuste": contrato_data.get('data_proximo_reajuste', ''),
                        "tipo_desconto_boleto_ate_vencimento": int(contrato_data.get('tipo_desconto_boleto_ate_vencimento', 0)),
                        "valor_desconto_boleto_ate_vencimento": format_decimal(contrato_data.get('valor_desconto_boleto_ate_vencimento', '0')),
                        "id_importacao_pessoa": contrato_data.get('id_importacao_pessoa', ''),
                        "servicos": servicos
                    }

                    # Adicionar cliente se existir
                    if 'cliente.cnpj_cpf' in contrato_data:
                        payload["cliente"] = {
                            "cnpj_cpf": contrato_data.get('cliente.cnpj_cpf', ''),
                            "inscricao_estadual_RG": contrato_data.get('cliente.inscricao_estadual_RG', ''),
                            "razao_social_nome": contrato_data.get('cliente.razao_social_nome', ''),
                            "nome_fantasia_apelido": contrato_data.get('cliente.nome_fantasia_apelido', ''),
                            "endereco": contrato_data.get('cliente.endereco', ''),
                            "numero": contrato_data.get('cliente.numero', ''),
                            "complemento": contrato_data.get('cliente.complemento', ''),
                            "bairro": contrato_data.get('cliente.bairro', ''),
                            "cidade": contrato_data.get('cliente.cidade', ''),
                            "estado": contrato_data.get('cliente.estado', ''),
                            "cep": contrato_data.get('cliente.cep', ''),
                            "telefone1": contrato_data.get('cliente.telefone1', ''),
                            "telefone2": contrato_data.get('cliente.telefone2', ''),
                            "contato_nome": contrato_data.get('cliente.contato_nome', ''),
                            "contato_telefone": contrato_data.get('cliente.contato_telefone', ''),
                            "email": contrato_data.get('cliente.email', ''),
                            "observacao": contrato_data.get('cliente.observacao', ''),
                            "codigo_vendedor": int(contrato_data.get('cliente.codigo_vendedor', 0)),
                            "codigo_ramo_atividade": int(contrato_data.get('cliente.codigo_ramo_atividade', 0)),
                            "somente_criacao": contrato_data.get('cliente.somente_criacao', '').lower() == 'true'
                        }

                    # Adicionar cartão de crédito se existir
                    if 'cartao_credito.numero_cartao' in contrato_data:
                        payload["cartao_credito"] = {
                            "nome_titular": contrato_data.get('cartao_credito.nome_titular', ''),
                            "cnpj_cpf_titular": contrato_data.get('cartao_credito.cnpj_cpf_titular', ''),
                            "numero_cartao": contrato_data.get('cartao_credito.numero_cartao', ''),
                            "mes_validade": contrato_data.get('cartao_credito.mes_validade', ''),
                            "ano_validade": contrato_data.get('cartao_credito.ano_validade', ''),
                            "codigo_seguranca": contrato_data.get('cartao_credito.codigo_seguranca', ''),
                            "ip_remoto": contrato_data.get('cartao_credito.ip_remoto', ''),
                            "titular": {
                                "nome": contrato_data.get('cartao_credito.titular.nome', ''),
                                "email": contrato_data.get('cartao_credito.titular.email', ''),
                                "cnpj_cpf": contrato_data.get('cartao_credito.titular.cnpj_cpf', ''),
                                "cep": contrato_data.get('cartao_credito.titular.cep', ''),
                                "numero": contrato_data.get('cartao_credito.titular.numero', ''),
                                "complemento": contrato_data.get('cartao_credito.titular.complemento', ''),
                                "telefone": contrato_data.get('cartao_credito.titular.telefone', ''),
                                "celular": contrato_data.get('cartao_credito.titular.celular', '')
                            }
                        }

                    response = requests.post(url, headers=headers, json=payload)
                    response_data = response.json()

                    if response.status_code != 200 or not response_data.get("Sucesso", False):
                        for linha in linhas:
                            erro_writer.writerow([
                                linha.get('cnpj_cpf_cliente', ''), referencia,
                                linha.get('referencia_busca_cliente', '0'),
                                linha.get('data_primeiro_vencimento', ''),
                                linha.get('dia_vencimento', ''), linha.get('descricao', ''),
                                linha.get('numero', ''), linha.get('codigo_vendedor', '0'),
                                linha.get('data_vigencia_inicial', ''),
                                linha.get('data_vigencia_final', ''),
                                linha.get('situacao', ''), linha.get('recorrencia', ''),
                                linha.get('tipo_faturamento', ''),
                                linha.get('indice_reajuste', ''),
                                linha.get('codigo_banco_conta', '0'),
                                linha.get('codigo_forma_pagamento', '0'),
                                linha.get('codigo_plano_contas', '0'),
                                linha.get('codigo_condicao_pagamento', '0'),
                                linha.get('observacao_fiscal', ''),
                                linha.get('observacao_nao_fiscal', ''),
                                linha.get('data_proximo_reajuste', ''),
                                linha.get('servicos.referencia', ''),
                                linha.get('servicos.quantidade', '1'),
                                linha.get('servicos.valor_unitario', '0'),
                                linha.get('servicos.complemento', ''),
                                linha.get('tipo_desconto_boleto_ate_vencimento', '0'),
                                linha.get('valor_desconto_boleto_ate_vencimento', '0'),
                                linha.get('id_importacao_pessoa', ''),
                                response_data.get("Mensagem", "Erro desconhecido"),
                                response_data.get("Id", "N/A")
                            ])
                        logging.error(f"Erro ao cadastrar contrato {referencia}: {response_data.get('Mensagem', 'Erro desconhecido')}")
                    else:
                        logging.info(f"Contrato {referencia} cadastrado com sucesso! ID: {response_data.get('Id')}")

                    time.sleep(0.2)

                except Exception as e:
                    logging.error(f"Erro ao processar contrato {referencia}: {str(e)}")
                    for linha in linhas:
                        erro_writer.writerow(list(linha.values()) + [f"Erro de processamento: {str(e)}", "N/A"])
                    continue

except Exception as e:
    logging.error(f"Erro inesperado ao processar arquivo: {e}")
