import csv
import time
import requests
import chardet
import logging
from datetime import datetime
print("Script iniciado.")
logging.basicConfig(filename='contas_a_pagar.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Corrigido o endpoint para ContasAPagar
url = "https://raffinato.flyerp.com.br/APIs/ImportarContasAPagar"
bearer_token = "b20f764f-4736-40a9-9d48-54f36bc5d2bb"
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Content-Type": "application/json"
}
csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/Contas_pagas_fic_31.03_csv.csv"
erro_csv_file_path = r"D:/FlyERP/Dev/Automação de cadastros/Pasta de Arquivos/FALHAS_Contas_pagas_fic_31.03_csv.csv"
print("Detectando codificação do CSV...")
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
encoding = result['encoding']
print(f"Codificação detectada: {encoding}")
logging.info(f"Codificacao detectada: {encoding}")
def limpar_texto(texto):
    return texto.replace("\xa0", " ").strip() if texto else None
def formatar_data(data_str):
    try:
        return datetime.strptime(data_str, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        return None
def converter_valor(valor_str):
    try:
        return float(valor_str.replace(',', '.'))
    except:
        return None
try:
    with open(csv_file_path, mode='r', encoding=encoding) as file:
        reader = csv.DictReader(file, delimiter=';')
        if reader.fieldnames:
            print(f"Colunas detectadas no CSV: {reader.fieldnames}")
        else:
            print(":atenção: Nenhuma coluna detectada no CSV.")
            raise ValueError("CSV sem cabeçalho válido.")
        reader.fieldnames = [field.strip() for field in reader.fieldnames]
        with open(erro_csv_file_path, mode='w', newline='', encoding='utf-8-sig') as erro_file:
            erro_writer = csv.writer(erro_file)
            erro_writer.writerow([
                "id", "nome_cliente", "cnpj_cliente", "id_importacao_cliente", "chave_pix",
                "numero", "parcela", "emissao", "vencimento", "ultimo_pagamento", "valor",
                "valor_pago", "juros", "desconto", "numero_nf", "serie_nf", "banco",
                "plano_contas", "forma_pagamento", "observacao",
                "nome_centro_resultados_1", "percentual_centro_resultados_1",
                "nome_centro_resultados_2", "percentual_centro_resultados_2",
                "referencia_busca_pessoa", "Mensagem", "Id"
            ])
            linha = 0
            print("Iniciando processamento das linhas...")
            for row in reader:
                linha += 1
                print(f"Processando linha {linha}: {row}")
                # Convertendo dados para os formatos corretos
                conta = {
                    "id": limpar_texto(row.get("id")),
                    "nome_cliente": limpar_texto(row.get("nome_cliente")),
                    "cnpj_cliente": limpar_texto(row.get("cnpj_cliente")),
                    "id_importacao_cliente": limpar_texto(row.get("id_importacao_cliente")),
                    "chave_pix": limpar_texto(row.get("chave_pix")),
                    "numero": limpar_texto(row.get("numero")),
                    "parcela": limpar_texto(row.get("parcela")),
                    "emissao": formatar_data(limpar_texto(row.get("emissao"))),
                    "vencimento": formatar_data(limpar_texto(row.get("vencimento"))),
                    "ultimo_pagamento": formatar_data(limpar_texto(row.get("ultimo_pagamento"))),
                    "valor": converter_valor(limpar_texto(row.get("valor"))),
                    "valor_pago": converter_valor(limpar_texto(row.get("valor_pago"))),
                    "juros": converter_valor(limpar_texto(row.get("juros"))),
                    "desconto": converter_valor(limpar_texto(row.get("desconto"))),
                    "numero_nf": limpar_texto(row.get("numero_nf")),
                    "serie_nf": limpar_texto(row.get("serie_nf")),
                    "banco": limpar_texto(row.get("banco")),
                    "plano_contas": limpar_texto(row.get("plano_contas")),
                    "forma_pagamento": limpar_texto(row.get("forma_pagamento")),
                    "observacao": limpar_texto(row.get("observacao")),
                    "nome_centro_resultados_1": limpar_texto(row.get("nome_centro_resultados_1")),
                    "percentual_centro_resultados_1": converter_valor(limpar_texto(row.get("percentual_centro_resultados_1"))),
                    "nome_centro_resultados_2": limpar_texto(row.get("nome_centro_resultados_2")),
                    "percentual_centro_resultados_2": converter_valor(limpar_texto(row.get("percentual_centro_resultados_2"))),
                    "referencia_busca_pessoa": int(row.get("referencia_busca_pessoa", 0))
                }
                # Remover campos None para evitar envio de campos vazios
                conta = {k: v for k, v in conta.items() if v is not None}
                print(f"Enviando requisição: {conta}")
                logging.info(f"Enviando requisição: {conta}")
                try:
                    response = requests.post(url, headers=headers, json=conta)
                    # Log mais detalhado da resposta
                    print(f"Status Code: {response.status_code}")
                    print(f"Response: {response.text}")
                    response.raise_for_status()
                    response_data = response.json()
                    if not response_data.get("Sucesso"):
                        erro_msg = response_data.get('Mensagem', 'Erro desconhecido')
                        print(f":x_vermelho: Falha no envio da linha {linha}: {erro_msg}")
                        erro_writer.writerow(list(row.values()) + [erro_msg, response_data.get("Id", "N/A")])
                    else:
                        print(f":marca_de_verificação_branca: Linha {linha} processada com sucesso!")
                except requests.exceptions.HTTPError as e:
                    erro_msg = f"HTTP Error: {e.response.status_code} - {e.response.text}"
                    logging.error(f"Erro ao enviar dados na linha {linha}: {erro_msg}")
                    print(f":atenção: Erro na requisição da linha {linha}: {erro_msg}")
                    erro_writer.writerow(list(row.values()) + [erro_msg, "N/A"])
                except Exception as e:
                    logging.error(f"Erro ao enviar dados na linha {linha}: {str(e)}")
                    print(f":atenção: Erro na requisição da linha {linha}: {str(e)}")
                    erro_writer.writerow(list(row.values()) + [str(e), "N/A"])
                time.sleep(0.2)
    print(":marca_de_verificação_branca: Processamento finalizado.")
except Exception as e:
    logging.error(f"Erro inesperado: {e}")
    print(f":x_vermelho: Erro inesperado: {e}")
