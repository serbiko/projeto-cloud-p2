from datetime import datetime, timedelta
from helpers import yymmdd
import requests
import os
import zipfile
from azure_storage import save_file_to_blob
import shutil
import tempfile

# MUDANÇA: Usar diretório temporário do sistema
PATH_TO_SAVE = tempfile.gettempdir()  # ← MUDANÇA AQUI!


def build_url_download(date_to_download: str) -> str:
    """
    Monta a URL de download do arquivo SPRE<date>.zip na B3.
    """
    return f"https://www.b3.com.br/pesquisapregao/download?filelist=SPRE{date_to_download}.zip"


def try_http_download(url: str):
    """
    Faz o GET na URL e valida se o conteúdo é um zip válido.
    """
    session = requests.Session()
    try:
        print(f"[INFO] Tentando {url}")
        resp = session.get(url, timeout=30)
        if resp.ok and resp.content and len(resp.content) > 200:
            if resp.content[:2] == b"PK":
                return resp.content, os.path.basename(url)
    except requests.RequestException as e:
        print(f"[ERROR] Falha ao acessar {url}: {e}")
    return None, None


def run():
    """
    Baixa o XML do pregão do dia anterior
    """
    import logging
    
    # Data do pregão: dia anterior
    data_pregao = datetime.now() - timedelta(days=1)
    dt = yymmdd(data_pregao)
    
    logging.info(f"[DOWNLOAD] Baixando dados do pregão de {data_pregao.strftime('%d/%m/%Y')}")
    
    url_to_download = build_url_download(dt)

    # 1) Download do zip
    zip_bytes, zip_name = try_http_download(url_to_download)
    if not zip_bytes:
        raise RuntimeError(f"Não foi possível baixar o arquivo para {dt}")

    logging.info(f"[DOWNLOAD] ✓ Arquivo baixado: {zip_name}")

    # 2) Criar diretório temporário único para esta execução
    temp_dir = os.path.join(PATH_TO_SAVE, f"pregao_{dt}_{os.getpid()}")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # 3) Salvar zip temporariamente
        zip_path = os.path.join(temp_dir, f"pregao_{dt}.zip")
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)

        logging.info(f"[DOWNLOAD] Zip salvo temporariamente")

        # 4) Extrair primeira camada
        extract_dir_1 = os.path.join(temp_dir, "extract1")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(extract_dir_1)

        # 5) Extrair SPRE<dt>.zip (que está dentro)
        inner_zip_path = os.path.join(extract_dir_1, f"SPRE{dt}.zip")
        extract_dir_2 = os.path.join(temp_dir, "extract2")
        
        with zipfile.ZipFile(inner_zip_path, "r") as zf:
            zf.extractall(extract_dir_2)

        # 6) Enviar XML para o Blob
        arquivos = [f for f in os.listdir(extract_dir_2) if f.endswith('.xml')]
        
        if not arquivos:
            raise RuntimeError("Nenhum arquivo XML encontrado")
        
        for arquivo in arquivos:
            caminho_arquivo = os.path.join(extract_dir_2, arquivo)
            blob_name = f"BVBG186_{dt}.xml"
            
            logging.info(f"[UPLOAD] Enviando {blob_name} para Blob Storage...")
            save_file_to_blob(blob_name, caminho_arquivo)
            logging.info(f"[UPLOAD] ✓ Arquivo enviado com sucesso!")
            
    finally:
        # 7) Limpar temporários (sempre executa, mesmo com erro)
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                logging.info("[CLEANUP] Arquivos temporários removidos")
        except Exception as e:
            logging.warning(f"[CLEANUP] Erro ao limpar temporários: {e}")

    logging.info("[DOWNLOAD] ✓ Processo concluído!")


if __name__ == "__main__":
    run()