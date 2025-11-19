from azure.storage.blob import BlobServiceClient, PublicAccess
import os

# Tenta pegar da variável de ambiente, mas se não tiver, usa a string fixa do seu storage
AZURE_BLOB_CONNECTION = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or (
    "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;"
    "AccountName=stpregaoandre123;"
    "AccountKey=EbVkcI3ffgQJ9a1o0S+tNEIZ8UiutEwVoL9dZUMJCfHrnncnP7lnRJD1NSWhL+NRnfN6m64KcXZV+AStr2chgQ==;"
    "BlobEndpoint=https://stpregaoandre123.blob.core.windows.net/;"
    "FileEndpoint=https://stpregaoandre123.file.core.windows.net/;"
    "QueueEndpoint=https://stpregaoandre123.queue.core.windows.net/;"
    "TableEndpoint=https://stpregaoandre123.table.core.windows.net/;"
)


# Nome do container que usamos na Function e no projeto
CONTAINER = "pregao-xml"


def _get_service():
    if not AZURE_BLOB_CONNECTION:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING não configurada")
    return BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION)


def save_file_to_blob(file_name, local_path_file):
    """
    Envia um arquivo local para o Blob, no container pregao-xml.
    """
    service = _get_service()
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception:
        # container já existe
        pass

    with open(local_path_file, "rb") as data:
        container.upload_blob(name=file_name, data=data, overwrite=True)


def get_file_from_blob(file_name):
    """
    Baixa um arquivo de texto do Blob e retorna seu conteúdo em string.
    """
    service = _get_service()
    container = service.get_container_client(CONTAINER)
    try:
        service.create_container(CONTAINER, public_access=PublicAccess.Container)
    except Exception:
        # container já existe
        pass

    blob_client = container.get_blob_client(file_name)

    try:
        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall().decode("utf-8")
        return blob_content
    except Exception as e:
        print("Erro ao obter arquivo", e)
        return None
