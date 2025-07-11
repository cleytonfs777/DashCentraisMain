from fastapi import FastAPI, Response
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, text
import pandas as pd
import io
import os
from sshtunnel import SSHTunnelForwarder
import pymysql

app = FastAPI(title="API Telefonia CBMMG")

DB_URL = os.getenv("LOCAL_DB_URL")
engine = create_engine(DB_URL)

# Variáveis para conexão remota
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

REMOTE_DB_HOST = os.getenv("REMOTE_DB_HOST", "127.0.0.1")
REMOTE_DB_PORT = int(os.getenv("REMOTE_DB_PORT", 3306))
REMOTE_DB_USER = os.getenv("REMOTE_DB_USER")
REMOTE_DB_PASS = os.getenv("REMOTE_DB_PASS")
REMOTE_DB_NAME = os.getenv("REMOTE_DB_NAME")

ESTADO_MAP = {"atendida": 1, "abandonado": 0}

@app.get("/")
def health_check():
    return {"status": "alive"}

@app.get("/api/fato_chamadas")
def get_fato_chamadas():
    query = text("""
        SELECT * FROM fato_chamadas 
        ORDER BY data DESC, hora DESC 
        LIMIT 1000
    """)
    with engine.connect() as conn:
        result = conn.execute(query).mappings().all()
        return result

@app.get("/api/export-csv")
def export_all_data_csv():
    """
    Extrai todos os dados da tabela meso_detalhe do banco remoto e retorna como CSV
    """
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASS,
            remote_bind_address=(REMOTE_DB_HOST, REMOTE_DB_PORT)
        ) as tunnel:
            
            conn_str = f"mysql+pymysql://{REMOTE_DB_USER}:{REMOTE_DB_PASS}@127.0.0.1:{tunnel.local_bind_port}/{REMOTE_DB_NAME}"
            remote_engine = create_engine(conn_str)
            
            # Query para extrair todos os dados
            query = """
                SELECT datahora, duracao, fila, holdtime, teleatendente, estado
                FROM meso_detalhe
                ORDER BY datahora
            """
            
            with remote_engine.connect() as conn:
                df = pd.read_sql(query, conn)
            
            # Aplica as transformações
            df["data"] = pd.to_datetime(df["datahora"]).dt.date
            df["hora"] = pd.to_datetime(df["datahora"]).dt.time
            df.drop(columns=["datahora"], inplace=True)
            
            # Substitui NULL por 0 em duracao e holdtime
            df["duracao"] = df["duracao"].fillna(0).astype(int)
            df["holdtime"] = df["holdtime"].fillna(0).astype(int)
            
            # Mapeia estado para número
            df["estado"] = df["estado"].map(ESTADO_MAP).fillna(-1).astype(int)
            
            # Adiciona coluna cob
            df["cob"] = "52"
            
            # Converte para CSV
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()
            
            # Retorna como resposta de download
            return StreamingResponse(
                io.BytesIO(csv_content.encode('utf-8')),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=dados_centrais_telefonicas.csv"}
            )
            
    except Exception as e:
        return {"error": f"Erro ao exportar dados: {str(e)}"}

# Você pode continuar adicionando os demais endpoints aqui

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
