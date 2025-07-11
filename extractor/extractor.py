import os
import time
import pandas as pd
import paramiko
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, text
import pymysql
from dotenv import load_dotenv
load_dotenv(dotenv_path="../.env")  # se estiver na raiz do projeto

# Carrega variáveis do ambiente
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

REMOTE_DB_HOST = os.getenv("REMOTE_DB_HOST", "127.0.0.1")
REMOTE_DB_PORT = int(os.getenv("REMOTE_DB_PORT", 3306))
REMOTE_DB_USER = os.getenv("REMOTE_DB_USER")
REMOTE_DB_PASS = os.getenv("REMOTE_DB_PASS")
REMOTE_DB_NAME = os.getenv("REMOTE_DB_NAME")

LOCAL_DB_URL = os.getenv("LOCAL_DB_URL")
# LOCAL_DB_URL = os.getenv("HOST_DB_URL")
COB_ID = 52  # fixo por enquanto

ESTADO_MAP = {"atendida": 1, "abandonado": 0}

def get_last_datetime(engine):
    """Busca a data/hora mais recente no banco local"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT MAX(data) as ultima_data, MAX(hora) as ultima_hora 
                FROM fato_chamadas 
                WHERE data = (SELECT MAX(data) FROM fato_chamadas)
            """)
            result = conn.execute(query).mappings().first()
            
            if result and result['ultima_data'] and result['ultima_hora']:
                return pd.Timestamp.combine(result['ultima_data'], result['ultima_hora'])
            return pd.Timestamp('2000-01-01')  # Data inicial caso tabela esteja vazia
    except Exception as e:
        print(f"Erro ao buscar última data: {e}")
        return pd.Timestamp('2000-01-01')

def extract_and_load():
    # Aguarda a conexão com o banco estar disponível
    print("Verificando conexão com o banco de dados...")
    while True:
        try:
            local_engine = create_engine(LOCAL_DB_URL)
            with local_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Conexão com banco local estabelecida!")
            break
        except Exception as e:
            print(f"Aguardando banco... {e}")
            time.sleep(2)
    
    # Cria a tabela com índices se não existir
    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS fato_chamadas (
        data DATE,
        hora TIME,
        duracao INTEGER,
        fila VARCHAR(10),
        holdtime INTEGER,
        teleatendente VARCHAR(100),
        estado INTEGER,
        cob VARCHAR(3)
    );
    
    -- Índices para melhorar performance
    CREATE INDEX IF NOT EXISTS idx_fato_chamadas_data ON fato_chamadas(data);
    CREATE INDEX IF NOT EXISTS idx_fato_chamadas_data_hora ON fato_chamadas(data, hora);
    CREATE INDEX IF NOT EXISTS idx_fato_chamadas_estado ON fato_chamadas(estado);
    '''
    with local_engine.begin() as conn:
        conn.execute(text(create_table_sql))

    while True:
        try:
            print("=== INICIANDO NOVA EXTRAÇÃO ===")
            ultima_atualizacao = get_last_datetime(local_engine)
            
            # Query com filtro de data/hora (se banco vazio, traz todos os registros)
            if ultima_atualizacao == pd.Timestamp('2000-01-01'):
                # Para primeira carga, vamos processar em lotes de 1000 registros
                print("Banco vazio - carregando registros em lotes...")
                
                # Primeiro, conta quantos registros existem
                print("Estabelecendo túnel SSH...")
                with SSHTunnelForwarder(
                    (SSH_HOST, SSH_PORT),
                    ssh_username=SSH_USER,
                    ssh_password=SSH_PASS,
                    remote_bind_address=(REMOTE_DB_HOST, REMOTE_DB_PORT)
                ) as tunnel:
                    conn_str = f"mysql+pymysql://{REMOTE_DB_USER}:{REMOTE_DB_PASS}@127.0.0.1:{tunnel.local_bind_port}/{REMOTE_DB_NAME}"
                    remote_engine = create_engine(conn_str)
                    
                    with remote_engine.connect() as conn:
                        count_result = conn.execute(text("SELECT COUNT(*) as total FROM meso_detalhe")).mappings().first()
                        total_records = count_result['total']
                        print(f"Total de registros a processar: {total_records}")
                        
                        batch_size = 1000
                        offset = 0
                        
                        while offset < total_records:
                            print(f"Processando lote {offset//batch_size + 1}: registros {offset+1} a {min(offset+batch_size, total_records)}")
                            
                            batch_query = f"""
                                SELECT datahora, duracao, fila, holdtime, teleatendente, estado
                                FROM meso_detalhe
                                ORDER BY datahora
                                LIMIT {batch_size} OFFSET {offset}
                            """
                            
                            df = pd.read_sql(batch_query, conn)
                            
                            if not df.empty:
                                # Processa o lote
                                df["data"] = pd.to_datetime(df["datahora"]).dt.date
                                df["hora"] = pd.to_datetime(df["datahora"]).dt.time
                                df.drop(columns=["datahora"], inplace=True)
                                df["duracao"] = df["duracao"].fillna(0).astype(int)
                                df["holdtime"] = df["holdtime"].fillna(0).astype(int)
                                df["estado"] = df["estado"].map(ESTADO_MAP).fillna(-1).astype(int)
                                df["cob"] = "52"
                                
                                # Salva o lote
                                with local_engine.begin() as local_conn:
                                    df.to_sql("fato_chamadas", local_conn, index=False, if_exists="append")
                                
                                print(f"✅ Lote processado: {len(df)} registros inseridos")
                            
                            offset += batch_size
                        
                        print(f"✅ Carga inicial completa! {total_records} registros processados")
                
                # Pula para o próximo ciclo (não executa o código de busca incremental)
                continue
            
            else:
                QUERY = f"""
                    SELECT datahora, duracao, fila, holdtime, teleatendente, estado
                    FROM meso_detalhe
                    WHERE datahora > '{ultima_atualizacao}'
                    ORDER BY datahora
                """
                print(f"Buscando registros mais recentes que {ultima_atualizacao}...")
            
                print("Estabelecendo túnel SSH...")
                with SSHTunnelForwarder(
                    (SSH_HOST, SSH_PORT),
                    ssh_username=SSH_USER,
                    ssh_password=SSH_PASS,
                    remote_bind_address=(REMOTE_DB_HOST, REMOTE_DB_PORT)
                ) as tunnel:
                    print(f"Túnel SSH estabelecido na porta local: {tunnel.local_bind_port}")
                    conn_str = f"mysql+pymysql://{REMOTE_DB_USER}:{REMOTE_DB_PASS}@127.0.0.1:{tunnel.local_bind_port}/{REMOTE_DB_NAME}"
                    remote_engine = create_engine(conn_str)

                    print("Executando query no banco remoto...")
                    with remote_engine.connect() as conn:
                        # Agora executa a query incremental
                        print(f"Executando query: {QUERY}")
                        df = pd.read_sql(QUERY, conn)
                        print(f"Query executada. Registros encontrados: {len(df)}")

                    if not df.empty:
                        # Divide datahora em data e hora
                        df["data"] = pd.to_datetime(df["datahora"]).dt.date
                        df["hora"] = pd.to_datetime(df["datahora"]).dt.time
                        df.drop(columns=["datahora"], inplace=True)

                        # Substitui NULL por 0 em duracao e holdtime
                        df["duracao"] = df["duracao"].fillna(0).astype(int)
                        df["holdtime"] = df["holdtime"].fillna(0).astype(int)

                        # Mapeia estado para número
                        df["estado"] = df["estado"].map(ESTADO_MAP).fillna(-1).astype(int)
                        
                        # Adiciona coluna cob com valor fixo '52'
                        df["cob"] = "52"

                        print(f"Dados novos extraídos: {len(df)} registros.")

                        # Insere apenas registros novos
                        with local_engine.begin() as conn:
                            df.to_sql("fato_chamadas", conn, index=False, if_exists="append")
                            print("✅ Dados carregados em fato_chamadas.")
                    else:
                        print("Nenhum registro novo encontrado.")

        except Exception as e:
            print(f"Erro durante a extração: {e}")
        
        # Aguarda 2 minutos antes da próxima execução
        print("Aguardando 2 minutos para próxima execução...")
        time.sleep(120)

if __name__ == "__main__":
    extract_and_load()
