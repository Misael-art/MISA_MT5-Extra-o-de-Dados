import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import re
import json

from mt5_extracao.error_handler import with_error_handling, ExportError

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Adicionar um handler de console para depuração inicial
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    # Adicionar um handler de arquivo
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/data_exporter.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

class DataExporter:
    """
    Exporta dados do banco de dados para vários formatos (CSV, Excel).
    
    Esta classe fornece métodos para exportar dados armazenados no banco
    de dados SQLite para formatos como CSV e Excel, facilitando a análise
    externa dos dados coletados.
    """
    
    def __init__(self, db_manager):
        """
        Inicializa o exportador com referência ao DatabaseManager.
        
        Args:
            db_manager: Instância do DatabaseManager para acessar os dados
        """
        self.db_manager = db_manager
        log.info("DataExporter inicializado")
        
        # Diretório padrão para exportações
        self.export_dir = Path("exports")
        os.makedirs(self.export_dir, exist_ok=True)
    
    @with_error_handling(error_type=ExportError, retry_count=1)
    def export_to_csv(self, tabela, caminho_arquivo=None, filtros=None, adicionar_timestamp=True):
        """
        Exporta dados de uma tabela para CSV.
        
        Args:
            tabela (str): Nome da tabela a ser exportada
            caminho_arquivo (str, optional): Caminho onde salvar o arquivo CSV.
                                           Se None, usa exports/tabela_YYYYMMDD_HHMMSS.csv
            filtros (str, optional): Condições SQL WHERE para filtrar dados
            adicionar_timestamp (bool): Se True, adiciona timestamp ao nome do arquivo
            
        Returns:
            str: Caminho do arquivo CSV gerado
            
        Raises:
            ExportError: Em caso de falha na exportação
        """
        # Normalizar nome da tabela (remover caracteres especiais)
        tabela_limpa = self._limpar_nome_tabela(tabela)
        
        # Definir caminho do arquivo se não especificado
        if not caminho_arquivo:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if adicionar_timestamp else ""
            nome_arquivo = f"{tabela_limpa}_{timestamp}.csv" if timestamp else f"{tabela_limpa}.csv"
            caminho_arquivo = self.export_dir / nome_arquivo
        
        log.info(f"Exportando tabela {tabela} para CSV: {caminho_arquivo}")
        
        try:
            # Construir consulta SQL
            query = f"SELECT * FROM {tabela}"
            if filtros:
                query += f" WHERE {filtros}"
                
            # Executar consulta via DatabaseManager
            df = self.db_manager.execute_query(query)
            
            if df is None or df.empty:
                log.warning(f"Nenhum dado encontrado na tabela {tabela} com os filtros especificados")
                return None
            
            # Garantir que o diretório existe
            os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
            
            # Exportar para CSV
            df.to_csv(caminho_arquivo, index=False)
            log.info(f"Exportação concluída: {len(df)} registros exportados para {caminho_arquivo}")
            
            return str(caminho_arquivo)
        except Exception as e:
            error_msg = f"Erro ao exportar para CSV: {str(e)}"
            log.error(error_msg)
            raise ExportError(error_msg, format="csv", file_path=caminho_arquivo, details=str(e))
    
    @with_error_handling(error_type=ExportError, retry_count=1)
    def export_to_excel(self, tabela, caminho_arquivo=None, filtros=None, adicionar_timestamp=True):
        """
        Exporta dados de uma tabela para Excel.
        
        Args:
            tabela (str): Nome da tabela a ser exportada
            caminho_arquivo (str, optional): Caminho onde salvar o arquivo Excel.
                                           Se None, usa exports/tabela_YYYYMMDD_HHMMSS.xlsx
            filtros (str, optional): Condições SQL WHERE para filtrar dados
            adicionar_timestamp (bool): Se True, adiciona timestamp ao nome do arquivo
            
        Returns:
            str: Caminho do arquivo Excel gerado
            
        Raises:
            ExportError: Em caso de falha na exportação
        """
        # Normalizar nome da tabela
        tabela_limpa = self._limpar_nome_tabela(tabela)
        
        # Definir caminho do arquivo se não especificado
        if not caminho_arquivo:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") if adicionar_timestamp else ""
            nome_arquivo = f"{tabela_limpa}_{timestamp}.xlsx" if timestamp else f"{tabela_limpa}.xlsx"
            caminho_arquivo = self.export_dir / nome_arquivo
        
        log.info(f"Exportando tabela {tabela} para Excel: {caminho_arquivo}")
        
        try:
            # Construir consulta SQL
            query = f"SELECT * FROM {tabela}"
            if filtros:
                query += f" WHERE {filtros}"
                
            # Executar consulta via DatabaseManager
            df = self.db_manager.execute_query(query)
            
            if df is None or df.empty:
                log.warning(f"Nenhum dado encontrado na tabela {tabela} com os filtros especificados")
                return None
            
            # Garantir que o diretório existe
            os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
            
            # Exportar para Excel
            df.to_excel(caminho_arquivo, index=False, sheet_name=tabela_limpa[:31])  # Excel limita nome da sheet a 31 caracteres
            log.info(f"Exportação concluída: {len(df)} registros exportados para {caminho_arquivo}")
            
            return str(caminho_arquivo)
        except Exception as e:
            error_msg = f"Erro ao exportar para Excel: {str(e)}"
            log.error(error_msg)
            raise ExportError(error_msg, format="excel", file_path=caminho_arquivo, details=str(e))
    
    @with_error_handling(error_type=ExportError, retry_count=1)
    def export_multiple_tables(self, tabelas, caminho_arquivo=None, formato="xlsx"):
        """
        Exporta várias tabelas para um único arquivo.
        
        Args:
            tabelas (list): Lista de nomes de tabelas ou dicionário {tabela: filtro}
            caminho_arquivo (str, optional): Caminho onde salvar o arquivo
            formato (str): Formato de exportação ('xlsx' ou 'csv')
            
        Returns:
            str: Caminho do arquivo gerado
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Definir caminho do arquivo se não especificado
        if not caminho_arquivo:
            nome_arquivo = f"multi_export_{timestamp}.{formato}"
            caminho_arquivo = self.export_dir / nome_arquivo
        
        log.info(f"Exportando múltiplas tabelas para {formato}: {caminho_arquivo}")
        
        # Verificar se tabelas é uma lista ou dicionário
        if isinstance(tabelas, list):
            # Converter para dicionário sem filtros
            tabelas_dict = {tabela: None for tabela in tabelas}
        else:
            tabelas_dict = tabelas
            
        try:
            # Para Excel, podemos criar um único arquivo com múltiplas planilhas
            if formato.lower() == "xlsx":
                with pd.ExcelWriter(caminho_arquivo) as writer:
                    for tabela, filtro in tabelas_dict.items():
                        query = f"SELECT * FROM {tabela}"
                        if filtro:
                            query += f" WHERE {filtro}"
                            
                        df = self.db_manager.execute_query(query)
                        
                        if df is not None and not df.empty:
                            # Excel limita nome da sheet a 31 caracteres
                            sheet_name = self._limpar_nome_tabela(tabela)[:31]
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            log.info(f"Tabela {tabela}: {len(df)} registros exportados")
                        else:
                            log.warning(f"Nenhum dado encontrado na tabela {tabela}")
                
                log.info(f"Exportação múltipla para Excel concluída: {caminho_arquivo}")
                return str(caminho_arquivo)
            
            # Para CSV, precisamos criar vários arquivos
            elif formato.lower() == "csv":
                # Criar diretório para os arquivos
                base_dir = Path(os.path.dirname(caminho_arquivo))
                export_dir = base_dir / f"export_{timestamp}"
                os.makedirs(export_dir, exist_ok=True)
                
                arquivos_exportados = []
                
                for tabela, filtro in tabelas_dict.items():
                    tabela_limpa = self._limpar_nome_tabela(tabela)
                    arquivo_csv = export_dir / f"{tabela_limpa}.csv"
                    
                    query = f"SELECT * FROM {tabela}"
                    if filtro:
                        query += f" WHERE {filtro}"
                        
                    df = self.db_manager.execute_query(query)
                    
                    if df is not None and not df.empty:
                        df.to_csv(arquivo_csv, index=False)
                        arquivos_exportados.append(str(arquivo_csv))
                        log.info(f"Tabela {tabela}: {len(df)} registros exportados para {arquivo_csv}")
                    else:
                        log.warning(f"Nenhum dado encontrado na tabela {tabela}")
                
                # Criar arquivo de índice
                index_file = export_dir / "index.json"
                with open(index_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "exportacao": timestamp,
                        "tabelas": list(tabelas_dict.keys()),
                        "arquivos": [os.path.basename(a) for a in arquivos_exportados]
                    }, f, indent=2)
                
                log.info(f"Exportação múltipla para CSV concluída: {len(arquivos_exportados)} arquivos em {export_dir}")
                return str(export_dir)
            
            else:
                raise ValueError(f"Formato não suportado: {formato}")
        
        except Exception as e:
            error_msg = f"Erro ao exportar múltiplas tabelas: {str(e)}"
            log.error(error_msg)
            raise ExportError(error_msg, format=formato, file_path=caminho_arquivo, details=str(e))
    
    @with_error_handling(error_type=ExportError)
    def export_data_with_timeframe(self, simbolo, timeframe, formato="csv", caminho_arquivo=None, filtros=None):
        """
        Exporta dados de um símbolo e timeframe específicos.
        
        Args:
            simbolo (str): Símbolo a ser exportado
            timeframe (str): Timeframe dos dados (1m, 5m, 15m, etc.)
            formato (str): Formato de exportação ('csv' ou 'xlsx')
            caminho_arquivo (str, optional): Caminho onde salvar o arquivo
            filtros (str, optional): Condições SQL WHERE para filtrar dados
            
        Returns:
            str: Caminho do arquivo gerado
        """
        # Converter símbolo e timeframe para nome de tabela
        tabela = self._obter_nome_tabela(simbolo, timeframe)
        
        if formato.lower() == "csv":
            return self.export_to_csv(tabela, caminho_arquivo, filtros)
        elif formato.lower() in ["xlsx", "excel"]:
            return self.export_to_excel(tabela, caminho_arquivo, filtros)
        else:
            raise ValueError(f"Formato não suportado: {formato}")
    
    def _limpar_nome_tabela(self, tabela):
        """
        Remove caracteres especiais do nome da tabela para uso em nomes de arquivo.
        
        Args:
            tabela (str): Nome da tabela
            
        Returns:
            str: Nome limpo para uso em nomes de arquivo
        """
        # Substituir caracteres especiais por underscore
        tabela_limpa = re.sub(r'[^\w\d]', '_', tabela)
        return tabela_limpa
    
    def _obter_nome_tabela(self, simbolo, timeframe):
        """
        Converte símbolo e timeframe para o nome da tabela no banco de dados.
        
        Args:
            simbolo (str): Símbolo (ex: "PETR4", "WIN$N")
            timeframe (str): Timeframe (ex: "1m", "5m", "1h", "D1")
            
        Returns:
            str: Nome da tabela correspondente
        """
        # Remover $ e outros caracteres especiais do símbolo
        simbolo_limpo = re.sub(r'[^\w\d]', '_', simbolo).lower()
        
        # Converter timeframe para formato de nome de tabela
        timeframe_map = {
            "1m": "1_minuto",
            "5m": "5_minutos",
            "15m": "15_minutos",
            "30m": "30_minutos",
            "1h": "1_hora",
            "4h": "4_horas",
            "D1": "diario",
            "W1": "semanal",
            "MN1": "mensal"
        }
        
        # Obter o sufixo do timeframe, usar o original se não mapeado
        timeframe_sufixo = timeframe_map.get(timeframe, timeframe)
        
        # Construir nome da tabela no formato simbolo_timeframe
        return f"{simbolo_limpo}_{timeframe_sufixo}" 