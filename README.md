Descrição
Este projeto utiliza o Apache Airflow para orquestrar um pipeline de dados que consulta uma API de mercado de ações, processa os dados, salva-os em um arquivo CSV e os armazena em um banco de dados MySQL. O projeto abrange desde a configuração do ambiente até a execução da pipeline.

Estrutura do Projeto
DAG (dag.py)
A DAG (Directed Acyclic Graph) principal do projeto é definida no arquivo dag.py. Esta DAG realiza as seguintes etapas:

1. Instalação de Dependências
Verifica a presença do pip.
Atualiza o pip.
Verifica a presença da biblioteca pandas.
Instala a biblioteca pandas.
2. Criação de Diretório
Cria um diretório temporário para armazenar os arquivos CSV.
3. Consulta à API de Mercado de Ações
Faz uma chamada à API brapi.dev para obter dados de ações com base em símbolos fornecidos em um arquivo CSV.
Processa os dados recebidos e os salva em um arquivo CSV.
4. Criação de Tabela no MySQL
Cria a tabela acoes_bolsa_brapi no banco de dados MySQL, caso não exista.
5. Importação de Dados para o MySQL
Lê os dados do arquivo CSV e os insere na tabela MySQL.
6. Fluxo de Execução
A DAG é agendada para rodar diariamente.
Configuração do Airflow
Conexão com o MySQL
No Airflow, a conexão com o MySQL é configurada com os seguintes parâmetros:

Conn Id: mysqlconn
Conn Type: MySQL
Host: 172.22.0.4 "altere conforme o ip do seu container mySql"
Schema: database_airflow
Login: root
Password: senha
Port: 3306 
Explicação das Tarefas
check_pip: Verifica se o pip está instalado.
update_pip: Atualiza o pip para a versão mais recente.
check_pandas: Verifica se a biblioteca pandas está instalada.
install_pandas: Instala a biblioteca pandas.
start_task: Marca o início do fluxo de tarefas.
cria_diretorio_task: Cria o diretório onde os arquivos CSV serão armazenados.
pega_task: Consulta a API brapi.dev para obter dados das ações e salva os resultados em um arquivo CSV.
salvar_task: Lê os dados do arquivo CSV e os salva novamente para garantir que estão no formato correto.
create_table_task: Cria a tabela MySQL se ela não existir.
import_csv_task: Importa os dados do CSV para a tabela MySQL.
end_task: Marca o fim do fluxo de tarefas.
