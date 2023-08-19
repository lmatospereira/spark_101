# PySpark to MariaDB Data Loading

Este é um tutorial simples sobre como usar o PySpark para ler dados de um DataFrame e gravá-los em uma tabela do banco de dados MariaDB.

## Requisitos

Certifique-se de ter o seguinte instalado em seu ambiente:

- [PySpark](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
- [pymysql](https://pypi.org/project/PyMySQL/)

## Uso

Siga os passos abaixo para utilizar o script PySpark:

1. Clone este repositório para o seu ambiente local.

2. Configure as informações de conexão do banco de dados MariaDB:
    - Abra o script `pyspark_to_mariadb.py` em um editor de texto.
    - Procure a seção "Definir informações de conexão ao banco de dados MariaDB".
    - Substitua os valores de `user`, `password`, `host` e `database` pelas informações do seu próprio banco de dados MariaDB.

3. Preparação dos Dados:
    - Certifique-se de que os dados que você deseja carregar estejam em um formato suportado pelo Spark, como CSV, Parquet, JSON, etc.
    - Atualize o valor da variável `input_data_path` no script `pyspark_to_mariadb.py` para o caminho dos seus dados.

4. Nome da Tabela de Destino:
    - Especifique o nome da tabela onde deseja gravar os dados no banco de dados MariaDB. Substitua o valor da variável `tabela_destino` no script `pyspark_to_mariadb.py`.

5. Execute o Script:
    - Abra um terminal e navegue até o diretório onde o script `pyspark_to_mariadb.py` está localizado.
    - Execute o seguinte comando para rodar o script:
      ```
      spark-submit pyspark_to_mariadb.py
      ```

6. Finalização:
    - Assim que o script terminar de executar, os dados serão carregados na tabela especificada do banco de dados MariaDB.

7. Encerrando a Sessão Spark:
    - Após a conclusão do processo, a sessão Spark será encerrada automaticamente.

## Notas

- Este tutorial é um exemplo básico para ilustrar o processo de leitura de dados com o PySpark e gravação no MariaDB. Adapte o script conforme necessário para atender às suas necessidades.

- Certifique-se de proteger suas informações de autenticação (usuário e senha do banco de dados) e não as compartilhe publicamente.

- Para consultas mais avançadas e ajustes de desempenho, consulte a [documentação oficial do PySpark](https://spark.apache.org/docs/latest/api/python/index.html).

- Consulte a [documentação do MariaDB](https://mariadb.com/kb/en/documentation/) para mais informações sobre o banco de dados.