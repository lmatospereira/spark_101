import pymysql
import random
import time
import pymysql.cursors

from faker import Faker
from pyspark.sql import SparkSession

fake = Faker()

spark = SparkSession.builder.master(
    "local[1]"
    ).appName(
        'test'
    ).config(
        "spark.jars", "mariadb-java-client-1.8.0.jar"
    ).getOrCreate()


num_rows = 1000
user = 'root'
pwd = 'secret'
columns = ["id","firstname","middlename","lastname","gender","salary"]

connection_properties = {
    "host": '127.0.0.1',
    "user": user,
    "password": pwd,
    "database": "teste",
    "tabela": "test1",
    "colunas": columns
}

data = [
    {
        "id": _ ,
        "firstname": fake.first_name(),
        "middlename": fake.last_name(),
        "lastname": fake.last_name(),
        "gender": random.choice(["Male", "Female"]),
        "salary": round(random.uniform(1000.00, 10000.00), 2)
    } for _ in range(num_rows)
]

df = spark.createDataFrame(data)

def process_row(data):
    connection_properties = brConnect.value
    db_conn = pymysql.connect(
        host=connection_properties.get("host"),
        user=connection_properties.get("user"),
        password=connection_properties.get("password"),
        database=connection_properties.get("database"),
        port=3306
    )
    cursor = db_conn.cursor()
    tabela = connection_properties.get("tabela")
    columns = connection_properties.get("colunas")
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join(columns)

    query = f"""
    INSERT INTO {tabela} ({columns_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
    """ + ", ".join([f"{column}=VALUES({column})" for column in columns])
    
    cursor.executemany(query, data)
    db_conn.commit()
    cursor.close()

def process_partition_bacth(partition):    
    itemBank = [ tuple(row) for row in partition ]
    yield process_row(itemBank)

sc = spark.sparkContext
df = df.select(columns)
start = time.time()

rowsPerPartition = 10

partitions = (df.count() / rowsPerPartition)
df = df.repartition(numPartitions=int(partitions))
print("qtd_lines " + str(df.count()) + " partitions " + str(df.rdd.getNumPartitions()) + " df")

brConnect = sc.broadcast(connection_properties)
df.foreachPartition(process_partition_bacth)

end = time.time()

print(f'Total de tempo executado: {end-start/60} minutes')

spark.stop()