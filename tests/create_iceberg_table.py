#!/usr/bin/env python3
"""
Script de teste Apache Iceberg adaptado para HDFS + YARN
Baseado no exemplo oficial: https://iceberg.apache.org/spark-quickstart/#docker-compose
Ajustado para usar catálogo Hadoop no HDFS sem Hive
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType, StructField, StringType

def main():
    # Criar SparkSession com configurações explícitas para Iceberg
    spark = SparkSession.builder \
        .appName("IcebergQuickstartHDFS") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_prod.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://masternode:9000/lakehouse") \
        .getOrCreate()
    
    try:
        print("=== INICIANDO TESTE ICEBERG ===")
        
        # Verificar catálogos disponíveis
        print("\n1. Catálogos disponíveis:")
        spark.sql("SHOW CATALOGS").show()
        
        # Criar namespace/database se não existir
        print("\n2. Criando namespace 'demo'...")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_prod.demo")
        
        # Verificar namespaces
        print("\n3. Namespaces disponíveis:")
        spark.sql("SHOW NAMESPACES IN hadoop_prod").show()
        
        # Definir schema da tabela
        schema = StructType([
            StructField("vendor_id", LongType(), True),
            StructField("trip_id", LongType(), True),
            StructField("trip_distance", FloatType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True)
        ])
        
        # Criar DataFrame vazio e criar a tabela Iceberg
        print("\n4. Criando tabela Iceberg 'hadoop_prod.demo.nyc_taxis'...")
        df_empty = spark.createDataFrame([], schema)
        
        # Remover tabela se já existir (para teste)
        try:
            spark.sql("DROP TABLE IF EXISTS hadoop_prod.demo.nyc_taxis")
            print("   Tabela existente removida.")
        except Exception as e:
            print(f"   Tabela não existia: {e}")
        
        # Criar nova tabela
        df_empty.writeTo("hadoop_prod.demo.nyc_taxis").create()
        print("   Tabela criada com sucesso!")
        
        # Verificar estrutura da tabela
        print("\n5. Schema da tabela criada:")
        spark.table("hadoop_prod.demo.nyc_taxis").printSchema()
        
        # Preparar dados de exemplo
        print("\n6. Inserindo dados na tabela...")
        schema_table = spark.table("hadoop_prod.demo.nyc_taxis").schema
        data = [
            (1, 1000371, 1.8, 15.32, "N"),
            (2, 1000372, 2.5, 22.15, "N"),
            (2, 1000373, 0.9, 9.01, "N"),
            (1, 1000374, 8.4, 42.13, "Y")
        ]
        
        df_data = spark.createDataFrame(data, schema_table)
        df_data.writeTo("hadoop_prod.demo.nyc_taxis").append()
        print("   Dados inseridos com sucesso!")
        
        # Ler e mostrar dados
        print("\n7. Dados na tabela:")
        df_result = spark.table("hadoop_prod.demo.nyc_taxis")
        df_result.show()
        
        # Informações da tabela
        print("\n8. Informações da tabela:")
        print(f"   Total de registros: {df_result.count()}")
        
        # Teste de query
        print("\n9. Teste de query (trips com distância > 2.0):")
        spark.sql("""
            SELECT vendor_id, trip_id, trip_distance, fare_amount 
            FROM hadoop_prod.demo.nyc_taxis 
            WHERE trip_distance > 2.0
        """).show()
        
        # Mostrar tabelas no namespace
        print("\n10. Tabelas no namespace 'demo':")
        spark.sql("SHOW TABLES IN hadoop_prod.demo").show()
        
        print("\n=== TESTE CONCLUÍDO COM SUCESSO! ===")
        
    except Exception as e:
        print(f"\n❌ ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Fechar SparkSession
        spark.stop()
        print("\nSparkSession encerrada.")

if __name__ == "__main__":
    main()



# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("TesteIceberg").getOrCreate()
# print("Catálogos disponíveis:")
# spark.sql("SHOW CATALOGS").show()
# spark.stop()