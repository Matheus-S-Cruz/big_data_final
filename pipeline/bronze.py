
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, ArrayType, IntegerType
)

from comum import (
    criar_sessao,
    caminho,
    ler_eventos,
    gravar_eventos,
    titulo,
)


# Schema explícito dos eventos, baseado no contrato do projeto.
SCHEMA_EVENTO = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("user_pseudo_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_hint", StringType(), True),

    StructField("page", StructType([
        StructField("location", StringType(), True),
        StructField("referrer", StringType(), True),
    ]), True),

    StructField("traffic", StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
    ]), True),

    StructField("device", StructType([
        StructField("category", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True),
    ]), True),

    StructField("params", StructType([
        StructField("page_title", StringType(), True),
        StructField("percent_scrolled", IntegerType(), True),
        StructField("link_url", StringType(), True),
        StructField("link_domain", StringType(), True),
        StructField("link_text", StringType(), True),
        StructField("link_id", StringType(), True),
        StructField("outbound", BooleanType(), True),
        StructField("search_term", StringType(), True),
        StructField("search_results", IntegerType(), True),
        StructField("search_area", StringType(), True),
        StructField("video_title", StringType(), True),
        StructField("video_provider", StringType(), True),
        StructField("video_duration", DoubleType(), True),
        StructField("video_current_time", DoubleType(), True),
        StructField("video_percent", DoubleType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_extension", StringType(), True),
        StructField("item_list_id", StringType(), True),
        StructField("item_list_name", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("transaction_id", StringType(), True),
        StructField("tax", DoubleType(), True),
        StructField("shipping", DoubleType(), True),
        StructField("shipping_tier", StringType(), True),
        StructField("payment_type", StringType(), True),
        StructField("coupon", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("lead_source", StringType(), True),
        StructField("method", StringType(), True),
        StructField("element_id", StringType(), True),
    ]), True),

    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])), True),
])


def main():
    titulo("CAMADA BRONZE")

    spark = criar_sessao("bronze")

    try:
        origem = caminho("landing")

        # Confere se a Landing existe antes de iniciar a leitura.
        fs_path = spark._jvm.org.apache.hadoop.fs.Path(origem)
        fs = fs_path.getFileSystem(spark._jsc.hadoopConfiguration())

        if not fs.exists(fs_path):
            raise SystemExit(
                f"Landing não encontrada em {origem}. "
                "Confira o ambiente e gere os eventos novamente."
            )

        # Lê os JSONL diretamente do S3 com schema explícito.
        df = ler_eventos(
            spark,
            "landing",
            formato="json",
            schema=SCHEMA_EVENTO
        )

        # Registra a origem do arquivo e o instante de ingestão.
        df = (
            df.withColumn("_arquivo_origem", F.input_file_name())
              .withColumn("_instante_ingestao", F.current_timestamp())
        )

        total = df.count()

        if total == 0:
            raise SystemExit(
                "Nenhum evento encontrado na Landing. "
                "Confira os arquivos de entrada."
            )

        # Mantém o histórico: a Bronze usa append.
        gravar_eventos(df, "bronze", "append")

        print(f"Eventos lidos: {total}")
        print(f"Destino: {caminho('bronze')}")
        print("Formato: Parquet")
        print("Modo de gravação: append")
        print("Partições: event_name/year/month/day")

        print("\nAmostra dos eventos gravados:")
        df.select(
            "event_id",
            "event_name",
            "event_ts",
            "_arquivo_origem",
            "_instante_ingestao",
            "year",
            "month",
            "day"
        ).show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
