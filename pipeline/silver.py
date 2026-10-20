
import sys
from pathlib import Path

# Adiciona a raiz do projeto ao caminho de importação do Python.
RAIZ_PROJETO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ_PROJETO))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.comum import (
    criar_sessao,
    caminho,
    exigir_camada,
    glob_eventos,
    ler_eventos,
    gravar_eventos,
    titulo,
)

# Catálogo de eventos definido no contrato do projeto.
CATALOGO_EVENTOS = [
    "page_view", "scroll", "click", "site_search",
    "video_start", "video_progress", "video_complete",
    "file_download", "view_item_list", "select_item",
    "view_item", "add_to_cart", "remove_from_cart",
    "view_cart", "begin_checkout", "add_shipping_info",
    "add_payment_info", "purchase", "refund",
    "generate_lead", "sign_up", "login",
]

spark = criar_sessao("silver")

try:
    exigir_camada(
        spark,
        glob_eventos("bronze"),
        "docker compose run --rm app python pipeline/bronze.py",
    )

    titulo("CAMADA SILVER")

    # 1. Leitura da Bronze e deduplicação por event_id.
    bronze = ler_eventos(spark, "bronze")

    total_bronze = bronze.count()

    janela_dedup = Window.partitionBy("event_id").orderBy(
        F.col("_instante_ingestao").asc(),
        F.col("_arquivo_origem").asc(),
    )

    eventos = (
        bronze
        .withColumn("_ordem", F.row_number().over(janela_dedup))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
    )

    total_deduplicado = eventos.count()
    print(f"Eventos na Bronze: {total_bronze}")
    print(f"Eventos após deduplicação: {total_deduplicado}")
    print(f"Duplicatas descartadas: {total_bronze - total_deduplicado}")

    # 2. Extração dos parâmetros para colunas próprias.
    # O schema da Bronze já tipa esses campos dentro de params.
    colunas_params = {
        "page_title": "string",
        "percent_scrolled": "int",
        "link_url": "string",
        "link_domain": "string",
        "link_text": "string",
        "link_id": "string",
        "outbound": "boolean",
        "search_term": "string",
        "search_results": "int",
        "search_area": "string",
        "video_title": "string",
        "video_provider": "string",
        "video_duration": "double",
        "video_current_time": "double",
        "video_percent": "double",
        "file_name": "string",
        "file_extension": "string",
        "item_list_id": "string",
        "item_list_name": "string",
        "currency": "string",
        "value": "double",
        "transaction_id": "string",
        "tax": "double",
        "shipping": "double",
        "shipping_tier": "string",
        "payment_type": "string",
        "coupon": "string",
        "reason": "string",
        "lead_source": "string",
        "method": "string",
        "element_id": "string",
    }

    for nome, tipo in colunas_params.items():
        eventos = eventos.withColumn(
            nome,
            F.col(f"params.{nome}").cast(tipo),
        )

    eventos = eventos.withColumn(
        "event_ts",
        F.try_to_timestamp(F.col("event_ts")),
    )

    
    # 3. Primeiras regras de validação.
    regras = [
        (
            "evento fora do catálogo",
            ~F.col("event_name").isin(CATALOGO_EVENTOS),
        ),
        (
            "timestamp ausente ou futuro",
            F.col("event_ts").isNull()
            | F.col("_instante_ingestao").isNull()
            | (
                F.col("event_ts")
                > F.col("_instante_ingestao").cast("timestamp")
            ),
        ),
        (
            "transação sem id",
            F.col("event_name").isin("purchase", "refund")
            & (
                F.col("transaction_id").isNull()
                | (F.trim(F.col("transaction_id")) == "")
            ),
        ),
        (
            "valor monetário inválido",
            F.col("value").isNotNull()
            & (
                (F.col("value") < 0)
                | F.col("currency").isNull()
                | (F.col("currency") != "BRL")
            ),
        ),
        (
            "marco fora do padrão",
            (
                F.col("percent_scrolled").isNotNull()
                & ~F.col("percent_scrolled").isin(25, 50, 75, 90)
            )
            | (
                F.col("video_percent").isNotNull()
                & ~F.col("video_percent").isin(0, 10, 25, 50, 75, 100)
            ),
        ),
    ]

    # Uma linha por evento e motivo, caso haja várias falhas.
    expressoes_motivos = [
        F.when(condicao, F.lit(motivo))
        for motivo, condicao in regras
    ]

    avaliados = eventos.withColumn(
        "motivos_rejeicao",
        F.filter(
            F.array(*expressoes_motivos),
            lambda x: x.isNotNull(),
        ),
    )

    # Compras que passam pelas regras anteriores são consideradas
    # válidas para verificar se um reembolso possui compra correspondente.
    compras_validas = (
        avaliados
        .filter(
            (F.col("event_name") == "purchase")
            & (F.size("motivos_rejeicao") == 0)
        )
        .select("transaction_id")
        .filter(F.col("transaction_id").isNotNull())
        .distinct()
        .withColumn("_tem_compra", F.lit(True))
    )

    avaliados = (
        avaliados
        .join(compras_validas, on="transaction_id", how="left")
        .withColumn(
            "motivos_rejeicao",
            F.when(
                (F.col("event_name") == "refund")
                & F.col("_tem_compra").isNull(),
                F.concat(
                    F.col("motivos_rejeicao"),
                    F.array(F.lit("reembolso sem compra")),
                ),
            ).otherwise(F.col("motivos_rejeicao")),
        )
        .drop("_tem_compra")
    )

    # Tabela de rejeitados: um registro por motivo.
    rejeitados = (
        avaliados
        .filter(F.size("motivos_rejeicao") > 0)
        .withColumn(
            "motivo_rejeicao",
            F.explode("motivos_rejeicao"),
        )
        .drop("motivos_rejeicao")
    )

    # Eventos aprovados para a Silver.
    validos = (
        avaliados
        .filter(F.size("motivos_rejeicao") == 0)
        .drop("motivos_rejeicao")
    )

    # 4. Sessionização: pausa superior a 30 minutos cria nova sessão.
    janela_usuario = Window.partitionBy("user_pseudo_id").orderBy(
        F.col("event_ts").asc(),
        F.col("event_id").asc(),
    )

    validos = validos.withColumn(
        "_evento_anterior",
        F.lag("event_ts").over(janela_usuario),
    )

    validos = validos.withColumn(
        "_nova_sessao",
        F.when(
            F.col("_evento_anterior").isNull()
            | (
                F.col("event_ts").cast("long")
                - F.col("_evento_anterior").cast("long")
                > 1800
            ),
            1,
        ).otherwise(0),
    )

    janela_acumulada = (
        Window.partitionBy("user_pseudo_id")
        .orderBy(
            F.col("event_ts").asc(),
            F.col("event_id").asc(),
        )
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    validos = (
        validos
        .withColumn(
            "_numero_sessao",
            F.sum("_nova_sessao").over(janela_acumulada),
        )
        .withColumn(
            "session_id",
            F.when(
                F.col("user_pseudo_id").isNotNull(),
                F.concat_ws(
                    "_",
                    F.col("user_pseudo_id"),
                    F.col("_numero_sessao").cast("string"),
                ),
            ),
        )
        .drop("_evento_anterior", "_nova_sessao", "_numero_sessao")
    )

    # 5. Identity stitching.
    # Busca o user_id em todos os eventos deduplicados, incluindo
    # eventos anteriores ao login e eventos que serão rejeitados.
    identidades = (
        eventos
        .filter(
            F.col("user_pseudo_id").isNotNull()
            & F.col("user_id").isNotNull()
        )
        .groupBy("user_pseudo_id")
        .agg(
            F.max("user_id").alias("_user_id_resolvido")
        )
    )

    # Propaga o user_id resolvido para todos os eventos aprovados
    # do mesmo navegador.
    validos = (
        validos
        .join(identidades, on="user_pseudo_id", how="left")
        .withColumn(
            "user_id",
            F.coalesce(
                F.col("user_id"),
                F.col("_user_id_resolvido"),
            ),
        )
        .drop("_user_id_resolvido")
    )

    # 6. Tabela separada de itens: uma linha por item por evento.
    itens = (
        validos
        .filter(F.col("items").isNotNull())
        .withColumn("item", F.explode("items"))
        .select(
            "event_id",
            "event_name",
            "event_ts",
            "user_pseudo_id",
            "user_id",
            "session_id",
            F.col("item.item_id").alias("item_id"),
            F.col("item.item_name").alias("item_name"),
            F.col("item.item_category").alias("item_category"),
            F.col("item.price").alias("price"),
            F.col("item.quantity").alias("quantity"),
            "year",
            "month",
            "day",
        )
    )

    # 7. Gravação das saídas.
    # overwrite dinâmico preserva as pastas vizinhas da Silver.
    gravar_eventos(validos, "silver", "overwrite")

    itens.write.mode("overwrite").partitionBy(
        "year", "month", "day"
    ).parquet(caminho("silver/itens"))

    rejeitados.write.mode("overwrite").parquet(
        caminho("silver/eventos_rejeitados")
    )

    # 8. Resumo de qualidade para conferência e relatório.
    total_validos = validos.count()
    total_rejeitados = rejeitados.count()
    total_itens = itens.count()

    print(f"Eventos aprovados: {total_validos}")
    print(f"Registros de rejeição (evento x motivo): {total_rejeitados}")
    print(f"Linhas na tabela de itens: {total_itens}")

    print("\nRejeições por motivo:")
    rejeitados.groupBy("motivo_rejeicao").count().orderBy(
        F.desc("count")
    ).show(30, False)

    print("\nEventos aprovados por tipo:")
    validos.groupBy("event_name").count().orderBy(
        "event_name"
    ).show(30, False)

    titulo("SILVER CONCLUÍDA")

finally:
    spark.stop()