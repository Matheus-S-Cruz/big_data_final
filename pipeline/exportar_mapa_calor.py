"""Exportação do mapa de calor para o site (R11).

Lê as duas tabelas Gold do mapa de calor pelo Spark (R1) e grava, no fim, o
arquivo local site/mapa_calor.js com uma única linha:

    window.MAPA_CALOR = {"gerado_em": "...", "cliques": [...], "rolagem": [...]};

Os nomes de coluna de cada linha são EXATAMENTE os da seção 5.3; o site lê por
esses nomes. As duas tabelas têm poucas dezenas de linhas, então o collect() é
seguro aqui (nunca use collect em Bronze/Silver).
"""

import json
from datetime import datetime, timezone

from comum import (
    criar_sessao,
    caminho,
    existe,
    arquivo_do_projeto,
    titulo,
)


def main():
    titulo("EXPORTAR MAPA DE CALOR → site/mapa_calor.js")
    spark = criar_sessao("exportar_mapa_calor")

    try:
        for tabela in ("gold/mapa_calor_cliques", "gold/mapa_calor_rolagem"):
            if not existe(spark, caminho(tabela)):
                spark.stop()
                raise SystemExit(
                    f"{caminho(tabela)} não existe. "
                    "Rode antes: docker compose run --rm app python pipeline/gold_mapa_calor.py"
                )

        cliques = [
            linha.asDict()
            for linha in spark.read.parquet(
                caminho("gold/mapa_calor_cliques")
            ).collect()
        ]
        rolagem = [
            linha.asDict()
            for linha in spark.read.parquet(
                caminho("gold/mapa_calor_rolagem")
            ).collect()
        ]

        agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        dados = {"gerado_em": agora, "cliques": cliques, "rolagem": rolagem}

        destino = arquivo_do_projeto("site/mapa_calor.js")
        texto = json.dumps(dados, ensure_ascii=False, default=float)
        destino.write_text(
            "window.MAPA_CALOR = " + texto + ";\n", encoding="utf-8"
        )

        print(f"Linhas de cliques exportadas: {len(cliques)}")
        print(f"Linhas de rolagem exportadas: {len(rolagem)}")
        print(f"Arquivo gravado: {destino}")

        titulo("EXPORTAÇÃO CONCLUÍDA")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
