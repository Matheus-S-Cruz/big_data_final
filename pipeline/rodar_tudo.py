"""Script de sequência (R3): reconstrói o datalake inteiro a partir da landing.

Roda os OITO jobs na ordem de dependência. NÃO gera dados (a landing já existe;
para gerá-la, use gerador/gerar_eventos.py).

    docker compose run --rm app python pipeline/rodar_tudo.py

Cada job roda como um processo próprio, com sua própria SparkSession, exatamente
como se fosse executado à mão. Se um job falhar, a sequência para e informa qual.
"""

import subprocess
import sys
import time

JOBS = [
    ("Bronze", "pipeline/bronze.py"),
    ("Silver", "pipeline/silver.py"),
    ("Gold funil", "pipeline/gold_funil.py"),
    ("Gold engajamento", "pipeline/gold_engajamento.py"),
    ("Gold receita e aquisição", "pipeline/gold_receita.py"),
    ("Gold ML", "pipeline/gold_ml.py"),
    ("Gold mapa de calor", "pipeline/gold_mapa_calor.py"),
    ("Exportação do mapa de calor (R11)", "pipeline/exportar_mapa_calor.py"),
]


def main():
    print("=" * 70)
    print("PIPELINE COMPLETO — reconstruindo o datalake a partir da landing")
    print("=" * 70)

    inicio_total = time.time()

    for i, (nome, script) in enumerate(JOBS, start=1):
        print(f"\n[{i}/{len(JOBS)}] {nome}  ->  python {script}")
        inicio = time.time()
        resultado = subprocess.run([sys.executable, script])
        dur = time.time() - inicio
        if resultado.returncode != 0:
            print(
                f"\n[FALHOU] {nome} (código {resultado.returncode}) "
                f"após {dur:.1f}s. A sequência foi interrompida."
            )
            raise SystemExit(resultado.returncode)
        print(f"[ok] {nome} concluído em {dur:.1f}s")

    total = time.time() - inicio_total
    print("\n" + "=" * 70)
    print(f"PIPELINE COMPLETO CONCLUÍDO em {total:.1f}s — {len(JOBS)} jobs")
    print("=" * 70)


if __name__ == "__main__":
    main()
