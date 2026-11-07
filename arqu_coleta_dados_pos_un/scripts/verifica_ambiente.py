import os, shutil, sys
import boto3
from dotenv import load_dotenv
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent      # pasta do projeto
load_dotenv(RAIZ / "docker" / ".env")
ok = True

def linha(nome, cond, dica=""):
    global ok
    ok = ok and bool(cond)
    print("OK   " if cond else "FALTA", nome, "" if cond else f"-> {dica}")

linha("Java", shutil.which("java") or os.environ.get("JAVA_HOME"),
      "instale o JDK 17 e confira o JAVA_HOME")

try:
    import pyspark
    linha(f"PySpark {pyspark.__version__}", True)
except ImportError:
    linha("PySpark", False, "pip install -r requirements.txt")

try:
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=os.environ["MINIO_USER"],
        aws_secret_access_key=os.environ["MINIO_PASSWORD"],
        region_name="us-east-1",
    )
    nomes = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    linha("MinIO (buckets landing e warehouse)",
          {"landing", "warehouse"} <= set(nomes),
          "rode: python scripts/criar_buckets.py")
except Exception as e:
    linha("MinIO", False, f"cd docker && docker compose up -d ({type(e).__name__})")

try:
    import tempfile
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
     
    spark = (SparkSession.builder.master("local[2]").appName("teste")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    destino = os.path.join(tempfile.gettempdir(), "teste_parquet")
    df.write.mode("overwrite").parquet(destino)          # no Windows, quebra aqui sem o winutils
    n = spark.read.parquet(destino).count()
    spark.stop()
    linha(f"Spark {pyspark.__version__} (grava e lê Parquet)", n == 2,
          "veja a tabela de erros do Spark")
except Exception as e:
    causa = [l for l in str(e).splitlines() if l.strip()][:3]
    linha("Spark", False, " | ".join(causa))

print("\nPostgres + pgvector e Metabase: vamos montar juntos nas próximas etapas.")
print("Tudo pronto." if ok else "Resolva os itens FALTA e rode de novo.")
sys.exit(0 if ok else 1)
