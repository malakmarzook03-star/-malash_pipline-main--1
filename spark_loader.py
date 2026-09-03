import sys
import os
import time
from datetime import datetime

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.path.exists("C:\\hadoop"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, row_number, lit, col
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import MONGO_URI, DB_NAME, COLLECTION_RAW
from src.monitor import SystemMonitor


def get_spark_session():
    jvm_flags = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )

    return SparkSession.builder \
        .appName("HybridDataPipeline_SparkEngine") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.extraJavaOptions", jvm_flags) \
        .config("spark.executor.extraJavaOptions", jvm_flags) \
        .config("spark.python.worker.reuse", "true") \
        .getOrCreate()


def run_spark_loader(file_path, run_id):
    monitor = SystemMonitor(interval=4)
    monitor.start()

    spark = get_spark_session()
    start_time = time.time()
    file_name = os.path.basename(file_path)
    current_time_str = datetime.utcnow().isoformat()

    columns = [
        "order_id", "order_date", "status", "customer_id", "customer_name",
        "customer_phone", "customer_email", "city", "district", "delivery_type",
        "delivery_cost", "payment_method", "payment_status", "payment_amount",
        "currency", "total_amount", "items_json"
    ]

    schema = StructType([
        StructField(c, StringType(), True)
        for c in columns
    ])

    try:
        print("\n╔══════════════════════════════════════════════╗")
        print("║              ⚡ SPARK ENGINE                 ║")
        print("╚══════════════════════════════════════════════╝")
        print(f"  File    : {file_name}")
        print(f"  Run ID  : {run_id}")
        print("  Status  : Reading dataset...")

        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .schema(schema) \
            .load(file_path)

        # 1. حساب عدد السجلات لكل Partition
        df_with_pid = df.withColumn("_pid", spark_partition_id())
        part_counts = df_with_pid.groupBy("_pid").count().orderBy("_pid").collect()

        # 2. بناء خريطة الإزاحة التراكمية
        offsets = []
        cum = 0

        for row in part_counts:
            offsets.append((int(row["_pid"]), int(cum)))
            cum += int(row["count"])

        offsets_df = spark.createDataFrame(
            offsets,
            ["_pid", "_offset"]
        )

        w = Window.partitionBy("_pid").orderBy(lit(1))

        # 3. توليد source_row_number
        df_indexed = (
            df_with_pid.join(offsets_df, on="_pid")
            .withColumn(
                "source_row_number",
                (col("_offset") + row_number().over(w)).cast(IntegerType())
            )
            .drop("_pid", "_offset")
        )

        input_partitions = df_indexed.rdd.getNumPartitions()

        print(f"  ✓ Partitions : {input_partitions}")
        print(f"  ✓ Records    : {cum:,}")
        print("  → Preparing MongoDB ingestion...")

        def insert_partition_to_raw(partition_iter):
            from pymongo import MongoClient
            import time

            client = MongoClient(
                MONGO_URI,
                connectTimeoutMS=60000,
                socketTimeoutMS=120000,
                serverSelectionTimeoutMS=60000
            )

            raw_col = client[DB_NAME][COLLECTION_RAW]
            batch = []
            count = 0

            def flush_batch(b):
                if not b:
                    return

                for attempt in range(5):
                    try:
                        raw_col.insert_many(b, ordered=False)
                        break
                    except Exception:
                        if attempt == 4:
                            raise
                        time.sleep(2)

            try:
                for row in partition_iter:
                    row_dict = row.asDict()
                    row_num = row_dict.pop("source_row_number", None)

                    clean_dict = {
                        str(k).replace('\ufeff', '').strip():
                            (str(v) if v is not None else "")
                        for k, v in row_dict.items()
                        if k
                    }

                    batch.append({
                        "run_id": run_id,
                        "source_file": file_name,
                        "source_row_number": int(row_num)
                            if row_num is not None else None,
                        "ingested_at": current_time_str,
                        "engine_used": "pyspark",
                        "raw_record": clean_dict
                    })

                    count += 1

                    if len(batch) >= 2000:
                        flush_batch(batch)
                        batch = []

                if batch:
                    flush_batch(batch)

            finally:
                client.close()

            yield count

        print("  → Ingesting partitions into orders_raw...")

        counts = (
            df_indexed.rdd
            .mapPartitions(insert_partition_to_raw)
            .collect()
        )

        total_rows = sum(counts)

        print(f"  ✓ MongoDB load complete: {total_rows:,} records")

    finally:
        monitor.stop()
        spark.stop()

    total_time = time.time() - start_time
    avg_throughput = (
        total_rows / total_time
        if total_time > 0
        else 0
    )

    print("\n╭──────────────────────────────────────────────╮")
    print("│           📊 SPARK RUN COMPLETE              │")
    print("├──────────────────────────────────────────────┤")
    print(f"│ Records    : {total_rows:,}")
    print(f"│ Partitions : {input_partitions}")
    print(f"│ Time       : {total_time:.2f}s")
    print(f"│ Speed      : {avg_throughput:.1f} rows/s")
    print("╰──────────────────────────────────────────────╯\n")

    return {
        "engine": "pyspark",
        "loaded_raw": total_rows,
        "seconds_elapsed": total_time,
        "throughput": avg_throughput,
        "partitions": input_partitions
    }