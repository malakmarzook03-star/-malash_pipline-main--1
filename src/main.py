import sys
import os
import uuid
import time
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.mongo_setup import init_mongo
from src.file_router import route_file
from src.batch_loader import run_batch_loader
from src.spark_loader import run_spark_loader
from src.elt_pipeline import process_elt_transformation
from src.metrics import save_metrics


def main():
    parser = argparse.ArgumentParser(
        description="Hybrid Big Data Pipeline CLI (Midterm Project)"
    )

    parser.add_argument(
        "--file",
        required=True,
        help="Path to input CSV dataset"
    )

    args = parser.parse_args()

    print("\n")
    print("╔══════════════════════════════════════════════╗")
    print("║          🚀 HYBRID DATA PIPELINE             ║")
    print("╚══════════════════════════════════════════════╝")
    print()
    print(f"  ▸ Input File : {os.path.basename(args.file)}")
    print("  ▸ Initializing pipeline...")


    init_mongo()

    print("  ✓ MongoDB initialized")


    engine, file_size_mb = route_file(args.file)

    run_id = str(uuid.uuid4())
    total_start = time.time()

    print(f"  ✓ Engine selected : {engine}")
    print(f"  ✓ File size       : {file_size_mb:.2f} MB")
    print(f"  ✓ Run ID          : {run_id}")

    print("\n  ────────────────────────────────────────────")
    print("  📥 STARTING DATA INGESTION")
    print("  ────────────────────────────────────────────")


    if engine == "python_batch":
        load_stats = run_batch_loader(
            args.file,
            run_id
        )
    else:
        load_stats = run_spark_loader(
            args.file,
            run_id
        )

    print("\n  ✓ Ingestion stage completed")


    print("\n  ────────────────────────────────────────────")
    print("  ⚡ STARTING ELT TRANSFORMATION")
    print("  ────────────────────────────────────────────")

    elt_stats = process_elt_transformation(run_id)


    total_duration = time.time() - total_start

    final_report = {
        "run_id": run_id,
        "file_name": os.path.basename(args.file),
        "file_size_mb": round(file_size_mb, 2),
        "engine_used": engine,
        "rows_read": load_stats.get("loaded_raw", 0),
        "raw_loaded": load_stats.get("loaded_raw", 0),
        "valid_count": elt_stats["count_valid"],
        "corrected_count": elt_stats["count_corrected"],
        "quarantine_count": elt_stats["count_quarantine"],
        "inserted_count": elt_stats["count_inserted"],
        "updated_count": elt_stats["count_updated"],
        "unchanged_count": elt_stats["count_unchanged"],
        "elapsed_seconds": round(total_duration, 2),
        "throughput": (
            round(
                load_stats.get("loaded_raw", 0)
                / total_duration,
                2
            )
            if total_duration > 0
            else 0
        ),
        "engine_details": {
            "batch_size": load_stats.get("batch_size"),
            "partitions": load_stats.get("partitions")
        },
        "error_case_counts": elt_stats["error_case_counts"],
        "consistency_check": elt_stats["consistency_check"]
    }

    save_metrics(final_report)

    print("\n")
    print("╔══════════════════════════════════════════════╗")
    print("║             🏁 PIPELINE COMPLETE             ║")
    print("╠══════════════════════════════════════════════╣")
    print(f"║ Engine       : {engine}")
    print(f"║ Rows Loaded  : {load_stats.get('loaded_raw', 0):,}")
    print(f"║ Valid        : {elt_stats['count_valid']:,}")
    print(f"║ Corrected    : {elt_stats['count_corrected']:,}")
    print(f"║ Quarantine   : {elt_stats['count_quarantine']:,}")
    print(f"║ Total Time   : {total_duration:.2f}s")
    print("╚══════════════════════════════════════════════╝")

    if elt_stats["consistency_check"]:
        print("\n  ✓ Consistency check: PASSED")
    else:
        print("\n  ✗ Consistency check: FAILED")

    print("\n  📊 Final metrics saved successfully.")
    print()

if __name__ == "__main__":
    main()