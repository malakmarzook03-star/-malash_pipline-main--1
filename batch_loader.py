import sys
import os
import csv
import time
from datetime import datetime
from pymongo import MongoClient

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_RAW,
    BATCH_SIZE
)


def run_batch_loader(file_path, run_id):

    # ==========================================
    # DATABASE CONNECTION
    # ==========================================

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    raw_col = db[COLLECTION_RAW]

    file_name = os.path.basename(file_path)

    start_time = time.time()

    batch = []
    batch_index = 1
    total_loaded = 0

    # ==========================================
    # START MESSAGE
    # ==========================================

    print("\n")
    print("╭──────────────────────────────────────────────╮")
    print("│            📥 PYTHON BATCH LOADER            │")
    print("╰──────────────────────────────────────────────╯")

    print()
    print(f"  ◉ Source File : {file_name}")
    print(f"  ◉ Run ID      : {run_id}")
    print(f"  ◉ Batch Size  : {BATCH_SIZE}")

    print()
    print("  ────────────────────────────────────────────")
    print("  ⏳ Reading and loading records...")
    print("  ────────────────────────────────────────────")

    try:

        # ==========================================
        # OPEN CSV FILE
        # ==========================================

        with open(
            file_path,
            mode="r",
            encoding="utf-8-sig",
            errors="ignore"
        ) as infile:

            reader = csv.DictReader(infile)

            # ==========================================
            # READ ROWS
            # ==========================================

            for row_num, row in enumerate(reader, start=1):

                clean_row = {
                    str(k).replace("\ufeff", "").strip():
                    (str(v) if v is not None else "")
                    for k, v in row.items()
                    if k
                }

                # ======================================
                # RAW LAYER DOCUMENT
                # ======================================

                raw_document = {
                    "run_id": run_id,
                    "source_file": file_name,
                    "source_row_number": row_num,
                    "ingested_at": datetime.utcnow().isoformat(),
                    "engine_used": "python_batch",
                    "raw_record": clean_row
                }

                batch.append(raw_document)

                # ======================================
                # INSERT FULL BATCH
                # ======================================

                if len(batch) >= BATCH_SIZE:

                    b_start = time.time()

                    try:

                        raw_col.insert_many(
                            batch,
                            ordered=False
                        )

                        b_duration = time.time() - b_start

                        total_loaded += len(batch)

                        rate = (
                            len(batch) / b_duration
                            if b_duration > 0
                            else 0
                        )

                        # ==================================
                        # BATCH SUCCESS DISPLAY
                        # ==================================

                        print(
                            f"  ✓ Batch {batch_index:03d} "
                            f"| Records: {len(batch):<6} "
                            f"| Rate: {rate:,.1f} rec/s"
                        )

                    except Exception as e:

                        # ==================================
                        # BATCH ERROR DISPLAY
                        # ==================================

                        print(
                            f"  ✗ Batch {batch_index:03d} "
                            f"| FAILED"
                        )

                        print(
                            f"      Error: {e}"
                        )

                    batch = []
                    batch_index += 1

            # ==========================================
            # INSERT REMAINING RECORDS
            # ==========================================

            if batch:

                final_start = time.time()

                try:

                    raw_col.insert_many(
                        batch,
                        ordered=False
                    )

                    final_duration = time.time() - final_start

                    total_loaded += len(batch)

                    final_rate = (
                        len(batch) / final_duration
                        if final_duration > 0
                        else 0
                    )

                    print(
                        f"  ✓ Final Batch "
                        f"| Records: {len(batch):<6} "
                        f"| Rate: {final_rate:,.1f} rec/s"
                    )

                except Exception as e:

                    print("  ✗ Final Batch | FAILED")
                    print(f"      Error: {e}")

    finally:

        # ==========================================
        # CLOSE DATABASE CONNECTION
        # ==========================================

        client.close()

    # ==========================================
    # FINAL METRICS
    # ==========================================

    total_time = time.time() - start_time

    avg_throughput = (
        total_loaded / total_time
        if total_time > 0
        else 0
    )

    # ==========================================
    # FINAL SUMMARY
    # ==========================================

    print("\n")
    print("╭──────────────────────────────────────────────╮")
    print("│             📊 LOADING SUMMARY               │")
    print("╰──────────────────────────────────────────────╯")

    print()
    print("  ┌────────────────────────────────────────────┐")
    print("  │ BATCH LOADER RESULTS                       │")
    print("  ├────────────────────────────────────────────┤")
    print(f"  │ Total Loaded  : {total_loaded:<24}│")
    print(f"  │ Total Time    : {total_time:.2f}s{' ' * 20}│")
    print(f"  │ Throughput    : {avg_throughput:,.1f} rec/s{' ' * 13}│")
    print(f"  │ Batch Size    : {BATCH_SIZE:<24}│")
    print("  └────────────────────────────────────────────┘")

    print()
    print("  🚀 LOADING STATUS")
    print("  ────────────────────────────────────────────")

    if total_loaded > 0:
        print("     ✓ DATA LOAD COMPLETED")
    else:
        print("     ⚠ NO RECORDS WERE LOADED")

    print()
    print("╭──────────────────────────────────────────────╮")
    print("│        📥 BATCH LOADER FINISHED              │")
    print("╰──────────────────────────────────────────────╯")

    print(
        f"  File: {file_name} | "
        f"Rows: {total_loaded} | "
        f"Time: {total_time:.2f}s"
    )

    print()

    # ==========================================
    # RETURN RESULTS
    # ==========================================

    return {
        "engine": "python_batch",
        "loaded_raw": total_loaded,
        "seconds_elapsed": total_time,
        "throughput": avg_throughput,
        "batch_size": BATCH_SIZE
    }