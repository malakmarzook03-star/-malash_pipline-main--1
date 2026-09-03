import os
import sys
import time

from pymongo import MongoClient, UpdateOne

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    MONGO_URI,
    DB_NAME,
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
    ELT_CHUNK_SIZE
)

from src.quality_rules import validate_and_clean_record


def process_elt_transformation(run_id, batch_chunk_size=ELT_CHUNK_SIZE):

    client = MongoClient(MONGO_URI, maxPoolSize=20)
    db = client[DB_NAME]

    raw_col = db[COLLECTION_RAW]
    valid_col = db[COLLECTION_VALIDATED]
    quarantine_col = db[COLLECTION_QUARANTINE]

    start_time = time.time()

    total_raw_in_run = raw_col.count_documents({
        "run_id": run_id
    })

    print("\n")
    print("╔════════════════════════════════════════════════════╗")
    print("║             ⚡ ELT TRANSFORMATION ENGINE           ║")
    print("╚════════════════════════════════════════════════════╝")

    print()
    print(f"  Run ID       : {run_id}")
    print(f"  Raw Records  : {total_raw_in_run:,}")
    print(f"  Chunk Size   : {batch_chunk_size:,}")

    if total_raw_in_run == 0:
        print()
        print("  ✗ No raw records found for this Run ID.")
        client.close()

        return {
            "run_id": run_id,
            "processed_raw": 0,
            "count_valid": 0,
            "count_corrected": 0,
            "count_quarantine": 0,
            "count_inserted": 0,
            "count_updated": 0,
            "count_unchanged": 0,
            "error_case_counts": {},
            "transformation_seconds": 0,
            "consistency_check": False
        }

    print()
    print("  ────────────────────────────────────────────────────")
    print("  ⏳ Starting transformation...")
    print("  ────────────────────────────────────────────────────")
    print()

    raw_cursor = raw_col.find(
        {"run_id": run_id},
        {
            "_id": 0,
            "raw_record": 1
        }
    ).batch_size(batch_chunk_size)

    valid_bulk_ops = []
    quarantine_docs = []

    count_valid = 0
    count_corrected = 0
    count_quarantine = 0

    total_inserted = 0
    total_updated = 0

    error_cases_count = {}
    processed = 0
    last_report = 0

    for doc in raw_cursor:

        raw_data = doc.get("raw_record", {})

        result = validate_and_clean_record(raw_data)

        status = result.get("status")
        data = result.get("data", {})

        if status in ["valid", "corrected"]:

            if status == "valid":
                count_valid += 1
            else:
                count_corrected += 1

            order_id = data.get("order_id")

            if order_id:
                valid_bulk_ops.append(
                    UpdateOne(
                        {"order_id": order_id},
                        {"$set": data},
                        upsert=True
                    )
                )

        else:

            count_quarantine += 1
            data["run_id"] = run_id
            quarantine_docs.append(data)

            for error_code in data.get("error_codes", []):
                error_cases_count[error_code] = (
                    error_cases_count.get(error_code, 0) + 1
                )

        processed += 1

        if len(valid_bulk_ops) >= batch_chunk_size:

            bulk_result = valid_col.bulk_write(
                valid_bulk_ops,
                ordered=False
            )

            total_inserted += bulk_result.upserted_count
            total_updated += bulk_result.modified_count

            valid_bulk_ops = []

        if len(quarantine_docs) >= batch_chunk_size:

            quarantine_col.insert_many(
                quarantine_docs,
                ordered=False
            )

            quarantine_docs = []

        if (
            processed - last_report >= 100_000
            or processed == total_raw_in_run
        ):

            elapsed = time.time() - start_time

            percentage = (
                processed / total_raw_in_run
            ) * 100

            speed = (
                processed / elapsed
                if elapsed > 0
                else 0
            )

            remaining = total_raw_in_run - processed

            eta = (
                remaining / speed
                if speed > 0
                else 0
            )

            print(
                f"  📦 Progress: "
                f"{processed:,} / "
                f"{total_raw_in_run:,} "
                f"({percentage:.2f}%)"
            )

            print(
                f"     Speed: {speed:,.0f} records/sec"
            )

            print(
                f"     ETA: {eta / 60:.1f} minutes"
            )

            print()

            last_report = processed

    if valid_bulk_ops:

        bulk_result = valid_col.bulk_write(
            valid_bulk_ops,
            ordered=False
        )

        total_inserted += bulk_result.upserted_count
        total_updated += bulk_result.modified_count

    if quarantine_docs:
        quarantine_col.insert_many(
            quarantine_docs,
            ordered=False
        )

    total_raw_processed = (
        count_valid
        + count_corrected
        + count_quarantine
    )

    total_successful_records = (
        count_valid
        + count_corrected
    )

    total_unchanged = (
        total_successful_records
        - total_inserted
        - total_updated
    )

    if total_unchanged < 0:
        total_unchanged = 0

    duration = time.time() - start_time

    consistency_passed = (
        total_raw_processed == total_raw_in_run
    )

    print("\n")
    print("╔════════════════════════════════════════════════════╗")
    print("║                 📊 ELT RUN SUMMARY                 ║")
    print("╠════════════════════════════════════════════════════╣")
    print(f"║ Total Processed : {total_raw_processed:,}")
    print(f"║ Valid           : {count_valid:,}")
    print(f"║ Corrected       : {count_corrected:,}")
    print(f"║ Quarantine      : {count_quarantine:,}")
    print(f"║ Inserted        : {total_inserted:,}")
    print(f"║ Updated         : {total_updated:,}")
    print(f"║ Unchanged       : {total_unchanged:,}")
    print(f"║ Execution Time  : {duration:.2f} sec")

    if duration > 0:
        final_speed = total_raw_processed / duration
        print(f"║ Throughput      : {final_speed:,.2f} rows/sec")

    print("╚════════════════════════════════════════════════════╝")

    print()
    print("  🔍 CONSISTENCY VALIDATION")
    print("  ────────────────────────────────────────────────────")

    if consistency_passed:
        print("  ✓ STATUS : PASSED")
        print("  ✓ All raw records were processed successfully.")
    else:
        print("  ✗ STATUS : FAILED")
        print("  ✗ Record count mismatch detected.")

    if error_cases_count:

        print()
        print("  ⚠ ERROR DISTRIBUTION")
        print("  ────────────────────────────────────────────────────")

        for error_code, error_count in sorted(
            error_cases_count.items(),
            key=lambda x: x[1],
            reverse=True
        ):
            print(
                f"     • {error_code:<30} : {error_count:,}"
            )

    print()
    print("╔════════════════════════════════════════════════════╗")
    print("║          🚀 ELT TRANSFORMATION COMPLETED          ║")
    print("╚════════════════════════════════════════════════════╝")

    print()
    print(f"  Run ID : {run_id}")
    print(f"  Records: {total_raw_processed:,}")
    print(f"  Time   : {duration:.2f} sec")
    print()

    client.close()

    return {
        "run_id": run_id,
        "processed_raw": total_raw_processed,
        "count_valid": count_valid,
        "count_corrected": count_corrected,
        "count_quarantine": count_quarantine,
        "count_inserted": total_inserted,
        "count_updated": total_updated,
        "count_unchanged": total_unchanged,
        "error_case_counts": error_cases_count,
        "transformation_seconds": duration,
        "consistency_check": consistency_passed
    }
