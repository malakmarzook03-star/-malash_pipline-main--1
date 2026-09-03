import sys
import json
import os

sys.path.append(
    os.path.dirname(
        os.path.dirname(
            os.path.abspath(__file__)
        )
    )
)

from src.quality_rules import validate_and_clean_record


def test_valid_record():
    raw = {
        "order_id": "ORD-1001",
        "customer_id": "CUST-500",
        "order_date": "2025-01-31",
        "items_json": '[{"item_id": 1, "price": 100}]',
        "total_amount": "100"
    }

    res = validate_and_clean_record(raw)

    assert res["status"] == "valid"


def test_corrected_record():
    raw = {
        "order_id": "ORD-1002",
        "customer_id": "CUST-501",
        "order_date": "31/01/2025",
        "customer_email": "user@@mail..com",
        "items_json": '[{"item_id": 1, "price": 100}]',
        "total_amount": "٥٠٠٠"
    }

    res = validate_and_clean_record(raw)

    assert res["status"] == "corrected"
    assert res["data"]["customer_email"] == "user@mail.com"
    assert res["data"]["total_amount"] == "5000"


def test_quarantine_record():
    raw = {
        "order_id": "",
        "customer_id": "CUST-502",
        "order_date": "invalid-date",
        "items_json": "[]"
    }

    res = validate_and_clean_record(raw)

    assert res["status"] == "quarantined"
    assert "MISSING_ORDER_ID" in res["data"]["error_codes"]


def save_metrics(metrics_data, output_path="reports/results.json"):
    os.makedirs(
        os.path.dirname(output_path),
        exist_ok=True
    )

    history = []

    # قراءة النتائج السابقة
    if os.path.exists(output_path):
        try:
            with open(
                output_path,
                "r",
                encoding="utf-8"
            ) as f:

                content = json.load(f)

                if isinstance(content, list):
                    history = content

                elif isinstance(content, dict):
                    history = [content]

        except Exception:
            history = []

    # إضافة نتائج التشغيل الحالي
    history.append(metrics_data)

    # حفظ النتائج
    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            history,
            f,
            ensure_ascii=False,
            indent=4
        )

    print("\n╭──────────────────────────────────────────────╮")
    print("│              📊 METRICS REPORT              │")
    print("╰──────────────────────────────────────────────╯")
    print()
    print("  ✓ Results saved successfully")
    print(f"  ▸ Output file : {output_path}")
    print(f"  ▸ Runs stored : {len(history)}")
    print()


if __name__ == "__main__":

    print("\n╔══════════════════════════════════════════════╗")
    print("║             🧪 UNIT TEST SUITE               ║")
    print("╚══════════════════════════════════════════════╝")
    print()

    test_valid_record()
    print("  ✓ Valid record test passed")

    test_corrected_record()
    print("  ✓ Corrected record test passed")

    test_quarantine_record()
    print("  ✓ Quarantine record test passed")

    print()
    print("  ────────────────────────────────────────────")
    print("  🎯 ALL UNIT TESTS PASSED")
    print("  ────────────────────────────────────────────")
    print()