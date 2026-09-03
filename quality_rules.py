import re
import json
from datetime import datetime

# Rule 1: الأرقام العربية إلى لاتينية
def normalize_arabic_digits(val):
    if not val:
        return val
    arabic_digits = "٠١٢٣٤٥٦٧٨٩٫"
    latin_digits = "0123456789."
    trans = str.maketrans(arabic_digits, latin_digits)
    return str(val).translate(trans)

# Rule 2: فواصل الآلاف
def clean_thousand_separators(val):
    if not val:
        return val
    val = normalize_arabic_digits(val)
    return re.sub(r'(?<=\d),(?=\d)', '', str(val).strip())

# Rule 3: توحيد العملة
def standardize_currency(val):
    if not val:
        return "YER"
    val = str(val).strip()
    if val in ["لاير", "لاير يمني", "ريال", "ريال يمني", "YER", "YER "]:
        return "YER"
    return "YER"

# Rule 4: السعر بالكلمات
WORD_TO_NUM = {
    "ألف": 1000, "الف": 1000, "ألفان": 2000, "الفان": 2000, 
    "ألفين": 2000, "الفين": 2000, "ثلاثة آلاف": 3000, "ثلاثة الاف": 3000,
    "أربعة آلاف": 4000, "اربعة الاف": 4000, "خمسة آلاف": 5000, "خمسة الاف": 5000
}
def parse_price_words(val):
    if not val:
        return val
    val_clean = str(val).strip()
    return str(WORD_TO_NUM.get(val_clean, val))

# Rule 5: رقم الهاتف
def clean_phone(val):
    if not val:
        return val
    val = normalize_arabic_digits(val)
    return re.sub(r'[\s\-\(\)\+]', '', str(val))

# Rule 6: البريد الإلكتروني
def clean_email(val):
    if not val:
        return val
    val = str(val).strip()
    val = re.sub(r'@+', '@', val)
    return re.sub(r'\.+', '.', val)

# Rule 7: التاريخ
def standardize_date(val):
    if not val:
        return None
    val = normalize_arabic_digits(val).strip()
    val = val.replace("T", " ")
    val = re.sub(r'\s*/\s*', '/', val)
    val = re.sub(r'\s*-\s*', '-', val)
    
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d",
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(val, fmt)
            if 1990 <= parsed.year <= 2035:
                return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

# Rule 8: المسافات والمرادفات
def clean_string_status(val):
    if not val:
        return val
    return str(val).strip()

def validate_and_clean_record(raw_dict):
    clean_raw = {}
    for k, v in raw_dict.items():
        if k:
            k_clean = str(k).replace('\ufeff', '').strip().lower()
            clean_raw[k_clean] = v

    corrections = []
    quarantine_errors = []

    # 1. فحص order_id
    order_id = str(clean_raw.get("order_id", "") or "").strip()
    if not order_id or order_id.lower() in ["null", "none", "nan", ""]:
        quarantine_errors.append("MISSING_ORDER_ID")

    # 2. فحص customer_id
    customer_id = str(clean_raw.get("customer_id", "") or "").strip()
    if not customer_id or customer_id.lower() in ["null", "none", "nan", ""]:
        quarantine_errors.append("MISSING_CUSTOMER_ID")

    # 3. فحص التاريخ
    raw_date = str(clean_raw.get("order_date", "") or "").strip()
    norm_date = standardize_date(raw_date)
    if not norm_date:
        quarantine_errors.append("INVALID_IMPOSSIBLE_DATE")
    elif norm_date != raw_date:
        corrections.append({
            "field": "order_date",
            "original_value": raw_date,
            "corrected_value": norm_date,
            "rule_code": "DATE_STANDARDIZED"
        })

    # 4. فحص items_json
    items_raw = str(clean_raw.get("items_json", "") or "").strip()
    items_parsed = None
    if not items_raw or items_raw in ["[]", "{}", '""', "''"]:
        quarantine_errors.append("EMPTY_ITEMS")
    else:
        cleaned_json = items_raw
        if cleaned_json.startswith('"') and cleaned_json.endswith('"') and len(cleaned_json) > 1:
            cleaned_json = cleaned_json[1:-1]
        cleaned_json = cleaned_json.replace('""', '"').replace('\\"', '"').replace("'", '"').strip()
        if cleaned_json.startswith('[') and not cleaned_json.endswith(']'):
            cleaned_json += '}]' if cleaned_json.endswith('}') else ']'

        try:
            items_parsed = json.loads(cleaned_json)
            if not items_parsed:
                quarantine_errors.append("EMPTY_ITEMS")
            else:
                if cleaned_json != items_raw:
                    corrections.append({
                        "field": "items_json",
                        "original_value": items_raw,
                        "corrected_value": json.dumps(items_parsed, ensure_ascii=False),
                        "rule_code": "JSON_SYNTAX_REPAIRED"
                    })
        except Exception:
            quarantine_errors.append("CORRUPTED_ITEMS_JSON")

    # فحص القيم المالية والسالبة
    for field in ["delivery_cost", "payment_amount", "total_amount"]:
        orig = str(clean_raw.get(field, "") or "")
        parsed_words = parse_price_words(orig)
        cleaned_num_str = clean_thousand_separators(parsed_words)
        try:
            num_val = float(cleaned_num_str) if cleaned_num_str else 0.0
            if num_val < 0:
                quarantine_errors.append("AMBIGUOUS_NEGATIVE_VALUE")
        except ValueError:
            pass

    # إذا وجد خطأ عزل فادح -> يتم تحويل السجل مباشرة إلى Quarantine
    if quarantine_errors:
        if len(quarantine_errors) > 1:
            quarantine_errors.append("MULTIPLE_CONFLICTING_ERRORS")
        return {
            "status": "quarantined",
            "data": {
                "order_id": order_id if order_id else None,
                "error_codes": list(set(quarantine_errors)),
                "raw_record": raw_dict
            }
        }

    # إذا كان السجل سليماً أو قابلاً للتصحيح
    clean_data = dict(clean_raw)
    clean_data["order_id"] = order_id
    clean_data["customer_id"] = customer_id
    clean_data["order_date"] = norm_date
    if items_parsed:
        clean_data["items_json"] = json.dumps(items_parsed, ensure_ascii=False)

    # تطبيق بقية قواعد التصحيح مع الـ Audit Trail
    raw_email = str(clean_raw.get("customer_email", "") or "")
    c_email = clean_email(raw_email)
    if c_email != raw_email:
        corrections.append({
            "field": "customer_email",
            "original_value": raw_email,
            "corrected_value": c_email,
            "rule_code": "EMAIL_REPEATED_SYMBOLS"
        })
        clean_data["customer_email"] = c_email

    raw_phone = str(clean_raw.get("customer_phone", "") or "")
    c_phone = clean_phone(raw_phone)
    if c_phone != raw_phone:
        corrections.append({
            "field": "customer_phone",
            "original_value": raw_phone,
            "corrected_value": c_phone,
            "rule_code": "PHONE_NORMALIZED"
        })
        clean_data["customer_phone"] = c_phone

    raw_curr = str(clean_raw.get("currency", "") or "")
    c_curr = standardize_currency(raw_curr)
    if c_curr != raw_curr:
        corrections.append({
            "field": "currency",
            "original_value": raw_curr,
            "corrected_value": c_curr,
            "rule_code": "CURRENCY_STANDARDIZED"
        })
        clean_data["currency"] = c_curr

    for field in ["delivery_cost", "payment_amount", "total_amount"]:
        orig = str(clean_raw.get(field, "") or "")
        parsed_words = parse_price_words(orig)
        cleaned_num = clean_thousand_separators(parsed_words)
        if cleaned_num != orig:
            corrections.append({
                "field": field,
                "original_value": orig,
                "corrected_value": cleaned_num,
                "rule_code": "NUMBER_NORMALIZED"
            })
            clean_data[field] = cleaned_num

    for str_field in ["status", "payment_status", "delivery_type", "city", "district", "customer_name"]:
        orig = str(clean_raw.get(str_field, "") or "")
        cleaned_str = clean_string_status(orig)
        if cleaned_str != orig:
            corrections.append({
                "field": str_field,
                "original_value": orig,
                "corrected_value": cleaned_str,
                "rule_code": "STRING_TRIMMED"
            })
            clean_data[str_field] = cleaned_str

    quality_status = "corrected" if len(corrections) > 0 else "valid"
    clean_data["quality_status"] = quality_status
    clean_data["corrections"] = corrections

    return {
        "status": quality_status,
        "data": clean_data
    }