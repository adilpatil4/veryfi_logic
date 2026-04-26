#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
veryfi_capture.py
- Process receipts with Veryfi v8 API.
- Normalize line items (merge qty/price detail lines, drop headers).
- Extract item numbers (7–13 digits; DPCI when 9 digits) from the **image itself**
  and align to items by price + fuzzy description (retailer-agnostic; Target works great).

Env vars required (Veryfi portal -> Settings -> Keys):
  VERYFI_CLIENT_ID
  VERYFI_USERNAME
  VERYFI_API_KEY
  VERYFI_CLIENT_SECRET

Examples:
  python veryfi_capture.py --file /path/to/target_receipt.png --save-json --save-normalized
  python veryfi_capture.py --url "https://example.com/receipt.jpg" --save-json
"""

import os, sys, json, base64, argparse, pathlib, re, datetime, calendar, hashlib, hmac
import sqlite3, hashlib, time
from pathlib import Path
from typing import Any, Dict, List, Optional
from target_bridge import lookup_target_by_dpci, normalize_dpci
from product_name_formatting import format_product_name

import requests

VERYFI_URL = "https://api.veryfi.com/api/v8/partner/documents"  # v8 process endpoint

# =========================== Utilities ===========================

def env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"[config] Missing environment variable: {name}", file=sys.stderr)
        sys.exit(2)
    return v

def now_ms() -> int:
    # timezone-aware to avoid deprecation warnings
    dt = datetime.datetime.now(datetime.timezone.utc)
    return int(dt.timestamp() * 1000)

def create_signature(secret: str, payload: dict, ts_ms: int) -> str:
    """
    Per Veryfi docs: build 'timestamp:<ms>,key1:value1,key2:value2,...' string
    (keys sorted), HMAC-SHA256 with CLIENT_SECRET, then Base64 encode.
    """
    parts = [f"timestamp:{ts_ms}"]
    for k in sorted(payload.keys()):
        parts.append(f"{k}:{payload[k]}")
    payload_str = ",".join(parts)
    digest = hmac.new(secret.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8").strip()

def read_file_b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

# =========================== Veryfi I/O ==========================

def post_to_veryfi(payload: dict) -> dict:
    client_id = env("VERYFI_CLIENT_ID")
    username = env("VERYFI_USERNAME")
    api_key = env("VERYFI_API_KEY")
    client_secret = env("VERYFI_CLIENT_SECRET")

    ts = now_ms()
    signature = create_signature(client_secret, payload, ts)

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "CLIENT-ID": client_id,
        "AUTHORIZATION": f"apikey {username}:{api_key}",
        "X-VERYFI-REQUEST-TIMESTAMP": str(ts),
        "X-VERYFI-REQUEST-SIGNATURE": signature,
    }

    r = requests.post(VERYFI_URL, headers=headers, data=json.dumps(payload), timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        print("\n[Veryfi error] Status:", r.status_code, file=sys.stderr)
        try:
            print(r.json(), file=sys.stderr)
        except Exception:
            print(r.text[:1000], file=sys.stderr)
        raise e
    return r.json()

def process_local_file(path: str, extra_params: dict = None) -> dict:
    file_name = pathlib.Path(path).name
    payload = {"file_name": file_name, "file_data": read_file_b64(path)}
    if extra_params: payload.update(extra_params)
    return post_to_veryfi(payload)

def process_file_url(url: str, extra_params: dict = None) -> dict:
    payload = {"file_url": url}
    if extra_params: payload.update(extra_params)
    return post_to_veryfi(payload)

# ====================== Normalization layer ======================

CURRENCY = r"\$?\d{1,3}(?:,\d{3})*\.\d{2}"

# lines that are definitely not items (boilerplate/payments/survey bits)
BOILERPLATE = (
    "SUBTOTAL","TOTAL","TAX","CHANGE","CASH","VISA","MASTERCARD","AMEX","DISCOVER",
    "CARD","DEBIT","CREDIT","BALANCE","PAYMENT","AMOUNT","ITEMS SOLD","THANK YOU",
    "AID","AUTH","TC#","ORDER","REG#","GST","PST","SURVEY","USER ID","PASSWORD",
    "CUENTENOS","ESPAÑOL","ESPANOL","PLEASE TAKE THIS SURVEY","AUTH CODE","US DEBIT"
)

# section headers that we want to treat as detail, not items
SECTION_HEADERS = (
    "GROCERY","KITCHEN","PRODUCE","BAKERY","DELI","MEAT","SEAFOOD",
    "PHARMACY","HOUSEHOLD","FROZEN","PANTRY","ELECTRONICS","HOME"
)
SECTION_RE = re.compile(
    r"^\s*[*\u2022\-]?\s*(%s)\s*$" % "|".join(map(re.escape, SECTION_HEADERS)),
    flags=re.I
)

# Numeric IDs we care about inside descriptions (fallback)
SKU_RE      = re.compile(r"(?<!\d)(\d{7,13})(?!\d)")          # 7–13 digit retailer/UPC-ish
DPCI_HYP_RE = re.compile(r"\b(\d{3}-\d{2}-\d{4})\b")          # already formatted Target DPCI
QTY_AT_RE   = re.compile(rf"(?P<q>\d+(?:\.\d+)?)\s*@\s*(?P<u>{CURRENCY})", flags=re.I)

def _to_float(x: Any) -> Optional[float]:
    if x is None: return None
    try: return float(x)
    except Exception: return None

def _is_boilerplate(s: str) -> bool:
    up = (s or "").upper()
    return any(b in up for b in BOILERPLATE)

def _is_section_header(s: str) -> bool:
    return bool(SECTION_RE.match(s or ""))

def _looks_like_qty_at(desc: str) -> bool:
    return bool(QTY_AT_RE.search(desc or ""))

def _is_price_only_desc(desc: str) -> bool:
    s = (desc or "").strip()
    return bool(re.fullmatch(rf"{CURRENCY}", s))

def _merge_detail(prev: Dict[str,Any], detail: Dict[str,Any], note: str):
    prev.setdefault("raw_detail", [])
    prev["raw_detail"].append({**detail, "_merged_as": note})

def _format_dpci_from_9digits(num: str) -> Optional[str]:
    d = re.sub(r"\D+", "", num or "")
    if len(d) == 9:
        return f"{d[:3]}-{d[3:5]}-{d[5:]}"
    return None

def _extract_ids_from_text(text: Optional[str]) -> Dict[str, Optional[str]]:
    """Try to pull SKU & DPCI from any given text."""
    s = text or ""
    m_dpci = DPCI_HYP_RE.search(s)
    if m_dpci:
        dpci = m_dpci.group(1)
        digits = re.sub(r"\D+", "", dpci)  # 9 digits
        return {"sku": digits, "dpci": dpci}
    m_sku = SKU_RE.search(s)
    if m_sku:
        sku = m_sku.group(1)
        dpci = _format_dpci_from_9digits(sku)
        return {"sku": sku, "dpci": dpci}
    return {"sku": None, "dpci": None}

def normalize_line_items(raw_items: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    """
    Normalize Veryfi line_items into clean 'items' AND extract item numbers from text if present:
      - Merge quantity/price detail lines into previous item
      - Treat only known retail section headers as details
      - Drop obvious boilerplate as items
      - Extract SKU (7–13 digits) from description or keep Veryfi 'sku' if provided
      - If 9 digits (Target), add formatted DPCI XXX-XX-XXXX
    """
    out : List[Dict[str,Any]] = []

    for li in raw_items or []:
        desc = (li.get("description") or "").strip()
        qty  = _to_float(li.get("quantity"))
        unit = _to_float(li.get("unit_price") or li.get("price"))
        tot  = _to_float(li.get("total") or li.get("amount"))

        # Veryfi sometimes has these identifiers already:
        li_sku = li.get("sku") or li.get("item_code") or li.get("product_code")
        li_dpci = None
        if li_sku and len(re.sub(r"\D+","", str(li_sku))) == 9:
            li_dpci = _format_dpci_from_9digits(str(li_sku))

        # Skip completely empty rows
        if not desc and qty is None and unit is None and tot is None and not li_sku:
            continue

        # Boilerplate → attach to previous as detail
        if desc and _is_boilerplate(desc):
            if out:
                _merge_detail(out[-1], li, "boilerplate")
            continue

        # Section headers → attach to previous
        if desc and _is_section_header(desc):
            if out:
                _merge_detail(out[-1], li, "section_header")
            continue

        # Price-only or "qty @ price" lines → merge into previous item
        if desc and (_is_price_only_desc(desc) or _looks_like_qty_at(desc)):
            if out:
                m = QTY_AT_RE.search(desc)
                if m:
                    q = float(m.group("q"))
                    u = float(m.group("u").replace("$","").replace(",",""))
                    qty = qty if qty is not None else q
                    unit = unit if unit is not None else u
                    if qty is not None and unit is not None:
                        tot = tot if tot is not None else round(qty*unit, 2)
                if qty is not None: out[-1]["quantity"] = qty
                if unit is not None: out[-1]["unit_price"] = unit
                if tot is not None: out[-1]["total"] = tot
                _merge_detail(out[-1], li, "qty_or_price_detail")
            continue

        # Numeric-only detail rows (no desc) → merge if previous exists
        if not desc and (qty is not None or unit is not None or tot is not None):
            if out:
                prev = out[-1]
                if qty is not None: prev["quantity"] = qty
                if unit is not None: prev["unit_price"] = unit
                if tot is not None and prev.get("total") is None: prev["total"] = tot
                _merge_detail(prev, li, "numeric_detail")
            continue

        # Otherwise: real item
        ids = _extract_ids_from_text(desc)
        sku = li_sku or ids["sku"]
        dpci = li_dpci or ids["dpci"]

        item = {
            "description": desc or None,
            "quantity": qty if qty is not None else 1.0,
            "unit_price": unit,
            "total": tot,
        }
        if sku:  item["sku"] = sku
        if dpci: item["dpci"] = dpci

        out.append(item)

    # Final polish: compute unit if missing and quantity>1 & total present
    for it in out:
        if (it.get("unit_price") is None and
            it.get("total") is not None and
            it.get("quantity") not in (None, 0, 1)):
            it["unit_price"] = round(float(it["total"]) / float(it["quantity"]), 2)

        # If only DPCI exists, also expose digits as sku for convenience
        if "dpci" in it and "sku" not in it:
            digits = re.sub(r"\D+", "", it["dpci"])
            if len(digits) == 9:
                it["sku"] = digits

    return out

# ======= Extract item numbers directly from the image (OCR) =======

import difflib

ID_RE_IMG    = re.compile(r"(?<!\d)(\d{7,13})(?!\d)")
DPCI_RE_IMG  = re.compile(r"\b(\d{3}-\d{2}-\d{4})\b")
PRICE_RE_IMG = re.compile(r"\$?\d{1,3}(?:,\d{3})*\.\d{2}")

def _fmt_dpci(num: str) -> Optional[str]:
    d = re.sub(r"\D+", "", num or "")
    return f"{d[:3]}-{d[3:5]}-{d[5:]}" if len(d) == 9 else None

def _to_float_price(s: str) -> Optional[float]:
    try:
        return float(s.replace("$","").replace(",",""))
    except Exception:
        return None

def _preprocess_for_ocr(path: str):
    import cv2
    img = cv2.imread(path)
    if img is None:
        return None
    h, w = img.shape[:2]
    if max(h, w) < 1500:
        scale = 1500 / max(h, w)
        img = cv2.resize(img, (int(w*scale), int(h*scale)), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.bilateralFilter(gray, 9, 75, 75)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                cv2.THRESH_BINARY, 35, 11)
    return thr

def _ocr_lines_from_image(path: str) -> List[str]:
    # returns normalized per-line text
    from PIL import Image
    import pytesseract, cv2, tempfile, os as _os
    np_img = _preprocess_for_ocr(path)
    if np_img is None:
        return []
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        cv2.imwrite(tmp.name, np_img); tmp_path = tmp.name
    try:
        cfg = "--oem 3 --psm 6 -c preserve_interword_spaces=1"
        text = pytesseract.image_to_string(Image.open(tmp_path), config=cfg, lang="eng")
    finally:
        try: _os.remove(tmp_path)
        except Exception: pass
    lines = []
    for ln in text.splitlines():
        s = ln.strip().replace("—","-").replace("–","-")
        s = re.sub(r"\s+", " ", s)
        if s: lines.append(s)
    return lines

def attach_item_numbers_from_image(image_path: Optional[str],
                                   items: List[Dict[str,Any]],
                                   debug: bool=False) -> List[Dict[str,Any]]:
    """
    Parse the image lines for (ID, optional DPCI, price), then map lines to items.
    Priority: match by exact total price; tie-break by fuzzy description.
    """
    if not image_path:
        return items

    try:
        lines = _ocr_lines_from_image(image_path)
    except Exception as e:
        if debug: print("[ids] local OCR failed:", e)
        return items

    # Build candidate records from raw lines
    cands = []
    for ln in lines:
        dpci_m = DPCI_RE_IMG.search(ln)
        id_m   = ID_RE_IMG.search(ln)
        price_ms = PRICE_RE_IMG.findall(ln)
        if (dpci_m or id_m) and price_ms:
            p = _to_float_price(price_ms[-1])  # last price on the line is usually the line total
            if p is None:
                continue
            sku = None
            dpci = None
            if dpci_m:
                dpci = dpci_m.group(1)
                sku = re.sub(r"\D+","", dpci)
            elif id_m:
                sku = id_m.group(1)
                dpci = _fmt_dpci(sku)
            cands.append({"line": ln, "sku": sku, "dpci": dpci, "price": p})

    if debug:
        print(f"[ids] found {len(cands)} candidate lines with ID+price")

    # Map candidates to items
    for it in items:
        if it.get("sku") or it.get("dpci"):
            continue
        desc = (it.get("description") or "").upper()
        total = it.get("total")

        best = None
        best_score = -1.0

        for c in cands:
            score = 0.0
            # Strong anchor: exact price match on total
            if total is not None and abs((c["price"] or 0.0) - float(total)) < 0.01:
                score += 1.0
            # Soft anchor: description similarity to the candidate line
            if desc:
                score += 0.5 * difflib.SequenceMatcher(None, desc, c["line"].upper()).ratio()
            if score > best_score:
                best, best_score = c, score

        # Accept if price matches OR fuzzy score is reasonable
        if best and ((total is not None and abs(best["price"] - float(total)) < 0.01) or best_score >= 0.35):
            if best.get("sku"):  it["sku"] = best["sku"]
            if best.get("dpci"): it["dpci"] = best["dpci"]
            if debug:
                print(f"[ids] matched '{it.get('description')}' → sku={it.get('sku')} dpci={it.get('dpci')} via line: {best['line']}")

    return items

def enrich_items_with_target(items, api_key=None, cache_path=None, prefer_replace_desc=True):
    """
    For items with a DPCI, fetch canonical Target data via RedCircle (cached),
    format a nice title, and replace the description if requested.
    """
    enriched = []
    for it in items:
        dpci = it.get("dpci")
        if not dpci:
            enriched.append(it)
            continue

        try:
            canonical, _raw = lookup_target_by_dpci(
                dpci=dpci,
                api_key=api_key,           # or rely on REDCIRCLE_API_KEY env var
                cache_path=cache_path,
                cache_only=False,
                include_raw=False
            )
        except Exception:
            canonical = None

        if canonical:
            brand = (canonical.get("brand") or "").strip() or None
            size  = (canonical.get("size") or "").strip() or None
            title = canonical.get("title")
            pretty = format_product_name(title, brand=brand, size_hint=size)

            # Attach canonical metadata
            it["canonical_title"] = pretty or title or it.get("description")
            it["brand"] = brand
            it["size"] = size
            ids = canonical.get("ids") or {}
            # Backfill UPC/GTIN/TCIN if we got them
            if ids.get("tcin"): it["tcin"] = ids["tcin"]
            if ids.get("upc"):  it["upc"]  = ids["upc"]
            if ids.get("gtin"): it["gtin"] = ids["gtin"]

            # Optionally replace the item's description with the canonical title
            if prefer_replace_desc and it["canonical_title"]:
                it["description"] = it["canonical_title"]

            # Tag the source + a confidence hint (DPCI → strong)
            it["name_source"] = "target:redcircle"
            it["name_confidence"] = 0.95
        else:
            # No canonical found; keep original description
            it["name_source"] = "fallback"
            it["name_confidence"] = 0.5

        enriched.append(it)
    return enriched

# ============================ Output =============================
def export_items_to_csv(doc: dict, items: list, out_path: str) -> None:
    import csv
    vendor = (doc.get("vendor") or {}).get("name") or (doc.get("vendor", {}).get("raw_name"))
    date = doc.get("date") or doc.get("document_date") or doc.get("purchase_date")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "merchant","purchase_date","dpci","tcin","upc","sku",
            "name","brand","size","qty","unit_price","total","tax"
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            w.writerow({
                "merchant": vendor,
                "purchase_date": date,
                "dpci": it.get("dpci"),
                "tcin": it.get("tcin"),
                "upc": it.get("upc"),
                "sku": it.get("sku"),
                "name": it.get("description"),   # this is your *formatted* product name now
                "brand": it.get("brand"),
                "size": it.get("size"),
                "qty": it.get("quantity"),
                "unit_price": it.get("unit_price"),
                "total": it.get("total"),
                "tax": doc.get("tax") or doc.get("tax_amount"),
            })

def summarize(doc: dict, items: List[Dict[str,Any]], show_raw: bool=False) -> None:
    vendor = (doc.get("vendor") or {}).get("name") or (doc.get("vendor", {}).get("raw_name"))
    date = doc.get("date") or doc.get("document_date") or doc.get("purchase_date")
    total = doc.get("total") or doc.get("total_amount")
    tax = doc.get("tax") or doc.get("tax_amount")
    currency = doc.get("currency_code") or doc.get("currency")

    print("\n=== Receipt Summary ===")
    print(f"Merchant : {vendor}")
    print(f"Date     : {date}")
    print(f"Total    : {total} {currency or ''}".strip())
    if tax is not None:
        print(f"Tax      : {tax}")

    print("\nItems (normalized):")
    if not items:
        print("  (no items)")
    for i, li in enumerate(items, 1):
        desc = li.get("description")
        qty  = li.get("quantity")
        unit = li.get("unit_price")
        tot  = li.get("total")
        sku  = li.get("sku")
        dpci = li.get("dpci")
        id_str = ""
        if sku:  id_str += f" | sku={sku}"
        if dpci: id_str += f" dpci={dpci}"
        print(f"  {i:02d}. {desc}{id_str} | qty={qty} unit={unit} total={tot}")

    if show_raw:
        raw = doc.get("line_items") or []
        print("\n--- Raw items from Veryfi ---")
        for i, li in enumerate(raw, 1):
            desc = li.get("description")
            qty = li.get("quantity")
            unit = li.get("unit_price")
            tot = li.get("total")
            print(f"  {i:02d}. {desc} | qty={qty} unit={unit} total={tot}")

# ============================ SQLite DB ============================

def _db_connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def _db_init(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS receipts (
      id               INTEGER PRIMARY KEY,
      retailer         TEXT,
      purchase_date    TEXT,
      total            REAL,
      tax              REAL,
      file_path        TEXT,
      veryfi_json_path TEXT,
      receipt_hash     TEXT UNIQUE,
      created_at       TEXT
    );

    CREATE TABLE IF NOT EXISTS items (
      id               INTEGER PRIMARY KEY,
      receipt_id       INTEGER NOT NULL,
      description      TEXT,
      brand            TEXT,
      size             TEXT,
      quantity         REAL,
      unit_price       REAL,
      total            REAL,
      dpci             TEXT,
      tcin             TEXT,
      upc              TEXT,
      sku              TEXT,
      name_source      TEXT,
      name_confidence  REAL,
      FOREIGN KEY (receipt_id) REFERENCES receipts(id) ON DELETE CASCADE
    );
    """)
    conn.commit()

def _compute_receipt_hash(file_path: str | None, doc: dict) -> str:
    """Use file bytes if we have a local file, else hash the Veryfi JSON."""
    h = hashlib.sha256()
    if file_path and Path(file_path).exists():
        h.update(Path(file_path).read_bytes())
    else:
        # stable hash of JSON structure
        h.update(json.dumps(doc, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    return h.hexdigest()

def _extract_vendor_and_date(doc: dict) -> tuple[str|None, str|None]:
    vendor = (doc.get("vendor") or {}).get("name") or (doc.get("vendor") or {}).get("raw_name")
    date = doc.get("date") or doc.get("document_date") or doc.get("purchase_date")
    return vendor, date

def save_to_db(db_path: str, doc: dict, items: list, file_path: str | None, veryfi_json_path: str | None) -> int:
    """
    Upsert a receipt + replace its items. Returns receipt_id.
    De-duplication via receipt_hash.
    """
    conn = _db_connect(db_path)
    _db_init(conn)

    retailer, purchase_date = _extract_vendor_and_date(doc)
    total = doc.get("total") or doc.get("total_amount")
    tax = doc.get("tax") or doc.get("tax_amount")
    rhash = _compute_receipt_hash(file_path, doc)
    created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Try insert; if duplicate, update and reuse existing id
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO receipts (retailer,purchase_date,total,tax,file_path,veryfi_json_path,receipt_hash,created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (retailer, purchase_date, total, tax, file_path, veryfi_json_path, rhash, created_at))
        receipt_id = cur.lastrowid
    except sqlite3.IntegrityError:
        # existing receipt with same hash: fetch id and update headline fields
        cur.execute("SELECT id FROM receipts WHERE receipt_hash = ?", (rhash,))
        row = cur.fetchone()
        receipt_id = row[0]
        cur.execute("""
            UPDATE receipts
               SET retailer=?, purchase_date=?, total=?, tax=?, file_path=?, veryfi_json_path=?, created_at=?
             WHERE id=?
        """, (retailer, purchase_date, total, tax, file_path, veryfi_json_path, created_at, receipt_id))

        # wipe existing items (we re-insert below)
        cur.execute("DELETE FROM items WHERE receipt_id = ?", (receipt_id,))

    # Insert items
    item_sql = """
        INSERT INTO items (receipt_id, description, brand, size, quantity, unit_price, total,
                           dpci, tcin, upc, sku, name_source, name_confidence)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    for it in items:
        cur.execute(item_sql, (
            receipt_id,
            it.get("description"),
            it.get("brand"),
            it.get("size"),
            it.get("quantity"),
            it.get("unit_price"),
            it.get("total"),
            it.get("dpci"),
            it.get("tcin"),
            it.get("upc"),
            it.get("sku"),
            it.get("name_source"),
            it.get("name_confidence"),
        ))

    conn.commit()
    conn.close()
    return receipt_id

def print_db_stats(db_path: str) -> None:
    if not Path(db_path).exists():
        print(f"[db] no database yet at {db_path}")
        return
    conn = _db_connect(db_path); _db_init(conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM receipts"); n_r = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM items"); n_i = cur.fetchone()[0]
    print(f"[db] receipts: {n_r} | items: {n_i}")
    cur.execute("""SELECT id, retailer, purchase_date, total, created_at
                     FROM receipts ORDER BY id DESC LIMIT 10""")
    rows = cur.fetchall()
    if rows:
        print("[db] last 10 receipts:")
        for r in rows:
            print(f"  id={r[0]} | {r[1]} | {r[2]} | total={r[3]} | created={r[4]}")
    conn.close()

# ============================ DB Queries / Reports ============================

def _iso_or_none(s):
    return s if s else None

def db_export_items_between(db_path: str, out_csv: str, date_start: str | None, date_end: str | None) -> int:
    """
    Export all items between [date_start, date_end] (inclusive) to CSV.
    Dates should be 'YYYY-MM-DD'. Pass None to omit that bound.
    Returns number of rows exported.
    """
    import csv
    conn = _db_connect(db_path); _db_init(conn)
    cur = conn.cursor()

    where = []
    params = []
    if date_start:
        where.append("date(purchase_date) >= date(?)")
        params.append(date_start)
    if date_end:
        where.append("date(purchase_date) <= date(?)")
        params.append(date_end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT r.retailer, r.purchase_date, i.dpci, i.tcin, i.upc, i.sku,
           i.description, i.brand, i.size, i.quantity, i.unit_price, i.total, r.tax
      FROM items i
      JOIN receipts r ON i.receipt_id = r.id
      {where_sql}
      ORDER BY date(r.purchase_date) ASC, i.id ASC
    """
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    import os
    from pathlib import Path
    Path(os.path.dirname(out_csv) or ".").mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["merchant","purchase_date","dpci","tcin","upc","sku",
                    "name","brand","size","qty","unit_price","total","tax"])
        for r in rows:
            w.writerow(r)
    return len(rows)

def db_spend_summary(db_path: str, date_start: str | None, date_end: str | None) -> dict:
    """
    Return {'receipts': N, 'items': M, 'subtotal': x, 'tax': y, 'total': z}
    in the given date window (inclusive).
    """
    conn = _db_connect(db_path); _db_init(conn)
    cur = conn.cursor()

    where = []
    params = []
    if date_start:
        where.append("date(purchase_date) >= date(?)")
        params.append(date_start)
    if date_end:
        where.append("date(purchase_date) <= date(?)")
        params.append(date_end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    cur.execute(f"SELECT COUNT(*), SUM(total), SUM(tax) FROM receipts {where_sql}", params)
    n_receipts, sum_total, sum_tax = cur.fetchone()
    cur.execute(f"""
        SELECT COUNT(*), COALESCE(SUM(i.total), 0.0)
          FROM items i
          JOIN receipts r ON i.receipt_id = r.id
          {where_sql}
    """, params)
    n_items, items_total = cur.fetchone()
    conn.close()

    return {
        "receipts": int(n_receipts or 0),
        "items": int(n_items or 0),
        "subtotal_items_sum": float(items_total or 0.0),
        "tax_sum": float(sum_tax or 0.0),
        "receipts_total_sum": float(sum_total or 0.0),
    }

# ============================ CLI/Main ===========================

def main():
    ap = argparse.ArgumentParser(description="Extract receipt data with Veryfi v8 (line-item normalization + image-based item IDs)")
    g = ap.add_mutually_exclusive_group(required=False)
    g.add_argument("--file", help="Path to image/PDF")
    g.add_argument("--url", help="Public URL to the file")
    ap.add_argument("--category", help="Optional category label")
    ap.add_argument("--auto-delete", action="store_true", help="Ask Veryfi to delete the document after processing")
    ap.add_argument("--save-json", action="store_true", help="Save full Veryfi JSON next to the file (or in cwd for URL)")
    ap.add_argument("--save-normalized", action="store_true", help="Also save normalized items JSON")
    ap.add_argument("--show-raw", action="store_true", help="Print original Veryfi line items after normalized view")
    ap.add_argument("--no-image-ids", action="store_true", help="Disable image-based item ID extraction (use only Veryfi fields/text)")
    ap.add_argument("--debug-ids", action="store_true", help="Verbose matching logs for item number extraction")
    ap.add_argument("--export-csv", nargs="?", const="AUTO", help="Export items to CSV. Pass a path or leave blank to auto-name.")
    ap.add_argument("--save-db", action="store_true", help="Persist receipt + items to SQLite")
    ap.add_argument("--db-path", default="data/receipts.db", help="SQLite file path (default: data/receipts.db)")
    ap.add_argument("--db-stats", action="store_true", help="Print database stats and exit")
    ap.add_argument("--export-from-db", nargs="?", const="AUTO", help="Export items from DB over a date range to CSV (use with --from and/or --to)")
    ap.add_argument("--from", dest="date_from", help="Start date YYYY-MM-DD (inclusive)")
    ap.add_argument("--to", dest="date_to", help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--report", action="store_true", help="Print spend summary for the date range (use with --from/--to)")

    args = ap.parse_args()

    # allow DB-only operations without a file/url
    if not (args.file or args.url) and not (args.db_stats or args.report or args.export_from_db):
        ap.error("one of the arguments --file --url is required (unless using --db-stats, --report, or --export-from-db)")

    # ensure DB exists if any DB op requested
    if args.db_stats or args.save_db or args.export_from_db or args.report:
        conn = _db_connect(args.db_path);
        _db_init(conn);
        conn.close()

    if args.db_stats:
        print_db_stats(args.db_path)
        sys.exit(0)

    # reports/exports from DB only (no need to process a file)
    if args.report:
        summary = db_spend_summary(args.db_path, args.date_from, args.date_to)
        print("[report]", summary)
        sys.exit(0)

    if args.export_from_db is not None:
        if args.export_from_db == "AUTO":
            out_csv = "exports/db_items.csv"
        else:
            out_csv = args.export_from_db
        n = db_export_items_between(args.db_path, out_csv, args.date_from, args.date_to)
        print(f"[db] exported {n} rows → {out_csv}")
        sys.exit(0)

    extra = {}
    if args.category: extra["categories"] = [args.category]
    if args.auto_delete: extra["auto_delete"] = True

    if args.file:
        doc = process_local_file(args.file, extra)
        if args.save_json:
            out = args.file + ".veryfi.json"
            pathlib.Path(out).write_text(json.dumps(doc, indent=2), encoding="utf-8")
            print(f"\nSaved JSON → {out}")
    else:
        doc = process_file_url(args.url, extra)
        if args.save_json:
            out = "veryfi_result.json"
            pathlib.Path(out).write_text(json.dumps(doc, indent=2), encoding="utf-8")
            print(f"\nSaved JSON → {out}")

    # Normalize
    normalized = normalize_line_items(doc.get("line_items") or [])

    # Attach item numbers from image (works even if Veryfi doesn't return text/sku)
    if args.file and not args.no_image_ids:
        normalized = attach_item_numbers_from_image(
            image_path=args.file,
            items=normalized,
            debug=args.debug_ids
        )

    normalized = enrich_items_with_target(
        normalized,
        api_key=os.environ.get("REDCIRCLE_API_KEY"),  # or None if you set env var already
        cache_path=None,  # you can give a path to a custom cache file
        prefer_replace_desc=True  # True = replace receipt code with nice title
    )

    # --- CSV export (overwrite same path for same receipt) ---
    if args.export_csv is not None:
        from pathlib import Path
        if args.export_csv == "AUTO":
            if args.file:
                out_csv = f"{args.file}.items.csv"  # always same filename → overwrites
            else:
                out_csv = "veryfi_items.csv"
        else:
            out_csv = args.export_csv

        # ensure parent dir exists (in case user passed a custom path)
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

        export_items_to_csv(doc, normalized, out_csv)  # overwrite by default
        print(f"Saved items CSV → {out_csv}")

    if args.save_db:
        file_path = args.file if args.file else None
        veryfi_json_path = (args.file + ".veryfi.json") if (args.file and args.save_json) else None
        rid = save_to_db(args.db_path, doc, normalized, file_path, veryfi_json_path)
        print(f"[db] saved receipt_id={rid} in {args.db_path}")

    if args.save_normalized:
        norm_path = (args.file + ".normalized.json") if args.file else "veryfi_normalized.json"
        pathlib.Path(norm_path).write_text(json.dumps(normalized, indent=2), encoding="utf-8")
        print(f"Saved normalized items → {norm_path}")

    if args.save_db:
        # choose whatever paths you already use/printed
        file_path = args.file if args.file else None
        veryfi_json_path = (args.file + ".veryfi.json") if (args.file and args.save_json) else None
        rid = save_to_db(args.db_path, doc, normalized, file_path, veryfi_json_path)
        print(f"[db] saved receipt_id={rid} in {args.db_path}")

    summarize(doc, items=normalized, show_raw=args.show_raw)

if __name__ == "__main__":
    main()