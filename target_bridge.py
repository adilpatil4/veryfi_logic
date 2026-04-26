#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
target_bridge.py

Target product lookup via RedCircle API with aggressive local caching.
- Primary key: DPCI (e.g., 261-02-0005 or 261020005)
- Falls back to RedCircle `type=search&search_term=<dpci>` when direct product by dpci is unsupported.
- Cache-first: avoids unnecessary API calls. Supports cache-only mode.

Env:
  REDCIRCLE_API_KEY   (preferred instead of hardcoding)

CLI examples:
  export REDCIRCLE_API_KEY="YOUR_KEY"
  python target_bridge.py --dpci 261-02-0005
  python target_bridge.py --dpci 261020005 --raw
  python target_bridge.py --dpci 261-02-0005 --cache-only   # no network, cache hit required
  python target_bridge.py --dpci 261-02-0005 --cache ./mycache.json
"""

from __future__ import annotations
import os, sys, json, argparse, re, time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import requests

REDCIRCLE_ENDPOINT = "https://api.redcircleapi.com/request"

# ---------------------------- Cache -----------------------------

def default_cache_path() -> Path:
    base = os.environ.get("XDG_CACHE_HOME") or os.path.join(Path.home(), ".cache")
    Path(base).mkdir(parents=True, exist_ok=True)
    return Path(base) / "target_redcircle_cache.json"

def load_cache(cache_path: Optional[str]) -> Dict[str, Any]:
    p = Path(cache_path) if cache_path else default_cache_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Any], cache_path: Optional[str]) -> None:
    p = Path(cache_path) if cache_path else default_cache_path()
    try:
        p.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        print(f"[cache] failed to save cache: {e}", file=sys.stderr)

def cache_get(cache: Dict[str, Any], dpci_norm: str) -> Optional[Dict[str, Any]]:
    return cache.get(dpci_norm)

def cache_put(cache: Dict[str, Any], dpci_norm: str, value: Dict[str, Any]) -> None:
    cache[dpci_norm] = {"value": value, "ts": int(time.time())}

def cache_value(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not entry: return None
    return entry.get("value")

# --------------------------- Helpers ----------------------------

def _env_or_arg(api_key: Optional[str]) -> str:
    k = api_key or os.environ.get("REDCIRCLE_API_KEY")
    if not k:
        print("[target_bridge] Missing API key. Set REDCIRCLE_API_KEY or use --api-key.", file=sys.stderr)
        sys.exit(2)
    return k

def normalize_dpci(dpci: str) -> str:
    """Normalize to 'XXX-XX-XXXX'. If already formatted, returns as-is."""
    if not dpci:
        return dpci
    digits = re.sub(r"\D+", "", dpci)
    if len(digits) == 9:
        return f"{digits[:3]}-{digits[3:5]}-{digits[5:]}"
    return dpci  # leave as-is if unexpected

def _first(obj: Any) -> Any:
    if isinstance(obj, list):
        return obj[0] if obj else None
    return obj

def _safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur

def _canonicalize_from_product(raw: Dict[str, Any]) -> Dict[str, Any]:
    product = _safe_get(raw, ["product"], {}) or {}
    def pick(*paths, default=None):
        for path in paths:
            v = _safe_get(product, path, None)
            if v: return v
            v2 = _safe_get(raw, path, None)
            if v2: return v2
        return default

    title = pick(["title"], ["name"])
    brand = pick(["brand", "name"], ["brand"])
    tcin  = pick(["tcin"])
    dpci  = pick(["dpci"])
    upc   = pick(["upc"])
    gtin  = pick(["gtin"], ["gtin13"], ["gtin14"], ["ean"], ["barcode"])

    offers = pick(["offers"])
    price = None; currency = None
    if isinstance(offers, dict):
        price = offers.get("price") or offers.get("lowPrice")
        currency = offers.get("priceCurrency")
    elif isinstance(offers, list) and offers:
        o = offers[0]
        if isinstance(o, dict):
            price = o.get("price") or o.get("lowPrice")
            currency = o.get("priceCurrency")

    size = None
    if isinstance(title, str):
        m = re.search(r"\b(\d+(?:\.\d+)?)\s*(oz|fl oz|lb|lbs|g|kg|ct|count|pk|pack|gal|inch|in)\b", title, flags=re.I)
        if m: size = f"{m.group(1)} {m.group(2)}".replace("count","ct")

    canonical = {
        "title": title,
        "brand": brand,
        "size": size,
        "ids": {"tcin": tcin, "dpci": dpci, "upc": upc, "gtin": gtin},
        "price": price,
        "currency": currency,
        "source": "target:redcircle",
        "raw_ok": bool(product) or bool(raw)
    }
    return canonical

def redcircle_request(api_key: str, **params) -> Dict[str, Any]:
    if "type" not in params:
        raise ValueError("RedCircle request requires 'type' parameter.")
    query = {"api_key": api_key}; query.update(params)
    resp = requests.get(REDCIRCLE_ENDPOINT, params=query, timeout=30)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"_non_json": resp.text}

def _lookup_by_dpci_network(api_key: str, dpci_norm: str) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    # Try direct product by dpci if supported for your plan
    try:
        raw = redcircle_request(api_key, type="product", dpci=dpci_norm)
        if _safe_get(raw, ["product"]):
            return _canonicalize_from_product(raw), raw
    except requests.HTTPError:
        pass
    # Fallback: search by dpci
    raw = redcircle_request(api_key, type="search", search_term=dpci_norm)
    results = _safe_get(raw, ["search_results"]) or _safe_get(raw, ["results"]) or []
    first = _first(results)
    if isinstance(first, dict):
        wrapped = {"product": first}
        return _canonicalize_from_product(wrapped), raw
    return None, raw

# --------------------------- Public API --------------------------

def lookup_target_by_dpci(dpci: str,
                          api_key: Optional[str] = None,
                          cache_path: Optional[str] = None,
                          cache_only: bool = False,
                          include_raw: bool = False) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Main entry point for your app:
      - Normalizes DPCI
      - Reads cache
      - If cache hit → return cached value (no network)
      - Else (unless cache_only) → query RedCircle and cache result

    Returns (canonical_product, raw_json_or_None)
    """
    k = _env_or_arg(api_key)
    dpci_norm = normalize_dpci(dpci)
    cache = load_cache(cache_path)

    hit = cache_value(cache_get(cache, dpci_norm))
    if hit:
        raw = hit.get("_raw") if include_raw else None
        return hit.get("canonical") or hit, raw

    if cache_only:
        return None, None

    canonical, raw = _lookup_by_dpci_network(k, dpci_norm)
    if canonical:
        cache_put(cache, dpci_norm, {"canonical": canonical, "_raw": raw})
        save_cache(cache, cache_path)
    return canonical, (raw if include_raw else None)

# ------------------------------ CLI ------------------------------

def main():
    ap = argparse.ArgumentParser(description="Target lookup via RedCircle (DPCI-first) with caching")
    ap.add_argument("--dpci", required=True, help="DPCI code (e.g., 261-02-0005 or 261020005)")
    ap.add_argument("--api-key", help="RedCircle API key (or set REDCIRCLE_API_KEY)")
    ap.add_argument("--cache", help="Path to cache json (default: ~/.cache/target_redcircle_cache.json)")
    ap.add_argument("--cache-only", action="store_true", help="Use cache only, do not call the API")
    ap.add_argument("--raw", action="store_true", help="Also print raw RedCircle JSON when not cache-only")
    args = ap.parse_args()

    canonical, raw = lookup_target_by_dpci(
        dpci=args.dpci,
        api_key=args.api_key,
        cache_path=args.cache,
        cache_only=args.cache_only,
        include_raw=args.raw
    )

    print("\n=== Canonical Product ===")
    print(json.dumps(canonical or {"found": False}, indent=2, ensure_ascii=False))

    if args.raw and raw:
        print("\n=== Raw RedCircle JSON ===")
        print(json.dumps(raw, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
