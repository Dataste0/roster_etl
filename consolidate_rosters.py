#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
consolidate_rosters.py

Deterministic roster consolidator for messy Excel workbooks.

Input:
- Iterate all .xlsx and .xlsm files in a folder (default: ./rosters_input)
- Detect the roster sheet(s), detect header row, map columns to a strict canonical schema, extract rows.

Output CSVs (utf-8-sig):
- _consolidated_roster.csv
- _roster_extraction_report.csv

Logs:
- Console + logs/roster_etl.log

Canonical fields (ONLY these):
- full_name
- first_name (if present)
- last_name  (if present)
- email
- srt_id
- active_status
- contributor_project_id (a02... if present)
- project_id     (a01... if present)
- source_file
- source_sheet

Important SRT rules:
- SRT ID is digits-only after extraction
- length >= 13
- prefix starts with "10" or "61"
- cells may contain multiple IDs; pick the candidate that matches the rule

Active status rules:
- If a status/state column exists -> normalize per-row
- Else, infer a default status from the sheet name (Active / Hold / Removed / Inactive)
- If sheet name is ambiguous -> default to "active"

Dependencies:
- pandas
- openpyxl

Notes:
- .xls is NOT supported by openpyxl. "--include-xls" is best-effort and will warn/skip.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from venv import logger
from pandas.api.types import is_scalar
import pandas as pd


# -----------------------
# Canonical schema (ONLY)
# -----------------------

CANONICAL_FIELDS = [
    "full_name",
    "first_name",
    "last_name",
    "email",
    "srt_id",
    "active_status",
    "contributor_project_id",
    "project_id",
    "source_file",
    "source_sheet",
]

# Required for a row to be kept (at least one of these must be present)
ROW_KEEP_IF_ANY_PRESENT = ["email", "srt_id", "full_name", "contributor_project_id"]

# Header scan
HEADER_SCAN_MAX_ROWS = 35

# Sheet emptiness check preview
SHEET_EMPTY_MIN_NONEMPTY = 8

# Profiling sample size (for deterministic scoring)
PROFILE_SAMPLE_MAX = 20

# SRT rules
SRT_MIN_DIGITS = 13
SRT_PREFIXES = ("10", "61") # SRT IDs start with these digit sequences

EMAIL_REGEX = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.I)

# Strong guard against mapping project columns to contributor name
PROJECT_NAME_GUARD_REGEX = re.compile(r"\bproject\b.*\bname\b|\bname\b.*\bproject\b", re.I)

# De-prioritize these sheet types
NON_ROSTER_SHEET_HINTS = re.compile(
    r"\b(pivot|dashboard|summary|readme|notes?|instruction|guide|config|setup|lookup|reference|template)\b",
    re.I,
)

# Sheet selection keywords
ROSTER_WORD = re.compile(r"\broster\b", re.I)
COMPLETE_WORDS = re.compile(r"\b(full|complete|master|all|total|entire)\b", re.I)
ACTIVE_WORDS = re.compile(r"\b(active|current)\b", re.I)
REMOVED_WORDS = re.compile(r"\b(removed|termed|paused|inactive|hold|archived|offboard|off-board|deactivated)\b", re.I)

# Status normalization keywords
STATUS_ACTIVE_WORDS = re.compile(r"\b(active|current|enabled|live)\b", re.I)
STATUS_INACTIVE_WORDS = re.compile(r"\b(inactive|disabled)\b", re.I)
STATUS_HOLD_WORDS = re.compile(r"\b(hold|on\s*hold|paused|pause|suspended|pending)\b", re.I)
STATUS_REMOVED_WORDS = re.compile(r"\b(removed|termed|terminated|offboard|off-board|archived|deactivated)\b", re.I)

# Validation thresholds (soft)
EMAIL_VALID_RATIO_MIN = 0.30
NAME_VALID_RATIO_MIN = 0.30
SRT_VALID_RATIO_MIN = 0.10


# -------------
# Data classes
# -------------

@dataclass
class SheetChoice:
    strategy: str
    chosen_sheets: List[str]
    sheet_scores: Optional[List[Tuple[str, float]]] = None
    warnings: Optional[List[str]] = None


@dataclass
class FileReport:
    file: str
    strategy: str
    chosen_sheets: str
    header_row_idx: Optional[int]
    rows_extracted: int
    mapping_json: str
    warnings: str
    errors: str
    top_sheet_scores_json: str


# ----------
# Logging
# ----------

def setup_logging(debug: bool) -> logging.Logger:
    logger = logging.getLogger("roster_etl")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)
    log_path = Path("logs") / "roster_etl.log"

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG if debug else logging.INFO)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG if debug else logging.INFO)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# -------------------------
# Workbook / sheet reading
# -------------------------

def read_workbook_sheets(
    file_path: Path,
    include_xls: bool,
    logger: logging.Logger,
) -> Tuple[Optional[pd.ExcelFile], List[str], List[str]]:
    """
    Returns (ExcelFile or None, non_empty_sheet_names, warnings).
    Uses openpyxl for .xlsx/.xlsm. .xls is not supported (warn/skip).
    """
    warnings: List[str] = []
    suffix = file_path.suffix.lower()

    if suffix == ".xls":
        msg = "Skipping .xls: openpyxl cannot read .xls. Convert to .xlsx/.xlsm or add xlrd."
        if include_xls:
            warnings.append(msg)
            logger.warning(f"{file_path.name}: {msg}")
        else:
            warnings.append(msg)
            logger.info(f"{file_path.name}: {msg}")
        return None, [], warnings

    if suffix not in (".xlsx", ".xlsm"):
        msg = f"Unsupported extension: {suffix}"
        warnings.append(msg)
        logger.warning(f"{file_path.name}: {msg}")
        return None, [], warnings

    try:
        xls = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception as e:
        warnings.append(f"Failed to open workbook: {e}")
        logger.exception(f"{file_path.name}: Failed to open workbook")
        return None, [], warnings

    non_empty: List[str] = []
    for sh in xls.sheet_names:
        try:
            raw = pd.read_excel(
                file_path,
                sheet_name=sh,
                header=None,
                engine="openpyxl",
                nrows=200,
            )
            non_empty_cells = int(raw.notna().sum().sum())
            if non_empty_cells >= SHEET_EMPTY_MIN_NONEMPTY:
                non_empty.append(sh)
            else:
                logger.debug(f"{file_path.name}: sheet '{sh}' seems empty (non_empty_cells={non_empty_cells})")
        except Exception as e:
            warnings.append(f"Failed reading sheet '{sh}' (preview): {e}")
            logger.debug(f"{file_path.name}: Failed reading sheet '{sh}' preview: {e}")

    return xls, non_empty, warnings


# --------------------------
# Sheet selection heuristics
# --------------------------

def _score_sheet_name(name: str) -> float:
    s = 0.0
    lname = (name or "").lower()

    if ROSTER_WORD.search(lname):
        s += 6.0
    if COMPLETE_WORDS.search(lname):
        s += 3.0
    if ACTIVE_WORDS.search(lname):
        s += 1.5
    if REMOVED_WORDS.search(lname):
        s += 1.0
    if NON_ROSTER_SHEET_HINTS.search(lname):
        s -= 4.0
    if len(lname) <= 18 and (ROSTER_WORD.search(lname) or ACTIVE_WORDS.search(lname)):
        s += 0.5

    return s


def _content_signals_for_scoring(raw: pd.DataFrame) -> Dict[str, float]:
    vals = raw.values.flatten()
    str_vals = ["" if pd.isna(v) else str(v).strip() for v in vals]
    str_vals = [v for v in str_vals if v]

    if not str_vals:
        return dict(email_like_ratio=0.0, srt_like_ratio=0.0, header_keyword_density=0.0)

    email_hits = sum(1 for v in str_vals if EMAIL_REGEX.match(v))
    email_ratio = email_hits / max(1, len(str_vals))

    srt_hits = 0
    for v in str_vals:
        d = re.sub(r"\D", "", v)
        if len(d) >= SRT_MIN_DIGITS and d.startswith(SRT_PREFIXES):
            srt_hits += 1
    srt_ratio = srt_hits / max(1, len(str_vals))

    header_keywords = re.compile(r"\b(email|e-mail|mail|full\s*name|first|last|srt|tool|id|status|state)\b", re.I)
    first = raw.head(min(HEADER_SCAN_MAX_ROWS, len(raw)))

    hit = 0
    tot = 0
    for r in range(first.shape[0]):
        for c in range(first.shape[1]):
            v = first.iat[r, c]
            if pd.isna(v):
                continue
            t = str(v).strip()
            if not t:
                continue
            tot += 1
            if header_keywords.search(t):
                hit += 1
    density = hit / max(1, tot)

    return dict(email_like_ratio=email_ratio, srt_like_ratio=srt_ratio, header_keyword_density=density)


def _score_sheet_content(raw: pd.DataFrame) -> float:
    sig = _content_signals_for_scoring(raw)
    return (sig["header_keyword_density"] * 8.0) + (sig["email_like_ratio"] * 6.0) + (sig["srt_like_ratio"] * 4.0)


def _pick_best_union(active_sheets: List[str], removed_sheets: List[str]) -> List[str]:
    a = sorted([(s, _score_sheet_name(s)) for s in active_sheets], key=lambda x: x[1], reverse=True)[0][0]
    r = sorted([(s, _score_sheet_name(s)) for s in removed_sheets], key=lambda x: x[1], reverse=True)[0][0]
    return [a] if a == r else [a, r]


def choose_roster_sheets(
    file_path: Path,
    non_empty_sheets: List[str],
    logger: logging.Logger,
) -> SheetChoice:
    warnings: List[str] = []

    roster_named = [s for s in non_empty_sheets if ROSTER_WORD.search(s)]
    if roster_named:
        complete = [s for s in roster_named if COMPLETE_WORDS.search(s)]
        if complete:
            chosen = [sorted(complete, key=lambda x: (len(x), x.lower()))[0]]
            return SheetChoice(strategy="roster_name_full", chosen_sheets=chosen, warnings=warnings)

        active = [s for s in non_empty_sheets if ACTIVE_WORDS.search(s)]
        removed = [s for s in non_empty_sheets if REMOVED_WORDS.search(s)]
        if active and removed:
            chosen = _pick_best_union(active, removed)
            warnings.append("No 'complete' roster sheet found; using union of active + removed-like sheets.")
            return SheetChoice(strategy="roster_union_active_removed", chosen_sheets=chosen, warnings=warnings)

        scores = [(s, _score_sheet_name(s)) for s in roster_named]
        scores.sort(key=lambda x: x[1], reverse=True)
        return SheetChoice(
            strategy="roster_name_best_scored",
            chosen_sheets=[scores[0][0]],
            sheet_scores=scores[:10],
            warnings=warnings,
        )

    active = [s for s in non_empty_sheets if ACTIVE_WORDS.search(s)]
    removed = [s for s in non_empty_sheets if REMOVED_WORDS.search(s)]
    if active and removed:
        chosen = _pick_best_union(active, removed)
        warnings.append("No sheet name contains 'roster'; using union of active + removed-like sheets.")
        return SheetChoice(strategy="union_active_removed_no_roster", chosen_sheets=chosen, warnings=warnings)

    combined_scores: List[Tuple[str, float]] = []
    for sh in non_empty_sheets:
        try:
            raw = pd.read_excel(file_path, sheet_name=sh, header=None, engine="openpyxl", nrows=200)
            score = _score_sheet_name(sh) + _score_sheet_content(raw)
            combined_scores.append((sh, score))
        except Exception as e:
            warnings.append(f"Failed scoring sheet '{sh}': {e}")

    if not combined_scores:
        return SheetChoice(strategy="no_sheets", chosen_sheets=[], warnings=warnings)

    combined_scores.sort(key=lambda x: x[1], reverse=True)
    return SheetChoice(strategy="scoring_all_sheets", chosen_sheets=[combined_scores[0][0]], sheet_scores=combined_scores[:10], warnings=warnings)


# --------------------
# Header row detection
# --------------------

def header_row_candidates(raw: pd.DataFrame, max_rows: int = HEADER_SCAN_MAX_ROWS) -> List[Tuple[int, float]]:
    """
    Find the most likely header row among the first N rows.
    """
    keywords = re.compile(
        r"\b(email|e-mail|mail|full\s*name|first|last|srt|tool|id|status|state|contributor)\b",
        re.I,
    )

    candidates: List[Tuple[int, float]] = []
    nrows = min(max_rows, len(raw))
    ncols = raw.shape[1]

    for r in range(nrows):
        row = raw.iloc[r, :]
        non_null = row.dropna()
        if non_null.empty:
            continue

        cells = [str(v).strip() for v in non_null.values if str(v).strip() != ""]
        if len(cells) < 3:
            continue

        kw_hits = sum(1 for t in cells if keywords.search(t))
        kw_score = kw_hits * 2.5

        short = sum(1 for t in cells if len(t) <= 24)
        short_score = (short / max(1, len(cells))) * 2.0

        uniq_ratio = len(set(t.lower() for t in cells)) / max(1, len(cells))
        uniq_score = uniq_ratio * 1.5

        long_pen = sum(1 for t in cells if len(t) > 50) * 0.7

        numeric_only = sum(1 for t in cells if re.fullmatch(r"[\d\W]+", t) is not None)
        num_pen = (numeric_only / max(1, len(cells))) * 2.0

        density = len(non_null) / max(1, ncols)
        dens_score = min(1.0, density * 2.0) * 1.0

        score = kw_score + short_score + uniq_score + dens_score - long_pen - num_pen
        candidates.append((r, score))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates


def build_df_with_header(raw: pd.DataFrame, header_row_idx: int) -> pd.DataFrame:
    header_row = raw.iloc[header_row_idx, :].tolist()
    headers: List[str] = []
    for i, h in enumerate(header_row):
        if pd.isna(h) or str(h).strip() == "":
            headers.append(f"__col_{i}")
        else:
            headers.append(str(h).strip())

    df = raw.iloc[header_row_idx + 1 :, :].copy()
    df.columns = make_unique_columns(headers)

    df = df.dropna(axis=0, how="all").dropna(axis=1, how="all")

    def _row_all_empty(row) -> bool:
        for v in row.values:
            if is_na_scalar(v):
                continue
            if str(v).strip() != "":
                return False
        return True

    if not df.empty:
        mask = df.apply(_row_all_empty, axis=1)
        df = df.loc[~mask].copy()

    return df


def detect_header_and_build_df(
    file_path: Path,
    sheet_name: str,
    logger: logging.Logger,
) -> Tuple[pd.DataFrame, Optional[int], List[str]]:
    warnings: List[str] = []

    raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="openpyxl")
    cands = header_row_candidates(raw, HEADER_SCAN_MAX_ROWS)

    if not cands:
        warnings.append("No header candidates found; using first non-empty row as header.")
        header_idx = 0
        for r in range(min(HEADER_SCAN_MAX_ROWS, len(raw))):
            if raw.iloc[r, :].dropna().shape[0] >= 3:
                header_idx = r
                break
    else:
        header_idx = cands[0][0]

    logger.debug(f"{file_path.name} | {sheet_name}: header candidates top5={cands[:5]}")
    logger.info(f"{file_path.name} | {sheet_name}: chosen header_row_idx={header_idx}")

    df = build_df_with_header(raw, header_idx)

    if df.shape[1] > 0:
        auto_cols = sum(1 for c in df.columns if str(c).startswith("__col_"))
        if auto_cols / max(1, df.shape[1]) > 0.6:
            warnings.append("Many blank header cells; mapping may be less reliable.")

    return df, header_idx, warnings


# -------------------------
# Profiling and detectors
# -------------------------

def _safe_series_to_str(s: Any) -> pd.Series:
    """
    Ensure we always return a 1D string Series. If a duplicated column label
    returns a DataFrame, pick the first sub-column deterministically.
    """
    if isinstance(s, pd.DataFrame):
        # duplicated column label -> take first physical column
        s = s.iloc[:, 0]
    if not isinstance(s, pd.Series):
        # last-resort: coerce to Series
        s = pd.Series(s)

    return s.apply(lambda v: "" if is_na_scalar(v) else str(v).strip())


def looks_like_email(v: str) -> bool:
    if not v:
        return False
    return EMAIL_REGEX.match(v) is not None


def extract_digits(v: str) -> str:
    return re.sub(r"\D", "", v or "")


def extract_srt_id_from_cell(v: Any) -> Optional[str]:
    """
    Extract a valid SRT ID from a cell:
    - handle numeric Excel values (including scientific notation)
    - find digit sequences in strings
    - accept candidates with >=13 digits and prefix 10/61
    - if multiple, return the longest valid candidate
    """
    if is_na_scalar(v):
        return None

    # Handle numeric types explicitly (Excel often stores large IDs as floats)
    if isinstance(v, int):
        t = str(v)
    elif isinstance(v, float):
        # Large IDs may appear as floats; format to integer-like string
        if abs(v) >= 1e12:
            t = f"{v:.0f}"
        else:
            t = str(v)
    else:
        t = str(v).strip()

    t = t.strip()
    if not t:
        return None

    # Candidate digit sequences
    seqs = re.findall(r"\d{6,}", t)
    whole = re.sub(r"\D", "", t)
    if whole and whole not in seqs:
        seqs.append(whole)

    candidates: List[str] = []
    for s in seqs:
        d = re.sub(r"\D", "", s)
        if len(d) >= SRT_MIN_DIGITS and d.startswith(SRT_PREFIXES):
            candidates.append(d)

    if not candidates:
        return None

    candidates.sort(key=lambda x: (len(x), x), reverse=True)
    return candidates[0]



def looks_like_person_name(v: str) -> bool:
    if not v:
        return False
    if PROJECT_NAME_GUARD_REGEX.search(v):
        return False
    if re.search(r"\bproject\b", v, re.I):
        return False
    if not re.search(r"[A-ZÀ-ÖØ-öø-ÿ]", v, re.I):
        return False
    if len(v) < 3:
        return False
    # Reject very numeric strings
    if len(re.sub(r"[^0-9]", "", v)) / max(1, len(v)) > 0.30:
        return False
    return True


def looks_like_a01(v: str) -> bool:
    # project id: starts with a01 (case-insensitive)
    return bool(v) and re.match(r"^a01[a-z0-9]+$", v.strip(), re.I) is not None


def looks_like_a02(v: str) -> bool:
    # contributor id: starts with a02 (case-insensitive)
    return bool(v) and re.match(r"^a02[a-z0-9]+$", v.strip(), re.I) is not None


def profile_columns(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    prof: Dict[str, Dict[str, Any]] = {}

    for col in df.columns:
        s = _safe_series_to_str(df[col])
        non_empty = s[s != ""]
        sample = non_empty.head(PROFILE_SAMPLE_MAX).tolist()

        if non_empty.empty:
            prof[col] = dict(
                sample=[],
                non_empty=0,
                looks_like_email=0.0,
                looks_like_name=0.0,
                srt_like=0.0,
                a01_like=0.0,
                a02_like=0.0,
                mostly_constant=True,
                numeric_ratio=0.0,
                unique_ratio=0.0,
                avg_len=0.0,
            )
            continue

        email_hits = sum(1 for v in non_empty if looks_like_email(v))
        name_hits = sum(1 for v in non_empty if looks_like_person_name(v))
        srt_hits = sum(1 for v in non_empty if extract_srt_id_from_cell(v) is not None)
        a01_hits = sum(1 for v in non_empty if looks_like_a01(v))
        a02_hits = sum(1 for v in non_empty if looks_like_a02(v))

        numeric_hits = sum(1 for v in non_empty if re.fullmatch(r"[0-9]+(\.[0-9]+)?", v) is not None)

        unique_vals = non_empty.nunique(dropna=True)
        unique_ratio = unique_vals / max(1, len(non_empty))

        avg_len = float(non_empty.map(len).mean()) if len(non_empty) else 0.0
        most_common = non_empty.value_counts().head(1).iloc[0]
        mostly_constant = (most_common / max(1, len(non_empty))) > 0.85

        prof[col] = dict(
            sample=sample,
            non_empty=int(len(non_empty)),
            looks_like_email=email_hits / max(1, len(non_empty)),
            looks_like_name=name_hits / max(1, len(non_empty)),
            srt_like=srt_hits / max(1, len(non_empty)),
            a01_like=a01_hits / max(1, len(non_empty)),
            a02_like=a02_hits / max(1, len(non_empty)),
            mostly_constant=bool(mostly_constant),
            numeric_ratio=numeric_hits / max(1, len(non_empty)),
            unique_ratio=float(unique_ratio),
            avg_len=float(avg_len),
        )

    return prof


# ----------------------------
# Deterministic column mapping
# ----------------------------

def _norm_col(c: str) -> str:
    """
    Normalize column header text for robust regex matching.
    """
    s = str(c)
    s = s.replace("\u00A0", " ")      # NBSP
    s = s.replace("\u2007", " ")      # figure space
    s = s.replace("\u202F", " ")      # narrow NBSP
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"[/\|,_\-:;]+", " ", s)   # separators to spaces
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def golden_map_columns(
    columns: List[str],
    profiles: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Optional[str]], Dict[str, List[str]], List[str]]:
    """
    Deterministic mapping using conservative regex synonyms + profile-based scoring.
    """
    warnings: List[str] = []
    mapping: Dict[str, Optional[str]] = {
        "email": None,
        "full_name": None,
        "first_name": None,
        "last_name": None,
        "srt_id": None,
        "active_status": None,
        "contributor_project_id": None,
        "project_id": None,
    }
    candidates: Dict[str, List[str]] = {k: [] for k in mapping.keys()}

    cols_norm = {c: _norm_col(c) for c in columns}

    rx_email = re.compile(r"\b(e-?mail|email|mail)\b", re.I)

    # Full name must be explicit to avoid false positives
    rx_full_name = re.compile(r"\b(full\s*name|name\s*\(full\)|contributor\s*name|worker\s*name|agent\s*name)\b", re.I)
    rx_first = re.compile(r"\b(first\s*name|given\s*name|fname)\b", re.I)
    rx_last = re.compile(r"\b(last\s*name|family\s*name|surname|lname)\b", re.I)

    # SRT: must contain srt + id, or compact forms; also accept "tool id" because often combined
    #rx_srt = re.compile(r"(?=.*\bsrt\b)(?=.*\bid\b)|\bsrtid\b|\bsrt\s*id\b|\bsrt\s*tool\b|\btool\s*id\b", re.I) # more conservative
    rx_srt = re.compile(r"\bsrt\s*id\b|\bsrtid\b|\b(srt)\b.*\b(id)\b|\btool\s*id\b", re.I) # more permissive



    # Status/state
    rx_status = re.compile(r"\b(status|state|activity|active\s*status|worker\s*status|contributor\s*status)\b", re.I)

    # IDs: accept explicit column names, but also rely on value-based detection via profiles
    rx_project_id = re.compile(r"\b(project\s*id|projectid)\b", re.I)
    rx_contributor_project_id = re.compile(r"\b(contributor\s*id|worker\s*id|agent\s*id|user\s*id|uid|cid)\b", re.I)

    for col, ncol in cols_norm.items():
        if rx_email.search(ncol):
            candidates["email"].append(col)

        if rx_full_name.search(ncol) and not PROJECT_NAME_GUARD_REGEX.search(ncol):
            candidates["full_name"].append(col)

        if rx_first.search(ncol):
            candidates["first_name"].append(col)

        if rx_last.search(ncol):
            candidates["last_name"].append(col)

        if rx_srt.search(ncol):
            candidates["srt_id"].append(col)

        if rx_status.search(ncol):
            candidates["active_status"].append(col)

        if rx_project_id.search(ncol):
            candidates["project_id"].append(col)

        if rx_contributor_project_id.search(ncol):
            candidates["contributor_project_id"].append(col)

    # Value-based candidates for a01/a02 even if headers are weird
    for col in columns:
        p = profiles.get(col, {})
        if float(p.get("a01_like", 0.0)) >= 0.20:
            if col not in candidates["project_id"]:
                candidates["project_id"].append(col)
        if float(p.get("a02_like", 0.0)) >= 0.20:
            if col not in candidates["contributor_project_id"]:
                candidates["contributor_project_id"].append(col)
    
    # Value-based candidates for SRT even if header matching fails (robust fallback)
    for col in columns:
        p = profiles.get(col, {})
        if float(p.get("srt_like", 0.0)) >= 0.20:
            if col not in candidates["srt_id"]:
                candidates["srt_id"].append(col)

    def pick_best(field: str, cols: List[str]) -> Optional[str]:
        if not cols:
            return None
        scored: List[Tuple[str, float]] = []
        for c in cols:
            p = profiles.get(c, {})
            score = 0.0

            if field == "email":
                score += float(p.get("looks_like_email", 0.0)) * 10.0
                if "email" in _norm_col(c):
                    score += 0.6

            elif field == "srt_id":
                score += float(p.get("srt_like", 0.0)) * 10.0
                if "srt" in _norm_col(c):
                    score += 0.6

            elif field == "full_name":
                # Prefer columns that look like personal names and are not constant
                score += float(p.get("looks_like_name", 0.0)) * 7.0
                score += (0.0 if p.get("mostly_constant", False) else 1.0)
                score += min(1.5, float(p.get("unique_ratio", 0.0)) * 2.0)
                if PROJECT_NAME_GUARD_REGEX.search(_norm_col(c)):
                    score -= 6.0

            elif field in ("first_name", "last_name"):
                score += float(p.get("looks_like_name", 0.0)) * 4.0
                score += (0.0 if p.get("mostly_constant", False) else 0.8)

            elif field == "active_status":
                # Status is usually short text with moderate variety
                score += (0.0 if float(p.get("numeric_ratio", 0.0)) > 0.2 else 1.0)
                score += (0.0 if p.get("mostly_constant", False) else 1.2)
                score += min(1.2, float(p.get("unique_ratio", 0.0)) * 1.5)
                if "status" in _norm_col(c) or "state" in _norm_col(c):
                    score += 0.6

            elif field == "project_id":
                score += float(p.get("a01_like", 0.0)) * 10.0
                if "project id" in _norm_col(c) or "projectid" in _norm_col(c):
                    score += 0.7

            elif field == "contributor_project_id":
                score += float(p.get("a02_like", 0.0)) * 10.0
                if "contributor id" in _norm_col(c) or "worker id" in _norm_col(c):
                    score += 0.5

            scored.append((c, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[0][0]

    for f in list(mapping.keys()):
        if f == "srt_id":
            # Delay: chosen in process_file (needs df to compute valid ratio)
            continue
        mapping[f] = pick_best(f, candidates[f])

    # Soft warnings for ambiguity on required-ish fields
    for f in ["email", "full_name", "srt_id"]:
        if len(candidates[f]) > 1:
            warnings.append(f"Ambiguous candidates for {f}: {candidates[f]}")
        if mapping[f] is None:
            warnings.append(f"{f} not mapped by golden set.")

    return mapping, candidates, warnings

def choose_best_srt_column(
    df: pd.DataFrame,
    candidates: List[str],
    profiles: Dict[str, Dict[str, Any]],
    logger: logging.Logger,
) -> Optional[str]:
    """
    Pick the best SRT column deterministically.
    Priority:
      1) Prefer headers explicitly named like 'SRT ID' if they yield valid SRTs.
      2) Otherwise choose the candidate with best valid extraction ratio.
      3) Penalize columns that look like notes/concat unless needed.
    """
    if not candidates:
        return None

    def header_priority(col: str) -> float:
        n = _norm_col(col)
        p = 0.0
        # Strong positives
        if re.search(r"\bsrt\s*id\b", n) or re.search(r"\bsrtid\b", n):
            p += 6.0
        if re.search(r"\bsrt\b", n) and re.search(r"\bid\b", n):
            p += 3.0
        if re.search(r"\btool\s*id\b", n):
            p += 1.0
        # Strong negatives
        if re.search(r"\bconcat\b", n):
            p -= 2.5
        if re.search(r"\b(notes?|qualification|qual|pivot)\b", n):
            p -= 3.5
        return p

    scored: List[Tuple[str, float, float]] = []  # (col, total_score, valid_ratio)
    for col in candidates:
        if col not in df.columns:
            continue

        s = df[col]
        non_empty = s[~s.isna()]
        if len(non_empty) == 0:
            valid_ratio = 0.0
        else:
            valid_hits = non_empty.apply(lambda x: extract_srt_id_from_cell(x) is not None).sum()
            valid_ratio = float(valid_hits) / float(len(non_empty))

        # Base score from valid_ratio and header cues
        score = (valid_ratio * 10.0) + header_priority(col)

        # Slight boost if profile already suggests srt_like
        score += float(profiles.get(col, {}).get("srt_like", 0.0)) * 2.0

        scored.append((col, score, valid_ratio))

    if not scored:
        return None

    scored.sort(key=lambda x: x[1], reverse=True)
    best_col, best_score, best_ratio = scored[0]

    logger.debug(f"SRT candidates scored: {scored[:8]}")

    # If we have an explicit 'SRT ID' column and it is good enough, force it
    explicit = [c for c in candidates if re.search(r"\bsrt\s*id\b|\bsrtid\b", _norm_col(c))]
    if explicit:
        explicit_best = explicit[0]
        # compute its ratio (may not be first in scored due to penalties)
        exp_ratio = None
        for c, _, r in scored:
            if c == explicit_best:
                exp_ratio = r
                break
        if exp_ratio is not None and exp_ratio >= 0.20:
            logger.debug(f"Forcing explicit SRT column '{explicit_best}' (valid_ratio={exp_ratio:.3f})")
            return explicit_best

    return best_col


# -------------------
# Status normalization
# -------------------

def sheet_default_status(sheet_name: str) -> str:
    """
    Infer a default status for all rows of a sheet, if no status column exists.
    """
    s = sheet_name or ""
    if STATUS_REMOVED_WORDS.search(s):
        return "removed"
    if STATUS_HOLD_WORDS.search(s):
        return "on_hold"
    if STATUS_INACTIVE_WORDS.search(s):
        return "inactive"
    if STATUS_ACTIVE_WORDS.search(s):
        return "active"
    if COMPLETE_WORDS.search(s) or ROSTER_WORD.search(s):
        return "active"
    return "active"


def normalize_status_value(v: Any) -> Optional[str]:
    """
    Normalize raw status cell values to:
      active / on_hold / removed / inactive
    """
    if pd.isna(v):
        return None
    t = str(v).strip().lower()
    if not t:
        return None

    if t in ("active", "current", "enabled", "live"):
        return "active"
    if t in ("inactive", "disabled"):
        return "inactive"
    if t in ("hold", "on hold", "on_hold", "paused", "pause", "suspended", "pending"):
        return "on_hold"
    if t in ("removed", "termed", "terminated", "offboard", "off-board", "archived", "deactivated"):
        return "removed"

    if STATUS_REMOVED_WORDS.search(t):
        return "removed"
    if STATUS_HOLD_WORDS.search(t):
        return "on_hold"
    if STATUS_INACTIVE_WORDS.search(t):
        return "inactive"
    if STATUS_ACTIVE_WORDS.search(t):
        return "active"

    return None


# -------------------
# Mapping validation
# -------------------

def validate_mapping(
    df: pd.DataFrame,
    mapping: Dict[str, Optional[str]],
    profiles: Dict[str, Dict[str, Any]],
) -> List[str]:
    """
    Soft validation warnings (never blocks extraction).
    """
    warnings: List[str] = []

    email_col = mapping.get("email")
    if email_col and email_col in df.columns:
        r = float(profiles.get(email_col, {}).get("looks_like_email", 0.0))
        if r < EMAIL_VALID_RATIO_MIN:
            warnings.append(f"Email mapping weak (looks_like_email={r:.2f}).")
    else:
        warnings.append("Missing email mapping.")

    name_col = mapping.get("full_name")
    if name_col and name_col in df.columns:
        r = float(profiles.get(name_col, {}).get("looks_like_name", 0.0))
        if r < NAME_VALID_RATIO_MIN:
            warnings.append(f"Full name mapping weak (looks_like_name={r:.2f}).")
        if profiles.get(name_col, {}).get("mostly_constant", False):
            warnings.append("Full name column appears mostly constant (suspicious).")
    else:
        # acceptable if first+last exist
        fn = mapping.get("first_name")
        ln = mapping.get("last_name")
        if not (fn and ln and fn in df.columns and ln in df.columns):
            warnings.append("Missing full_name mapping and cannot derive from first_name + last_name.")

    srt_col = mapping.get("srt_id")
    if srt_col and srt_col in df.columns:
        r = float(profiles.get(srt_col, {}).get("srt_like", 0.0))
        if r < SRT_VALID_RATIO_MIN:
            warnings.append(f"SRT mapping weak (srt_like={r:.2f}).")
    else:
        warnings.append("Missing srt_id mapping.")

    return warnings


# -------------------
# Extraction utilities
# -------------------

def make_unique_columns(cols: List[str]) -> List[str]:
    """
    Make column names unique by appending __dupN suffixes.
    Example: ["Email", "Email"] -> ["Email", "Email__dup2"]
    """
    seen: Dict[str, int] = {}
    out: List[str] = []
    for c in cols:
        base = str(c)
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__dup{seen[base]}")
    return out

def is_na_scalar(v: Any) -> bool:
    """
    Safe NA check that never returns an array/Series.
    Prevents: 'The truth value of a Series is ambiguous...'
    """
    if v is None:
        return True
    if is_scalar(v):
        return bool(pd.isna(v))
    return False


def _derive_full_name(df: pd.DataFrame, first_col: str, last_col: str) -> pd.Series:
    a = _safe_series_to_str(df[first_col])
    b = _safe_series_to_str(df[last_col])
    out = (a + " " + b).str.strip()
    out = out.replace({"": pd.NA})
    return out


def _extract_first_a01(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    t = str(v).strip()
    if not t:
        return None
    # Find token-like substrings that start with a01
    m = re.search(r"\b(a01[a-z0-9]+)\b", t, re.I)
    return m.group(1) if m else None


def _extract_first_a02(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    t = str(v).strip()
    if not t:
        return None
    m = re.search(r"\b(a02[a-z0-9]+)\b", t, re.I)
    return m.group(1) if m else None


def extract_roster(
    df: pd.DataFrame,
    mapping: Dict[str, Optional[str]],
    source_file: str,
    source_sheet: str,
    logger: logging.Logger,
) -> Tuple[pd.DataFrame, Dict[str, int], List[str]]:
    warnings: List[str] = []
    out = pd.DataFrame()

    # Email
    email_col = mapping.get("email")
    if email_col and email_col in df.columns:
        out["email"] = _safe_series_to_str(df[email_col]).str.lower().replace({"": pd.NA})
    else:
        out["email"] = pd.NA

    # First/Last name
    fn_col = mapping.get("first_name")
    ln_col = mapping.get("last_name")
    if fn_col and fn_col in df.columns:
        out["first_name"] = _safe_series_to_str(df[fn_col]).replace({"": pd.NA})
    else:
        out["first_name"] = pd.NA

    if ln_col and ln_col in df.columns:
        out["last_name"] = _safe_series_to_str(df[ln_col]).replace({"": pd.NA})
    else:
        out["last_name"] = pd.NA

    # Full name
    full_col = mapping.get("full_name")
    if full_col and full_col in df.columns:
        out["full_name"] = _safe_series_to_str(df[full_col]).replace({"": pd.NA})
    else:
        if fn_col and ln_col and fn_col in df.columns and ln_col in df.columns:
            out["full_name"] = _derive_full_name(df, fn_col, ln_col)
            warnings.append("Derived full_name from first_name + last_name.")
        else:
            out["full_name"] = pd.NA

    # SRT ID
    srt_col = mapping.get("srt_id")
    if srt_col and srt_col in df.columns:
        out["srt_id"] = df[srt_col].apply(extract_srt_id_from_cell).astype("string")
        out["srt_id"] = out["srt_id"].replace({"<NA>": pd.NA})
    else:
        out["srt_id"] = pd.NA

    # Contributor ID (a02...)
    cid_col = mapping.get("contributor_project_id")
    if cid_col and cid_col in df.columns:
        out["contributor_project_id"] = df[cid_col].apply(_extract_first_a02).astype("string")
        out["contributor_project_id"] = out["contributor_project_id"].replace({"<NA>": pd.NA})
    else:
        out["contributor_project_id"] = pd.NA

    # Project ID (a01...)
    pid_col = mapping.get("project_id")
    if pid_col and pid_col in df.columns:
        out["project_id"] = df[pid_col].apply(_extract_first_a01).astype("string")
        out["project_id"] = out["project_id"].replace({"<NA>": pd.NA})
    else:
        out["project_id"] = pd.NA

    # Active status
    status_col = mapping.get("active_status")
    default_status = sheet_default_status(source_sheet)
    if status_col and status_col in df.columns:
        out["active_status"] = df[status_col].apply(normalize_status_value)
        out["active_status"] = out["active_status"].fillna(default_status)
    else:
        out["active_status"] = default_status

    # Sources
    out["source_file"] = source_file
    out["source_sheet"] = source_sheet

    # Remove obvious header repeats in data
    out["email"] = out["email"].apply(lambda v: pd.NA if isinstance(v, str) and v.strip().lower() in ("email", "e-mail") else v)
    out["full_name"] = out["full_name"].apply(lambda v: pd.NA if isinstance(v, str) and v.strip().lower() in ("name", "full name") else v)

    # Keep row if any key identifier is present
    keep_mask = pd.Series([False] * len(out))
    for k in ROW_KEEP_IF_ANY_PRESENT:
        if k in out.columns:
            keep_mask = keep_mask | out[k].notna()
    out = out.loc[keep_mask].copy()

    stats = {
        "rows_in": int(len(df)),
        "rows_out": int(len(out)),
        "emails_nonnull": int(out["email"].notna().sum()),
        "srt_nonnull": int(out["srt_id"].notna().sum()),
        "a02_nonnull": int(out["contributor_project_id"].notna().sum()),
        "a01_nonnull": int(out["project_id"].notna().sum()),
    }

    if stats["rows_out"] == 0:
        warnings.append("Extracted 0 rows after filtering.")
    if stats["srt_nonnull"] == 0:
        warnings.append("No valid SRT IDs extracted.")

    logger.debug(
        f"{source_file} | {source_sheet}: extracted rows_out={stats['rows_out']} "
        f"emails={stats['emails_nonnull']} srt={stats['srt_nonnull']} a02={stats['a02_nonnull']} a01={stats['a01_nonnull']}"
    )

    # Ensure only canonical output columns exist and in the right order
    for c in CANONICAL_FIELDS:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[CANONICAL_FIELDS].copy()

    return out, stats, warnings


# ------------------------
# Deduplication preference
# ------------------------

def deduplicate_roster(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Deduplicate with preference:
    - keep "active" rows over non-active when duplicates exist
    - primary key: email (if present)
    - secondary key: srt_id
    - tertiary key: contributor_project_id
    """
    before = len(df)

    # Rank statuses (lower is better)
    rank = {"active": 0, "on_hold": 1, "inactive": 2, "removed": 3}
    if "active_status" in df.columns:
        df["_status_rank"] = df["active_status"].map(lambda x: rank.get(str(x), 9))
        df = df.sort_values(by=["_status_rank"]).drop(columns=["_status_rank"])

    if "email" in df.columns and df["email"].notna().any():
        df = df.drop_duplicates(subset=["email"], keep="first")
    elif "srt_id" in df.columns and df["srt_id"].notna().any():
        df = df.drop_duplicates(subset=["srt_id"], keep="first")
    elif "contributor_project_id" in df.columns and df["contributor_project_id"].notna().any():
        df = df.drop_duplicates(subset=["contributor_project_id"], keep="first")

    after = len(df)
    if after != before:
        logger.info(f"Deduplicated roster: {before} -> {after}")
    return df


# -------------------
# File-level pipeline
# -------------------

def process_file(
    file_path: Path,
    include_xls: bool,
    logger: logging.Logger,
) -> Tuple[pd.DataFrame, List[FileReport]]:
    reports: List[FileReport] = []
    all_rows: List[pd.DataFrame] = []

    xls, non_empty_sheets, wb_warnings = read_workbook_sheets(file_path, include_xls, logger)
    if xls is None or not non_empty_sheets:
        rep = FileReport(
            file=file_path.name,
            strategy="open_failed_or_no_sheets",
            chosen_sheets="",
            header_row_idx=None,
            rows_extracted=0,
            mapping_json="{}",
            warnings=" | ".join(wb_warnings),
            errors="",
            top_sheet_scores_json="[]",
        )
        reports.append(rep)
        return pd.DataFrame(columns=CANONICAL_FIELDS), reports

    choice = choose_roster_sheets(file_path, non_empty_sheets, logger)
    logger.info(f"{file_path.name}: strategy={choice.strategy} chosen_sheets={choice.chosen_sheets}")

    per_file_errors: List[str] = []
    per_file_warnings: List[str] = []
    per_file_warnings.extend(wb_warnings)
    if choice.warnings:
        per_file_warnings.extend(choice.warnings)

    header_idx_any: Optional[int] = None
    mapping_final: Dict[str, Optional[str]] = {
        "email": None,
        "full_name": None,
        "first_name": None,
        "last_name": None,
        "srt_id": None,
        "active_status": None,
        "contributor_project_id": None,
        "project_id": None,
    }

    extracted_total = 0

    for sh in choice.chosen_sheets:
        try:
            df, header_idx, hdr_warnings = detect_header_and_build_df(file_path, sh, logger)
            if header_idx_any is None:
                header_idx_any = header_idx
            per_file_warnings.extend(hdr_warnings)

            if df.empty or df.shape[1] == 0:
                per_file_warnings.append(f"Sheet '{sh}' produced empty dataframe after header parsing.")
                continue

            prof = profile_columns(df)
            mapping, candidates, map_warnings = golden_map_columns(list(df.columns), prof)

            # Deterministically resolve SRT column using real extraction quality
            srt_cands = candidates.get("srt_id", []) if isinstance(candidates, dict) else []
            best_srt = choose_best_srt_column(df, srt_cands, prof, logger)
            mapping["srt_id"] = best_srt
            if best_srt is None:
                per_file_warnings.append("srt_id not mapped by golden set.")

            logger.debug(f"{file_path.name} | {sh}: columns={list(df.columns)}")
            logger.debug(f"{file_path.name} | {sh}: srt candidates={candidates.get('srt_id')}")
            if df.shape[1] > 0:
                srt_scores = []
                for c in df.columns:
                    p = prof.get(c, {})
                    srt_scores.append((c, round(float(p.get("srt_like", 0.0)), 3), (p.get("sample") or [])[:3]))
                srt_scores.sort(key=lambda x: x[1], reverse=True)
                logger.debug(f"{file_path.name} | {sh}: top srt_like columns={srt_scores[:8]}")
            
            per_file_warnings.extend(map_warnings)

            val_warnings = validate_mapping(df, mapping, prof)
            per_file_warnings.extend(val_warnings)

            logger.info(
                'FILE_MAPPING | file="%s" | sheet="%s" | '
                'email="%s" | full_name="%s" | first_name="%s" | last_name="%s" | '
                'srt_id="%s" | active_status="%s" | contributor_project_id="%s" | project_id="%s"',
                file_path.name,
                sh,
                mapping.get("email"),
                mapping.get("full_name"),
                mapping.get("first_name"),
                mapping.get("last_name"),
                mapping.get("srt_id"),
                mapping.get("active_status"),
                mapping.get("contributor_project_id"),
                mapping.get("project_id"),
            )

            logger.debug(f"{file_path.name} | {sh}: columns={list(df.columns)}")
            logger.debug(f"{file_path.name} | {sh}: srt candidates={candidates.get('srt_id')}")

            out_df, stats, ex_warnings = extract_roster(
                df=df,
                mapping=mapping,
                source_file=file_path.name,
                source_sheet=sh,
                logger=logger,
            )
            per_file_warnings.extend(ex_warnings)

            extracted_total += int(stats["rows_out"])
            all_rows.append(out_df)

            # capture first non-null mapping as the "file mapping summary"
            for k in mapping_final.keys():
                if mapping_final.get(k) is None and mapping.get(k) is not None:
                    mapping_final[k] = mapping.get(k)

        except Exception as e:
            logger.exception(f"{file_path.name} | {sh}: extraction failed")
            per_file_errors.append(f"Sheet '{sh}' failed: {e}")

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        combined = deduplicate_roster(combined, logger)
    else:
        combined = pd.DataFrame(columns=CANONICAL_FIELDS)

    top_scores_json = "[]"
    if choice.sheet_scores:
        try:
            top_scores_json = json.dumps(choice.sheet_scores, ensure_ascii=False)
        except Exception:
            top_scores_json = str(choice.sheet_scores)

    try:
        mapping_json = json.dumps(mapping_final, ensure_ascii=False)
    except Exception:
        mapping_json = str(mapping_final)

    rep = FileReport(
        file=file_path.name,
        strategy=choice.strategy,
        chosen_sheets=";".join(choice.chosen_sheets),
        header_row_idx=header_idx_any,
        rows_extracted=int(len(combined)),
        mapping_json=mapping_json,
        warnings=" | ".join(per_file_warnings),
        errors=" | ".join(per_file_errors),
        top_sheet_scores_json=top_scores_json,
    )
    reports.append(rep)
    return combined, reports


# -----
# Main
# -----

def find_input_files(folder: Path, include_xls: bool) -> List[Path]:
    exts = [".xlsx", ".xlsm"]
    if include_xls:
        exts.append(".xls")
    files: List[Path] = []
    for p in sorted(folder.glob("*")):
        if p.is_file() and p.suffix.lower() in exts and not p.name.startswith("~$"):
            files.append(p)
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic consolidation of contributor rosters from Excel files.")
    parser.add_argument("--folder", default="rosters_input", help="Input folder containing .xlsx/.xlsm rosters (default: rosters_input)")
    parser.add_argument("--include-xls", action="store_true", help="Include .xls files (best-effort: will warn/skip unless you add support)")
    parser.add_argument("--out-roster", default="_consolidated_roster.csv", help="Output consolidated roster CSV")
    parser.add_argument("--out-report", default="_roster_extraction_report.csv", help="Output extraction report CSV")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging")
    parser.add_argument("--out-folder", default="out_per_file", help="Output folder for per-workbook roster CSVs")
    args = parser.parse_args()

    logger = setup_logging(args.debug)

    folder = Path(args.folder)
    if not folder.exists():
        logger.error(f"Input folder not found: {folder.resolve()}")
        sys.exit(2)
    
    out_folder = Path(args.out_folder)
    out_folder.mkdir(parents=True, exist_ok=True)

    files = find_input_files(folder, args.include_xls)
    if not files:
        logger.warning(f"No input files found in {folder.resolve()} (expected .xlsx/.xlsm)")
        pd.DataFrame(columns=CANONICAL_FIELDS).to_csv(args.out_roster, index=False, encoding="utf-8-sig")
        rep = FileReport(
            file="",
            strategy="no_files",
            chosen_sheets="",
            header_row_idx=None,
            rows_extracted=0,
            mapping_json="{}",
            warnings="No files found.",
            errors="",
            top_sheet_scores_json="[]",
        )
        pd.DataFrame([asdict(rep)]).to_csv(args.out_report, index=False, encoding="utf-8-sig")
        return

    logger.info(f"Found {len(files)} file(s) in {folder.resolve()}")

    report_rows: List[Dict[str, Any]] = []

    for fp in files:
        logger.info(f"Processing file: {fp.name}")
        df_out, reps = process_file(file_path=fp, include_xls=args.include_xls, logger=logger)

        # Write per-workbook CSV
        safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "_", fp.stem).strip("_")
        out_path = out_folder / f"{safe_stem}__roster.csv"

        # Ensure only canonical columns (no extras) and stable order
        if df_out is None or df_out.empty:
            pd.DataFrame(columns=CANONICAL_FIELDS).to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.warning(f"{fp.name}: no rows extracted -> wrote empty {out_path.resolve()}")
        else:
            df_out = deduplicate_roster(df_out, logger)

            for c in CANONICAL_FIELDS:
                if c not in df_out.columns:
                    df_out[c] = pd.NA
            df_out = df_out[CANONICAL_FIELDS].copy()

            df_out.to_csv(out_path, index=False, encoding="utf-8-sig")
            logger.info(f"{fp.name}: wrote per-file roster {out_path.resolve()} rows={len(df_out)}")

        # Collect report rows
        for r in reps:
            report_rows.append(asdict(r))

    rep_df = pd.DataFrame(report_rows)
    if rep_df.empty:
        rep_df = pd.DataFrame(columns=[f.name for f in FileReport.__dataclass_fields__.values()])
    rep_df.to_csv(args.out_report, index=False, encoding="utf-8-sig")
    logger.info(f"Wrote extraction report: {Path(args.out_report).resolve()} rows={len(rep_df)}")


if __name__ == "__main__":
    main()
