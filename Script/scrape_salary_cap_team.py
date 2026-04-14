#!/usr/bin/env python3
"""
scrape_salary_cap_team.py

Scrape herhoopstats WNBA team salary sheet and output only:
 - Player name (première colonne)
 - La colonne correspondant à l'année fournie (ex: "2026" si --year 2025)
 - La colonne "Core Years" (ou la dernière colonne si non trouvée)

Usage examples:
    python scrape_salary_cap_team.py --team "New York Liberty" --year 2025 --output liberty_2025.txt
    python scrape_salary_cap_team.py --team liberty --year 2024 --output liberty_2024.json --selenium
"""
from __future__ import annotations
import argparse
import json
import re
import time
from typing import List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd

# Detect available BS parser
try:
    import lxml  # type: ignore
    BS_PARSER = "lxml"
except Exception:
    try:
        import html5lib  # type: ignore
        BS_PARSER = "html5lib"
    except Exception:
        BS_PARSER = "html.parser"

# Optional selenium imports (only used if requested)
SELENIUM_AVAILABLE = False
try:
    from selenium import webdriver  # type: ignore
    from selenium.webdriver.chrome.options import Options  # type: ignore
    from webdriver_manager.chrome import ChromeDriverManager  # type: ignore
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

BASE = "https://herhoopstats.com"

# Regex to remove "This HTML5 audio player..." and everything after it in a cell
_AUDIO_RE = re.compile(r'\s*This HTML5 audio player.*', flags=re.IGNORECASE | re.DOTALL)
# Regex to match plain numeric currency strings like "$270,000" or "$0"
_CURRENCY_RE = re.compile(r'^\s*\$?\s*([0-9][0-9,]*)\s*$')


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/115.0 Safari/537.36 herhoopstats-scraper/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def fetch_url(url: str, session: requests.Session, use_selenium: bool = False, wait: float = 1.0) -> str:
    """
    Fetch URL contents. If use_selenium True, use selenium to render JS.
    """
    if use_selenium:
        if not SELENIUM_AVAILABLE:
            raise RuntimeError("Selenium/webdriver-manager not available. Install selenium and webdriver-manager.")
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        try:
            driver.get(url)
            time.sleep(wait)
            html = driver.page_source
        finally:
            driver.quit()
        return html
    else:
        r = session.get(url, timeout=30)
        r.raise_for_status()
        return r.text


def find_team_link(summary_html: str, team_query: str, year: Optional[str] = None) -> Optional[str]:
    """
    Parse the summary page and find the best matching link for the team.
    """
    soup = BeautifulSoup(summary_html, BS_PARSER)
    anchors = soup.find_all("a", href=True)
    team_query_low = team_query.strip().lower()

    candidates = []
    for a in anchors:
        text = (a.get_text(" ", strip=True) or "").lower()
        href = a["href"]
        href_low = href.lower()
        parent_text = (a.parent.get_text(" ", strip=True) or "").lower()
        if team_query_low in text or team_query_low in href_low:
            score = 0
            if team_query_low in text:
                score += 2
            if team_query_low in href_low:
                score += 1
            if year and (year in text or year in href_low or year in parent_text):
                score += 2
            candidates.append((score, a))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], len(x[1]["href"])), reverse=True)
    best = candidates[0][1]
    return best["href"]


def extract_salary_tables(page_html: str) -> List[pd.DataFrame]:
    """
    Try pandas.read_html first. If it fails, fallback to BeautifulSoup parsing.
    """
    try:
        dfs = pd.read_html(page_html)
        if dfs:
            return dfs
    except Exception:
        pass

    soup = BeautifulSoup(page_html, BS_PARSER)
    bs_tables = soup.find_all("table")
    tables: List[pd.DataFrame] = []
    for tbl in bs_tables:
        headers: List[str] = []
        thead = tbl.find("thead")
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        rows = []
        for tr in tbl.find_all("tr"):
            cols = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            if cols:
                rows.append(cols)
        if rows:
            df = pd.DataFrame(rows)
            if headers and len(headers) == df.shape[1]:
                df.columns = headers
            tables.append(df)
    return tables


def normalize_team_slug(team: str) -> str:
    import re as _re
    s = team.lower()
    s = _re.sub(r"[^a-z0-9]+", " ", s)
    s = "_".join(s.split())
    return s


def _clean_cell(x):
    """
    Remove audio JS text and collapse whitespace.
    """
    if isinstance(x, str):
        x = _AUDIO_RE.sub("", x)
        x = re.sub(r"\s+", " ", x).strip()
    return x


def _parse_currency_cell(x):
    """
    Convert currency-like strings to integers if possible.
    """
    if isinstance(x, str):
        m = _CURRENCY_RE.match(x)
        if m:
            num = m.group(1).replace(",", "")
            try:
                return int(num)
            except ValueError:
                return x
    return x


def _is_summary_row(name_value: Optional[str]) -> bool:
    """
    Heuristic to detect summary/footer rows to drop:
    - contains keywords like 'total', 'team totals', 'upcoming', 'draft', 'key cba', 'unsigned', 'salary cap', ...
    """
    if not isinstance(name_value, str):
        return False
    v = name_value.strip().lower()
    if not v:
        return True
    keywords = ["total", "team totals", "upcoming", "draft", "key cba", "unsigned", "salary cap", "player minimum", "all values shown"]
    for k in keywords:
        if k in v:
            return True
    return False


def scrape_team_salary(team: str, year: Optional[int], use_selenium: bool = False, output: Optional[str] = None):
    session = make_session()
    summary_url = urljoin(BASE, "/salary-cap-sheet/wnba/summary/")
    print(f"Fetching summary page: {summary_url}")
    summary_html = fetch_url(summary_url, session, use_selenium=use_selenium)

    print(f"Looking for team link matching '{team}' (year={year})...")
    href = find_team_link(summary_html, team, str(year) if year else None)
    if not href:
        slug = normalize_team_slug(team)
        if slug and slug in summary_html.lower():
            soup = BeautifulSoup(summary_html, BS_PARSER)
            anchors = soup.find_all("a", href=True)
            for a in anchors:
                if slug in a["href"].lower() or slug in a.get_text(" ", strip=True).lower():
                    href = a["href"]
                    break
    if not href:
        raise RuntimeError("Impossible de trouver un lien pour l'équipe fournie sur la page summary. Vérifie l'orthographe ou active --selenium si la page est JS-rendered.")

    team_url = href if urlparse(href).netloc else urljoin(BASE, href)
    print(f"Found team page: {team_url}")
    page_html = fetch_url(team_url, session, use_selenium=use_selenium)

    print("Extracting tables from team page...")
    dfs = extract_salary_tables(page_html)
    if not dfs:
        raise RuntimeError("Aucune table trouvée sur la page équipe. La structure HTML a peut-être changé ou la page est entièrement JS.")

    # Heuristic: pick the largest table (most rows)
    dfs_sorted = sorted(dfs, key=lambda d: d.shape[0], reverse=True)
    salary_df = dfs_sorted[0].copy()

    # Fix headers if pandas put them as first row
    try:
        unnamed_mask = salary_df.columns.astype(str).str.startswith("Unnamed")
        if unnamed_mask.any():
            first_row = salary_df.iloc[0]
            salary_df = salary_df[1:].copy()
            salary_df.columns = first_row.values
    except Exception:
        pass

    # Clean and parse currency
    salary_df = salary_df.applymap(_clean_cell)
    salary_df = salary_df.applymap(_parse_currency_cell)

    # Remove rows starting at "Key CBA Numbers" if present
    drop_pos = None
    target_lower = "key cba numbers"
    for idx, row in enumerate(salary_df.values.tolist()):
        for cell in row:
            if isinstance(cell, str) and target_lower in cell.lower():
                drop_pos = idx
                break
        if drop_pos is not None:
            break
    if drop_pos is not None:
        salary_df = salary_df.iloc[:drop_pos].reset_index(drop=True)

    # Identify columns to keep:
    # - first column (name)
    # - year column: try exact match of year+1 (see explanation below), else match column containing the year string
    # - core years column: try to find "Core Years" case-insensitive, else use last column
    name_col = salary_df.columns[0]
    if year is None:
        raise RuntimeError("Tu dois fournir --year pour sélectionner la colonne correspondant à l'année cible.")
    # As requested: the column to keep is the year after the provided --year.
    # Example: user passed --year 2025 -> they want to keep column "2026".
    target_year = str(year + 1)
    year_col = None
    # exact match
    for col in salary_df.columns:
        if str(col).strip() == target_year:
            year_col = col
            break
    # fallback: column label contains the target_year
    if year_col is None:
        for col in salary_df.columns:
            if target_year in str(col):
                year_col = col
                break
    if year_col is None:
        raise RuntimeError(f"Colonne pour l'année {target_year} introuvable dans la table (colonnes disponibles: {list(map(str, salary_df.columns))}).")

    # find core years column
    core_col = None
    for col in salary_df.columns:
        if isinstance(col, str) and col.strip().lower() == "core years":
            core_col = col
            break
    if core_col is None:
        # fallback to last column
        core_col = salary_df.columns[-1]

    # Build reduced dataframe
    reduced = salary_df[[name_col, year_col, core_col]].copy()
    reduced.columns = ["Player", target_year, "Core Years"]

    # Drop summary/footer rows heuristically
    keep_mask = reduced["Player"].apply(lambda v: not _is_summary_row(v))
    reduced = reduced[keep_mask].reset_index(drop=True)

    # Drop rows where Player is empty or numeric-only
    reduced = reduced[reduced["Player"].notna()]
    reduced = reduced[~reduced["Player"].astype(str).str.strip().eq("")].reset_index(drop=True)

    # Convert to records for JSON output
    records = reduced.to_dict(orient="records")

    # Save output if requested
    if output:
        out_lower = output.lower()
        if out_lower.endswith(".csv") or out_lower.endswith(".txt"):
            reduced.to_csv(output, index=False)
            print(f"Saved reduced table as CSV: {output}")
        elif out_lower.endswith(".json"):
            with open(output, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"Saved reduced table as JSON: {output}")
        else:
            reduced.to_csv(output, index=False)
            print(f"Saved reduced table as CSV: {output}")

    return {
        "team_url": team_url,
        "salary_table": reduced,
        "records": records,
    }


def main():
    parser = argparse.ArgumentParser(description="Scrape herhoopstats WNBA team salary sheet.")
    parser.add_argument("--team", required=True, help="Team name or slug (e.g. 'New York Liberty' or 'liberty').")
    parser.add_argument("--year", type=int, required=True, help="Season year to target (the script will keep the column year+1).")
    parser.add_argument("--output", required=False, help="Output filename (.csv, .txt or .json).")
    parser.add_argument("--selenium", action="store_true", help="Use Selenium (headless Chrome) to render JS if needed.")
    args = parser.parse_args()

    try:
        result = scrape_team_salary(args.team, args.year-1, use_selenium=args.selenium, output=args.output)
        df = result["salary_table"]
        print("\nPreview (first 50 rows):")
        with pd.option_context("display.max_colwidth", 120):
            print(df.head(50).to_string(index=False))
    except Exception as e:
        print("Erreur:", str(e))
        raise


if __name__ == "__main__":
    main()