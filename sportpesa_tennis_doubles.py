# sportpesa_tennis_doubles.py
from pathlib import Path
from sportpesa_common import scrape_sportpesa_tennis, save_to_excel, save_to_sqlite

OUT_XLSX = Path("sportpesa_tennis_doubles.xlsx")
OUT_SQLITE = Path("sportpesa_tennis_doubles.db")
TABLE_NAME = "tennis_doubles_odds"

def main():
    print("🔎 Scraping SportPesa Tennis (Doubles)...")
    rows = scrape_sportpesa_tennis(want_doubles=True, include_highlights=True, include_live=True)
    if not rows:
        print("⚠️ No doubles odds parsed.")
        return
    save_to_excel(rows, OUT_XLSX)
    save_to_sqlite(rows, OUT_SQLITE, TABLE_NAME)
    print(f"✅ Saved {len(rows)} rows. Done.")

if __name__ == "__main__":
    main()
