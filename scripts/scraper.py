"""
╔══════════════════════════════════════════════════════════════╗
║     ZAMA — HAITIAN CREOLE DATA COLLECTION PIPELINE           ║
║     Koleksyon Done Kreyòl Ayisyen pou LLM Training           ║
║     Version 2.0 — GitHub Actions Ready                       ║
╚══════════════════════════════════════════════════════════════╝
"""

import re
import json
import time
import logging
import hashlib
import requests
import pandas as pd
import wikipediaapi

from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from fake_useragent import UserAgent
from datasets import Dataset

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)
(OUTPUT_DIR / "raw").mkdir(exist_ok=True)
(OUTPUT_DIR / "cleaned").mkdir(exist_ok=True)
(OUTPUT_DIR / "logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "logs" / "scraper.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

try:
    ua = UserAgent()
    USER_AGENT = ua.random
except Exception:
    USER_AGENT = "Mozilla/5.0 (compatible; ZamaBot/1.0; +https://github.com/zama-data-kreyol)"

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "fr-HT, ht, fr",
}
DELAY = 1.5

# ─────────────────────────────────────────────
# DETEKSYON LANG
# ─────────────────────────────────────────────

CREOLE_WORDS = {
    "mwen", "ou", "li", "nou", "yo", "se", "pa", "ak",
    "nan", "pou", "ki", "sa", "gen", "te", "ap", "la",
    "men", "tout", "pi", "ka", "fè", "kap", "sou", "depi",
    "lè", "si", "tou", "jan", "wi", "non", "isit", "kote",
    "kouman", "konsa", "anpil", "toujou", "jwenn", "di",
    "ale", "vini", "pran", "bay", "wè", "ayiti", "ayisyen",
    "kreyol", "kreyòl", "peyi", "manje", "travay", "fanmi"
}

def detect_language(text: str) -> str:
    words = re.findall(r'\b\w+\b', text.lower()[:500])
    if not words:
        return "fr"
    ratio = sum(1 for w in words if w in CREOLE_WORDS) / len(words)
    return "ht" if ratio > 0.08 else "fr"


# ─────────────────────────────────────────────
# UTILITE
# ─────────────────────────────────────────────

def get_page(url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            time.sleep(DELAY)
            return BeautifulSoup(resp.text, "lxml")
        except Exception as e:
            log.warning(f"Tentativ {attempt+1}/{retries} echwe: {url} → {e}")
            time.sleep(3)
    return None

def clean_text(text: str) -> str:
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'http\S+', '', text)
    return text.strip()

def generate_id(url: str, text: str) -> str:
    return hashlib.md5(f"{url}{text[:50]}".encode()).hexdigest()[:12]

def save_batch(records: list, source_name: str):
    if not records:
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUTPUT_DIR / "raw" / f"{source_name}_{ts}.jsonl"
    with open(out, "a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    log.info(f"✅ {len(records)} tèks sovgade → {out.name}")


# ─────────────────────────────────────────────
# SCRAPER 1 — WIKIPEDIA KREYÒL
# ─────────────────────────────────────────────

class WikipediaCreoleScraper:

    def download_full_dump(self):
        log.info("📥 Download Wikipedia Kreyòl dump...")
        url = "https://dumps.wikimedia.org/htwiki/latest/htwiki-latest-pages-articles.xml.bz2"
        out = OUTPUT_DIR / "raw" / "wikipedia_ht_dump.xml.bz2"
        if out.exists():
            log.info("Dump deja egziste, skip.")
            return
        resp = requests.get(url, stream=True)
        total = int(resp.headers.get("content-length", 0))
        with open(out, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc="Wikipedia") as pbar:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
                pbar.update(len(chunk))
        log.info(f"✅ Wikipedia dump → {out}")

    def run(self, max_articles: int = 500):
        log.info("🌐 Scraping Wikipedia Kreyòl...")
        wiki = wikipediaapi.Wikipedia(
            language="ht",
            extract_format=wikipediaapi.ExtractFormat.WIKI,
            user_agent="ZamaBot/1.0"
        )
        seeds = [
            "Ayiti", "Pòtoprens", "Kreyòl ayisyen", "Jean-Jacques Dessalines",
            "Toussaint Louverture", "Edikasyon", "Lasante", "Agrikilti",
            "Kilti Ayiti", "Ekonomi Ayiti", "Jewografi Ayiti", "Istorik Ayiti",
            "Pòl Ogis", "Anivèsè", "Revolisyon Ayisyen", "Kafou"
        ]
        visited, queue, records = set(), list(seeds), []

        with tqdm(total=max_articles, desc="Wikipedia") as pbar:
            while queue and len(visited) < max_articles:
                title = queue.pop(0)
                if title in visited:
                    continue
                visited.add(title)
                page = wiki.page(title)
                if not page.exists():
                    continue
                text = clean_text(page.text)
                if len(text) < 50:
                    continue
                records.append({
                    "id": generate_id(page.fullurl, text),
                    "source": "wikipedia_ht",
                    "url": page.fullurl,
                    "title": title,
                    "text": text,
                    "language": "ht",
                    "scraped_at": datetime.now().isoformat()
                })
                queue += [t for t in list(page.links.keys())[:5] if t not in visited]
                pbar.update(1)
                if len(records) % 100 == 0:
                    save_batch(records, "wikipedia_ht")
                    records = []
                time.sleep(0.3)

        if records:
            save_batch(records, "wikipedia_ht")
        log.info(f"✅ Wikipedia: {len(visited)} paj")


# ─────────────────────────────────────────────
# SCRAPER 2 — LE NOUVELLISTE
# ─────────────────────────────────────────────

class LeNouvellisteScraper:
    BASE = "https://lenouvelliste.com"

    def scrape(self, url: str):
        soup = get_page(url)
        if not soup:
            return None
        try:
            title = soup.find("h1")
            title = title.get_text(strip=True) if title else ""
            body = soup.find("div", class_=re.compile("article|content|body", re.I)) or soup.find("article")
            if not body:
                return None
            text = clean_text(" ".join(p.get_text(strip=True) for p in body.find_all("p")))
            if len(text) < 100:
                return None
            return {"id": generate_id(url, text), "source": "le_nouvelliste",
                    "url": url, "title": title, "text": text,
                    "language": detect_language(text), "scraped_at": datetime.now().isoformat()}
        except Exception as e:
            log.error(f"Erè Nouvelliste {url}: {e}")
            return None

    def get_links(self, max_pages: int) -> list:
        links = set()
        for p in range(1, max_pages + 1):
            soup = get_page(f"{self.BASE}/page/{p}")
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "/article/" in h:
                    links.add(h if h.startswith("http") else self.BASE + h)
        log.info(f"Nouvelliste: {len(links)} lyen")
        return list(links)

    def run(self, max_pages: int = 20):
        log.info("🗞️ Scraping Le Nouvelliste...")
        records = []
        for url in tqdm(self.get_links(max_pages), desc="Nouvelliste"):
            rec = self.scrape(url)
            if rec:
                records.append(rec)
            if len(records) % 50 == 0 and records:
                save_batch(records, "le_nouvelliste")
                records = []
        if records:
            save_batch(records, "le_nouvelliste")
        log.info("✅ Le Nouvelliste fini!")


# ─────────────────────────────────────────────
# SCRAPER 3 — ALTERPRESSE
# ─────────────────────────────────────────────

class AlterPresseScraper:
    BASE = "https://www.alterpresse.org"

    def scrape(self, url: str):
        soup = get_page(url)
        if not soup:
            return None
        try:
            title = soup.find("h1") or soup.find("h2")
            title = title.get_text(strip=True) if title else ""
            text = clean_text(" ".join(
                p.get_text(strip=True) for p in soup.find_all("p")
                if len(p.get_text(strip=True)) > 30
            ))
            if len(text) < 100:
                return None
            return {"id": generate_id(url, text), "source": "alterpresse",
                    "url": url, "title": title, "text": text,
                    "language": detect_language(text), "scraped_at": datetime.now().isoformat()}
        except Exception as e:
            log.error(f"Erè AlterPresse {url}: {e}")
            return None

    def get_links(self, max_pages: int) -> list:
        links = set()
        for p in range(1, max_pages + 1):
            soup = get_page(f"{self.BASE}/spip.php?page=sommaire&debut_breves={p * 10}")
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "spip.php?article" in h or "/article" in h:
                    links.add(h if h.startswith("http") else self.BASE + h)
        log.info(f"AlterPresse: {len(links)} lyen")
        return list(links)

    def run(self, max_pages: int = 30):
        log.info("📰 Scraping AlterPresse...")
        records = []
        for url in tqdm(self.get_links(max_pages), desc="AlterPresse"):
            rec = self.scrape(url)
            if rec:
                records.append(rec)
            if len(records) % 50 == 0 and records:
                save_batch(records, "alterpresse")
                records = []
        if records:
            save_batch(records, "alterpresse")
        log.info("✅ AlterPresse fini!")


# ─────────────────────────────────────────────
# SCRAPER 4 — REZO NODWÈS
# ─────────────────────────────────────────────

class RezoNodwesScraper:
    BASE = "https://rezonodwes.com"

    def scrape(self, url: str):
        soup = get_page(url)
        if not soup:
            return None
        try:
            title = soup.find("h1")
            title = title.get_text(strip=True) if title else ""
            body = soup.find("div", class_=re.compile("entry|content|post", re.I))
            if not body:
                return None
            text = clean_text(body.get_text(separator=" ", strip=True))
            if len(text) < 100:
                return None
            return {"id": generate_id(url, text), "source": "rezo_nodwes",
                    "url": url, "title": title, "text": text,
                    "language": detect_language(text), "scraped_at": datetime.now().isoformat()}
        except Exception as e:
            log.error(f"Erè RezoNodwes {url}: {e}")
            return None

    def get_links(self, max_pages: int) -> list:
        links = set()
        for p in range(1, max_pages + 1):
            soup = get_page(f"{self.BASE}/page/{p}/")
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if self.BASE in h and len(h.split("/")) > 4:
                    links.add(h)
        return list(links)

    def run(self, max_pages: int = 15):
        log.info("📻 Scraping Rezo Nodwès...")
        records = [r for url in tqdm(self.get_links(max_pages), desc="RezoNodwes")
                   if (r := self.scrape(url)) is not None]
        save_batch(records, "rezo_nodwes")
        log.info("✅ Rezo Nodwès fini!")


# ─────────────────────────────────────────────
# SCRAPER 5 — HAITI LIBRE
# ─────────────────────────────────────────────

class HaitiLibreScraper:
    BASE = "https://www.haitilibre.com"

    def scrape(self, url: str):
        soup = get_page(url)
        if not soup:
            return None
        try:
            title = soup.find("h1") or soup.find("h2")
            title = title.get_text(strip=True) if title else ""
            body = soup.find("div", class_=re.compile("article|news|content", re.I))
            if not body:
                return None
            text = clean_text(" ".join(p.get_text(strip=True) for p in body.find_all("p")))
            if len(text) < 100:
                return None
            return {"id": generate_id(url, text), "source": "haiti_libre",
                    "url": url, "title": title, "text": text,
                    "language": detect_language(text), "scraped_at": datetime.now().isoformat()}
        except Exception as e:
            log.error(f"Erè HaitiLibre {url}: {e}")
            return None

    def get_links(self, max_pages: int) -> list:
        links = set()
        for p in range(1, max_pages + 1):
            soup = get_page(f"{self.BASE}/haiti-news-{p}.html")
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if "haiti-flash" in h or "haiti-news" in h:
                    links.add(h if h.startswith("http") else self.BASE + h)
        log.info(f"HaitiLibre: {len(links)} lyen")
        return list(links)

    def run(self, max_pages: int = 20):
        log.info("📄 Scraping Haiti Libre...")
        records = [r for url in tqdm(self.get_links(max_pages), desc="HaitiLibre")
                   if (r := self.scrape(url)) is not None]
        save_batch(records, "haiti_libre")
        log.info("✅ Haiti Libre fini!")


# ─────────────────────────────────────────────
# SCRAPER 6 — BIB LA AN KREYÒL
# ─────────────────────────────────────────────

class BibleCreoleScraper:

    def run(self):
        log.info("📖 Telechaje Bib la an Kreyòl...")
        url = "https://raw.githubusercontent.com/christos-c/bible-corpus/master/bibles/Haitian_Creole.xml"
        records = []
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            from xml.etree import ElementTree as ET
            root = ET.fromstring(resp.content)
            for seg in root.iter("seg"):
                text = seg.text
                if text and len(text.strip()) > 10:
                    records.append({
                        "id": generate_id(url, text),
                        "source": "bible_creole",
                        "url": url,
                        "title": "Bib la an Kreyòl",
                        "text": clean_text(text),
                        "language": "ht",
                        "scraped_at": datetime.now().isoformat()
                    })
        except Exception as e:
            log.error(f"Erè Bible: {e}")
        save_batch(records, "bible_creole")
        log.info(f"✅ Bib la: {len(records)} vèsè!")


# ─────────────────────────────────────────────
# DOWNLOADER — DONE KI DEJA PRÈT (HuggingFace)
# ─────────────────────────────────────────────

class HuggingFaceDownloader:

    def run(self):
        log.info("🤗 Download done HuggingFace...")
        sources = [
            ("jsbeaudry/haitian_creole_tts_11K", "hf_tts_11k"),
            ("jsbeaudry/cmu_haitian_creole_speech", "hf_cmu_speech"),
        ]
        for dataset_name, out_name in sources:
            try:
                from datasets import load_dataset
                log.info(f"  Telechaje {dataset_name}...")
                ds = load_dataset(dataset_name, trust_remote_code=True)
                ds.save_to_disk(str(OUTPUT_DIR / "raw" / out_name))
                log.info(f"  ✅ {dataset_name} sovgade!")
            except Exception as e:
                log.warning(f"  ⚠️ {dataset_name} echwe: {e}")


# ─────────────────────────────────────────────
# NETWAYMAN & STATISTIK
# ─────────────────────────────────────────────

class DataCleaner:

    def run(self):
        log.info("🧹 Netwayaj ak déduplikasyon...")
        all_records, seen_ids = [], set()

        for f in (OUTPUT_DIR / "raw").glob("*.jsonl"):
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rec = json.loads(line.strip())
                        if rec["id"] not in seen_ids and len(rec["text"]) > 50:
                            seen_ids.add(rec["id"])
                            all_records.append(rec)
                    except Exception:
                        continue

        log.info(f"Anvan déduplikasyon: {len(all_records)}")

        text_hashes, clean_records = set(), []
        for rec in all_records:
            h = hashlib.md5(rec["text"][:200].encode()).hexdigest()
            if h not in text_hashes:
                text_hashes.add(h)
                clean_records.append(rec)

        log.info(f"Apre déduplikasyon: {len(clean_records)}")

        df = pd.DataFrame(clean_records)
        df.to_csv(OUTPUT_DIR / "cleaned" / "dataset_final.csv", index=False, encoding="utf-8")
        df.to_json(OUTPUT_DIR / "cleaned" / "dataset_final.jsonl",
                   orient="records", lines=True, force_ascii=False)

        stats = {
            "total_records": len(clean_records),
            "by_source": df["source"].value_counts().to_dict(),
            "by_language": df["language"].value_counts().to_dict(),
            "avg_text_length": int(df["text"].str.len().mean()),
            "total_characters": int(df["text"].str.len().sum()),
            "total_mb": round(df["text"].str.len().sum() / 1_000_000, 2),
            "generated_at": datetime.now().isoformat()
        }

        with open(OUTPUT_DIR / "cleaned" / "stats.json", "w") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        log.info("📊 STATISTIK FINAL:")
        for k, v in stats.items():
            log.info(f"  {k}: {v}")

        return clean_records


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║   🇭🇹  ZAMA — HAITIAN CREOLE DATASET PIPELINE  🇭🇹            ║
║   Koleksyon Done pou Premye LLM Ayisyen                      ║
║   Version 2.0 — GitHub Actions Ready                         ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Etap 1 — Done ki deja prèt sou HuggingFace
    HuggingFaceDownloader().run()

    # Etap 2 — Wikipedia (pi enpòtan)
    wiki = WikipediaCreoleScraper()
    wiki.download_full_dump()
    wiki.run(max_articles=500)

    # Etap 3 — Medya Ayisyen
    LeNouvellisteScraper().run(max_pages=20)
    AlterPresseScraper().run(max_pages=30)
    RezoNodwesScraper().run(max_pages=15)
    HaitiLibreScraper().run(max_pages=20)

    # Etap 4 — Bib la
    BibleCreoleScraper().run()

    # Etap 5 — Netwaye tout
    DataCleaner().run()

    print("""
╔══════════════════════════════════════════════════════════════╗
║  ✅  KOLEKSYON FINI!                                          ║
║  📁  Done: data/cleaned/dataset_final.jsonl                  ║
║  📊  Statistik: data/cleaned/stats.json                      ║
╚══════════════════════════════════════════════════════════════╝
    """)

if __name__ == "__main__":
    main()
