# 🇭🇹 ZAMA — Premye Dataset Kreyòl Ayisyen pou LLM

> *Zama* vle di "konesans" an kreyòl.

Pwojè sa a kolekte done tèks kreyòl ayisyen pou antrene premye gwo modèl lang (LLM) nasyonal Ayiti.

---

## 📦 Sous Done

| Sous | Tip | Lang |
|------|-----|------|
| Wikipedia Kreyòl | Ansiklopedi | ht |
| Le Nouvelliste | Jounal | fr/ht |
| AlterPresse | Nouvèl | fr/ht |
| Rezo Nodwès | Nouvèl | ht |
| Haiti Libre | Nouvèl | fr/ht |
| Bib la | Literati | ht |
| HuggingFace CMU | Odyo + Tèks | ht |

---

## 🚀 Kijan pou Kouri

### Lokal (Windows/Mac/Linux)
```bash
pip install -r requirements.txt
python scripts/scraper.py
```

### GitHub Actions (otomatik)
1. Fork repo sa a
2. Ale sou **Actions** → **Zama — Haitian Creole Data Collection**
3. Klike **Run workflow**
4. Telechaje done nan **Artifacts** apre li fini

---

## 📊 Rezilta

Done yo ap nan `data/cleaned/`:
- `dataset_final.jsonl` — Tout done (JSONL format)
- `dataset_final.csv` — Tout done (CSV format)
- `stats.json` — Statistik koleksyon

---

## 🤝 Kontribiye

Pwojè sa a pou tout Ayisyen. Si ou vle kontribiye:
- Ajoute nouvo sous done
- Korije erè nan done egzistan yo
- Ede anotasyon done

---

## 📄 Lisans

MIT License — Libre pou tout moun itilize.

---

*Fèt ak ❤️ pou Ayiti*
