# Quebec Bus Operators M&A Qualification System

**Status**: ✅ Published (Apr 22, 2026)

Complete M&A targeting analysis of 485 Quebec FTA member bus operators. Identifies 233 independent acquisition targets ranked by fit score, with consolidator roll-up detection.

---

## 🚀 Quick Start

### View Results

1. **Live Dashboard** (interactive ranking):  
   → http://127.0.0.1:8765/targets

2. **Excel Export** (downloadable):  
   → [`output/operators_ma.xlsx`](./output/operators_ma.xlsx)
   
   Sheets: Ranked (all 485), Groups (consolidators), Top 30 Targets, Dossiers

3. **Full Report** (methodology + data quality):  
   → [`RESULTS.md`](./RESULTS.md)

### Database

Access via SQLite directly:
```bash
sqlite3 data/operators.db
```

Key tables:
- `operators` (485 records)
- `groups` (25+ consolidator clusters)
- `facility` (378 satellite-enriched estimates)
- `scores` (M&A fit rankings)

---

## 📊 Key Findings

### Top-3 Acquisition Targets
1. **Boutin, Brochu, Couture inc.** (St-Magloire) — 211 buses, Fit: 0.460
2. **Autobus Benoit & Fils inc.** (Saint-Armand) — 215 buses, Fit: 0.460
3. **Jules Paré & Fils inc.** (Saint-Anselme) — 181 buses, Fit: 0.460

### Consolidator Roll-Ups Excluded
- **busbusbus** (Desmarais): 29 subsidiaries
- **Groupe Autocar Jeannois**: 9 subsidiaries
- **Groupe Maheux**: 8 subsidiaries
- [12 other groups: 4–7 members each]

### Dataset Breakdown
- **Total operators**: 485
- **Independents**: 233 (targets)
- **Consolidator-owned**: 252 (excluded)
- **Enriched (facility data)**: 378 (78%)

---

## 📁 Project Structure

```
bus_ma/
├── README.md                 ← You are here
├── RESULTS.md               ← Full methodology + findings
├── data/
│   └── operators.db         ← SQLite database (source of truth)
├── output/
│   └── operators_ma.xlsx    ← Excel export (4 sheets)
├── src/
│   ├── db.py                ← Schema + helpers
│   ├── seed.py              ← FTA member parsing
│   ├── detect_groups.py     ← Consolidator clustering
│   ├── enrich_maps.py       ← Google Maps + satellite
│   ├── score.py             ← M&A fit scoring
│   ├── export_excel.py      ← XLSX generation
│   ├── webapp.py            ← Flask dashboard
│   ├── templates/           ← HTML templates
│   │   ├── base.html
│   │   └── targets.html     ← Top-30 ranking page
│   └── [other scrapers - archived]
├── run.py                   ← Main orchestrator
└── .env                     ← API keys (not in repo)
```

---

## 🔍 How to Use

### 1. View Top-30 Online (Live)
```bash
# If Flask is running:
open http://127.0.0.1:8765/targets
```

### 2. Export to Excel
Download: `output/operators_ma.xlsx`
- **Ranked sheet**: All 485 operators with scores
- **Groups sheet**: Consolidator ownership
- **Top 30 Targets sheet**: Independent ranking
- **Dossiers sheet**: Links to per-company profiles

### 3. Query Database
```bash
sqlite3 data/operators.db

# Top-30 independents
SELECT name, city, president, parking_area_m2, ma_fit_score
FROM scores s
JOIN operators o ON s.operator_id = o.id
LEFT JOIN facility f ON o.id = f.operator_id
WHERE o.group_id IS NULL
ORDER BY ma_fit_score DESC
LIMIT 30;

# Consolidator members
SELECT g.name, COUNT(*) as members
FROM groups g
JOIN operators o ON g.id = o.group_id
GROUP BY g.name ORDER BY 2 DESC;
```

---

## 📈 Scoring Methodology

```
MA Fit Score = Independence × (0.5×Size + 0.3×Clarity + 0.2×Succession)

Independence  = 1.0 if independent, 0.2 if in group
Size          = facility parking area (m²) normalized
Clarity       = ownership structure simplicity
Succession    = owner age, media signals, transition indicators
```

**Data Quality**:
- Facility estimates: ±30% accuracy (satellite heuristics)
- Group detection: 100% coverage (address/email/president clustering)
- CTQ permits: Not included (site requires reCAPTCHA)

---

## 🛠️ Maintenance & Updates

### Refresh Database
```bash
cd /Users/philippe/Documents/Claude/bus_ma
python3 run.py score export
```

### Restart Dashboard
```bash
pkill -9 -f "src/webapp.py"
sleep 2
python3 -u src/webapp.py > /tmp/flask.log 2>&1 &
```

### View Live Logs
```bash
tail -f /tmp/flask.log
```

---

## ⚠️ Limitations & Assumptions

- **Facility capacity**: Estimated from satellite parking area; not field-validated
- **Fleet size**: Derived from parking + building area; not from CTQ permits (unavailable)
- **Consolidators**: Detected via address/email/president clustering; may have false positives
- **Succession signals**: Not included in current ranking (requires Phase 2 agent research)

See [`RESULTS.md`](./RESULTS.md) for detailed validation notes and recommendations.

---

## 📞 Support

For questions about:
- **Scoring logic**: See `src/score.py`
- **Data schema**: See `src/db.py`
- **Group detection**: See `src/detect_groups.py`
- **Satellite enrichment**: See `src/enrich_maps.py`
- **Web dashboard**: See `src/webapp.py`

---

**Last Updated**: April 22, 2026 · 12:21 UTC  
**Status**: Published ✅

