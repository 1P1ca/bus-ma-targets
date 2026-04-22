"""Application web — Qualification des transporteurs par autobus (FTA).

Interface en français canadien pour consulter, filtrer, modifier les fiches
d'entreprises et documenter les communications et enrichissements manuels.

Démarrage :
    cd bus_ma && python3 src/webapp.py
    http://127.0.0.1:8765/
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from flask import (Flask, abort, flash, g, jsonify, redirect, render_template,
                   request, send_from_directory, url_for)

from db import DB_PATH, connect, init_db
from web_schema import upgrade as upgrade_web_schema

ROOT = Path(__file__).resolve().parent.parent

app = Flask(__name__, template_folder=str(Path(__file__).parent / "templates"),
            static_folder=str(Path(__file__).parent / "static"))
app.secret_key = os.environ.get("BUS_MA_SECRET", "dev-change-me")


# ---------------------------------------------------------------------------
# Libellés en français canadien
# ---------------------------------------------------------------------------

STATUT_CHOICES = [
    ("nouveau",       "Nouveau"),
    ("à contacter",   "À contacter"),
    ("en discussion", "En discussion"),
    ("offre envoyée", "Offre envoyée"),
    ("rejeté",        "Rejeté"),
    ("acquis",        "Acquis"),
]

COMM_KIND_CHOICES = [
    ("appel",     "Appel téléphonique"),
    ("courriel",  "Courriel"),
    ("rencontre", "Rencontre"),
    ("visite",    "Visite des installations"),
    ("autre",     "Autre"),
]

COMM_DIR_CHOICES = [
    ("sortant", "Sortant"),
    ("entrant", "Entrant"),
]

PERMIT_LABELS = {
    "scolaire":    "Scolaire",
    "nolise":      "Nolisé",
    "interurbain": "Interurbain",
    "urbain":      "Urbain",
    "adapte":      "Transport adapté",
    "taxi":        "Taxi",
}


# ---------------------------------------------------------------------------
# Connexion SQLite par requête
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(DB_PATH)
    return g.db


@app.teardown_appcontext
def close_db(_exc=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.context_processor
def inject_globals():
    return {
        "STATUT_CHOICES": STATUT_CHOICES,
        "COMM_KIND_CHOICES": COMM_KIND_CHOICES,
        "COMM_DIR_CHOICES": COMM_DIR_CHOICES,
        "PERMIT_LABELS": PERMIT_LABELS,
        "author": request.cookies.get("bus_ma_author", ""),
    }


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------

OPERATOR_EDITABLE_FIELDS = {
    "name", "legal_name", "neq", "address", "city", "postal", "province",
    "phone", "fax", "email", "website", "president", "delegue_votant",
    "notes", "status", "owner_flag",
}


def _audit(conn, op_id: int, field: str, old, new, author: str):
    if (old or "") == (new or ""):
        return
    conn.execute(
        "INSERT INTO edit_log (operator_id, field, old_value, new_value, author) "
        "VALUES (?, ?, ?, ?, ?)",
        (op_id, field, old, new, author),
    )


# ---------------------------------------------------------------------------
# Routes — liste + filtres
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    db = get_db()
    q          = (request.args.get("q") or "").strip()
    city       = (request.args.get("city") or "").strip()
    statut     = (request.args.get("statut") or "").strip()
    excl       = request.args.get("excl") or "non"   # 'oui'|'non'|'tous'
    has_website = request.args.get("has_website") == "1"
    min_cap    = request.args.get("min_cap", type=int)
    sort       = request.args.get("sort") or "rank"
    direction  = request.args.get("dir") or "asc"

    where = ["1=1"]
    params: list = []
    if q:
        where.append("(o.name LIKE ? OR o.legal_name LIKE ? OR o.president LIKE ? "
                     "OR o.email LIKE ? OR o.neq LIKE ? OR o.city LIKE ?)")
        params.extend([f"%{q}%"] * 6)
    if city:
        where.append("o.city LIKE ?"); params.append(f"%{city}%")
    if statut:
        where.append("o.status = ?"); params.append(statut)
    if excl == "oui":
        where.append("g.is_excluded = 1")
    elif excl == "non":
        where.append("(g.is_excluded IS NULL OR g.is_excluded = 0)")
    if has_website:
        where.append("o.website IS NOT NULL AND o.website <> ''")
    if min_cap:
        where.append("(SELECT MAX(bus_capacity_est) FROM facility WHERE operator_id=o.id) >= ?")
        params.append(min_cap)

    sort_cols = {
        "rank":         "s.rank",
        "name":         "o.name COLLATE NOCASE",
        "city":         "o.city COLLATE NOCASE",
        "fit":          "s.ma_fit_score",
        "cap":          "(SELECT MAX(bus_capacity_est) FROM facility WHERE operator_id=o.id)",
        "parking":      "(SELECT MAX(parking_area_m2) FROM facility WHERE operator_id=o.id)",
        "status":       "o.status",
        "last_contact": "o.last_contact_at",
    }
    order_sql = sort_cols.get(sort, "s.rank")
    direction = "DESC" if direction.lower() == "desc" else "ASC"
    nulls = "NULLS LAST" if direction == "ASC" else "NULLS LAST"

    sql = f"""
    SELECT o.id, o.fta_no, o.name, o.city, o.postal, o.phone, o.email,
           o.website, o.president, o.status, o.owner_flag, o.last_contact_at,
           g.name AS group_name, g.is_excluded,
           s.rank, s.ma_fit_score,
           (SELECT MAX(bus_capacity_est) FROM facility WHERE operator_id=o.id) AS cap,
           (SELECT MAX(parking_area_m2)  FROM facility WHERE operator_id=o.id) AS parking,
           (SELECT COUNT(*) FROM communications WHERE operator_id=o.id)        AS n_comm
    FROM operators o
    LEFT JOIN groups g ON o.group_id = g.id
    LEFT JOIN scores s ON s.operator_id = o.id
    WHERE {' AND '.join(where)}
    ORDER BY {order_sql} {direction} {nulls}, o.name COLLATE NOCASE
    LIMIT 1000
    """
    rows = db.execute(sql, params).fetchall()
    total = db.execute("SELECT COUNT(*) FROM operators").fetchone()[0]

    # liste des villes distinctes pour le filtre
    cities = [r[0] for r in db.execute(
        "SELECT DISTINCT city FROM operators WHERE city IS NOT NULL "
        "ORDER BY city COLLATE NOCASE"
    ).fetchall()]

    return render_template("index.html",
                           rows=rows, total=total, filtered=len(rows),
                           q=q, city=city, statut=statut, excl=excl,
                           has_website=has_website, min_cap=min_cap or "",
                           sort=sort, direction=direction.lower(),
                           cities=cities)


# ---------------------------------------------------------------------------
# Fiche entreprise
# ---------------------------------------------------------------------------

@app.route("/operateurs/<int:op_id>")
def operateurs_redirect(op_id: int):
    """Redirect from old /operateurs/ URL to new /op/ URL"""
    return redirect(url_for('op_detail', op_id=op_id))

@app.route("/op/<int:op_id>")
def op_detail(op_id: int):
    db = get_db()
    op = db.execute("""
        SELECT o.*, g.name AS group_name, g.kind AS group_kind,
               g.is_excluded AS group_excluded,
               s.rank, s.ma_fit_score, s.size_score, s.independence_score,
               s.clarity_score, s.succession_score, s.rationale
        FROM operators o
        LEFT JOIN groups g ON o.group_id = g.id
        LEFT JOIN scores s ON s.operator_id = o.id
        WHERE o.id = ?
    """, (op_id,)).fetchone()
    if not op:
        abort(404)
    facility = db.execute(
        "SELECT * FROM facility WHERE operator_id=? ORDER BY id DESC LIMIT 1",
        (op_id,)).fetchone()
    fleet = db.execute(
        "SELECT * FROM fleet WHERE operator_id=? ORDER BY id DESC",
        (op_id,)).fetchall()
    permits = db.execute(
        "SELECT * FROM permits WHERE operator_id=? ORDER BY permit_type",
        (op_id,)).fetchall()
    ownership = db.execute(
        "SELECT * FROM ownership WHERE operator_id=? ORDER BY role, entity_name",
        (op_id,)).fetchall()
    comms = db.execute(
        "SELECT * FROM communications WHERE operator_id=? "
        "ORDER BY occurred_at DESC, id DESC", (op_id,)).fetchall()
    enrichments = db.execute(
        "SELECT * FROM enrichments WHERE operator_id=? "
        "ORDER BY created_at DESC", (op_id,)).fetchall()
    edits = db.execute(
        "SELECT * FROM edit_log WHERE operator_id=? "
        "ORDER BY created_at DESC LIMIT 50", (op_id,)).fetchall()
    return render_template("op_detail.html",
                           op=op, facility=facility, fleet=fleet,
                           permits=permits, ownership=ownership,
                           comms=comms, enrichments=enrichments, edits=edits)


@app.route("/op/<int:op_id>/edit", methods=["POST"])
def op_edit(op_id: int):
    db = get_db()
    op = db.execute("SELECT * FROM operators WHERE id=?", (op_id,)).fetchone()
    if not op:
        abort(404)
    author = request.form.get("author", "").strip() or "anonyme"
    changes = 0
    sets = []
    params: list = []
    for field in OPERATOR_EDITABLE_FIELDS:
        if field not in request.form:
            continue
        new = (request.form.get(field) or "").strip() or None
        if field == "owner_flag":
            new = 1 if new else 0
        old = op[field] if field in op.keys() else None
        if (old or "") != (new or ""):
            sets.append(f"{field} = ?")
            params.append(new)
            _audit(db, op_id, field, old, new, author)
            changes += 1
    if sets:
        params.append(op_id)
        db.execute(
            f"UPDATE operators SET {', '.join(sets)}, "
            f"updated_at = CURRENT_TIMESTAMP WHERE id = ?", params)
        db.commit()
    if changes:
        flash(f"{changes} champ(s) mis à jour.", "success")
    else:
        flash("Aucune modification détectée.", "info")
    resp = redirect(url_for("op_detail", op_id=op_id))
    resp.set_cookie("bus_ma_author", author, max_age=60 * 60 * 24 * 365)
    return resp


# ---------------------------------------------------------------------------
# Communications
# ---------------------------------------------------------------------------

@app.route("/op/<int:op_id>/comm", methods=["POST"])
def comm_add(op_id: int):
    db = get_db()
    if not db.execute("SELECT 1 FROM operators WHERE id=?", (op_id,)).fetchone():
        abort(404)
    author = (request.form.get("author") or "anonyme").strip()
    kind = request.form.get("kind") or "autre"
    direction = request.form.get("direction") or "sortant"
    occurred_at = (request.form.get("occurred_at")
                   or datetime.now().strftime("%Y-%m-%dT%H:%M"))
    contact = (request.form.get("contact") or "").strip() or None
    subject = (request.form.get("subject") or "").strip() or None
    notes = (request.form.get("notes") or "").strip() or None
    next_step = (request.form.get("next_step") or "").strip() or None

    db.execute(
        "INSERT INTO communications (operator_id, kind, direction, occurred_at, "
        "contact, subject, notes, next_step, author) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (op_id, kind, direction, occurred_at, contact, subject, notes, next_step, author),
    )
    db.execute(
        "UPDATE operators SET last_contact_at = ?, updated_at = CURRENT_TIMESTAMP "
        "WHERE id = ?", (occurred_at, op_id))
    db.commit()
    flash("Communication enregistrée.", "success")
    resp = redirect(url_for("op_detail", op_id=op_id) + "#communications")
    resp.set_cookie("bus_ma_author", author, max_age=60 * 60 * 24 * 365)
    return resp


@app.route("/comm/<int:cid>/delete", methods=["POST"])
def comm_delete(cid: int):
    db = get_db()
    row = db.execute("SELECT operator_id FROM communications WHERE id=?", (cid,)).fetchone()
    if not row:
        abort(404)
    db.execute("DELETE FROM communications WHERE id=?", (cid,))
    db.commit()
    flash("Communication supprimée.", "info")
    return redirect(url_for("op_detail", op_id=row["operator_id"]) + "#communications")


# ---------------------------------------------------------------------------
# Enrichissements (données externes entrées manuellement)
# ---------------------------------------------------------------------------

ENRICH_FIELDS = [
    ("neq",            "NEQ"),
    ("fleet_total",    "Nombre total d'autobus (véridique)"),
    ("fleet_scolaire", "Nombre d'autobus scolaires"),
    ("fleet_nolise",   "Nombre d'autocars nolisés"),
    ("fleet_adapte",   "Nombre de véhicules adaptés"),
    ("owner_age",      "Âge approximatif du propriétaire"),
    ("successor",      "Successeur identifié"),
    ("revenue_est",    "Revenus annuels estimés (CAD)"),
    ("shareholders",   "Actionnaires (liste, %)"),
    ("parking_m2_manual", "Surface de stationnement mesurée (m²)"),
    ("building_m2_manual","Surface de bâtiment mesurée (m²)"),
    ("website",        "Site Internet (correction)"),
    ("email",          "Courriel (correction)"),
    ("phone",          "Téléphone (correction)"),
    ("note",           "Note libre"),
]


@app.route("/op/<int:op_id>/enrich", methods=["POST"])
def enrich_add(op_id: int):
    db = get_db()
    if not db.execute("SELECT 1 FROM operators WHERE id=?", (op_id,)).fetchone():
        abort(404)
    author = (request.form.get("author") or "anonyme").strip()
    field = request.form.get("field") or "note"
    value = (request.form.get("value") or "").strip()
    source = (request.form.get("source") or "").strip() or None
    source_url = (request.form.get("source_url") or "").strip() or None
    try:
        confidence = float(request.form.get("confidence") or 0.8)
    except ValueError:
        confidence = 0.8

    if not value:
        flash("La valeur est requise.", "error")
        return redirect(url_for("op_detail", op_id=op_id) + "#enrichissements")

    db.execute(
        "INSERT INTO enrichments (operator_id, field, value, source, source_url, "
        "confidence, author) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (op_id, field, value, source, source_url, confidence, author),
    )

    # "Promote" selected enrichments into the canonical operators row when
    # they're a direct correction and the user asks for it.
    promote = request.form.get("promote") == "1"
    promote_map = {
        "neq":      "neq",
        "website":  "website",
        "email":    "email",
        "phone":    "phone",
    }
    if promote and field in promote_map:
        col = promote_map[field]
        old = db.execute(f"SELECT {col} FROM operators WHERE id=?", (op_id,)).fetchone()[0]
        db.execute(f"UPDATE operators SET {col}=?, updated_at=CURRENT_TIMESTAMP "
                   f"WHERE id=?", (value, op_id))
        _audit(db, op_id, col, old, value, f"{author} (promu depuis enrichissement)")
    db.commit()
    flash("Enrichissement ajouté.", "success")
    resp = redirect(url_for("op_detail", op_id=op_id) + "#enrichissements")
    resp.set_cookie("bus_ma_author", author, max_age=60 * 60 * 24 * 365)
    return resp


@app.route("/enrich/<int:eid>/delete", methods=["POST"])
def enrich_delete(eid: int):
    db = get_db()
    row = db.execute("SELECT operator_id FROM enrichments WHERE id=?", (eid,)).fetchone()
    if not row:
        abort(404)
    db.execute("DELETE FROM enrichments WHERE id=?", (eid,))
    db.commit()
    flash("Enrichissement supprimé.", "info")
    return redirect(url_for("op_detail", op_id=row["operator_id"]) + "#enrichissements")


# ---------------------------------------------------------------------------
# Création d'un nouvel opérateur (hors liste FTA)
# ---------------------------------------------------------------------------

@app.route("/op/nouveau", methods=["GET", "POST"])
def op_new():
    db = get_db()
    if request.method == "POST":
        author = (request.form.get("author") or "anonyme").strip()
        name = (request.form.get("name") or "").strip()
        if not name:
            flash("Le nom de l'entreprise est requis.", "error")
            return redirect(url_for("op_new"))
        fields = {k: (request.form.get(k) or "").strip() or None
                  for k in OPERATOR_EDITABLE_FIELDS if k in request.form}
        fields["name"] = name
        cols = ",".join(fields.keys())
        qmarks = ",".join("?" for _ in fields)
        cur = db.execute(
            f"INSERT INTO operators ({cols}) VALUES ({qmarks})",
            list(fields.values()))
        op_id = cur.lastrowid
        for f, v in fields.items():
            if v:
                _audit(db, op_id, f, None, v, author)
        db.commit()
        flash(f"Fiche créée (#{op_id}).", "success")
        return redirect(url_for("op_detail", op_id=op_id))
    return render_template("op_new.html")


# ---------------------------------------------------------------------------
# Groupes / consolidators view
# ---------------------------------------------------------------------------

@app.route("/groupes")
def groupes():
    db = get_db()
    rows = db.execute("""
        SELECT g.id, g.name, g.kind, g.is_excluded, g.description,
               COUNT(o.id) AS n,
               GROUP_CONCAT(o.name, ' · ') AS members
        FROM groups g LEFT JOIN operators o ON o.group_id = g.id
        GROUP BY g.id
        ORDER BY g.is_excluded DESC, n DESC, g.name COLLATE NOCASE
    """).fetchall()
    return render_template("groupes.html", rows=rows)


# ---------------------------------------------------------------------------
# Statistiques
# ---------------------------------------------------------------------------

@app.route("/stats")
def stats():
    db = get_db()
    def one(q): return db.execute(q).fetchone()[0]
    counts = {
        "Opérateurs (total)":            one("SELECT COUNT(*) FROM operators"),
        "Groupes":                       one("SELECT COUNT(*) FROM groups"),
        "Filiales de consolidateurs":    one(
            "SELECT COUNT(*) FROM operators o JOIN groups g ON o.group_id=g.id "
            "WHERE g.is_excluded=1"),
        "Indépendants (candidats)":      one(
            "SELECT COUNT(*) FROM operators o LEFT JOIN groups g ON o.group_id=g.id "
            "WHERE g.is_excluded IS NULL OR g.is_excluded=0"),
        "Avec NEQ":                      one("SELECT COUNT(*) FROM operators WHERE neq IS NOT NULL AND neq <> ''"),
        "Avec coordonnées GPS":          one("SELECT COUNT(*) FROM operators WHERE lat IS NOT NULL"),
        "Avec permis CTQ":               one("SELECT COUNT(DISTINCT operator_id) FROM permits"),
        "Avec estimation d'installations": one("SELECT COUNT(DISTINCT operator_id) FROM facility"),
        "Avec actionnariat (REQ)":       one("SELECT COUNT(DISTINCT operator_id) FROM ownership"),
        "Opérateurs notés (pointage)":   one("SELECT COUNT(*) FROM scores"),
        "Communications enregistrées":   one("SELECT COUNT(*) FROM communications"),
        "Enrichissements saisis":        one("SELECT COUNT(*) FROM enrichments"),
    }
    statuts = db.execute(
        "SELECT COALESCE(status,'(non défini)') AS s, COUNT(*) AS n "
        "FROM operators GROUP BY s ORDER BY n DESC").fetchall()
    return render_template("stats.html", counts=counts, statuts=statuts)


# ---------------------------------------------------------------------------
# Image satellite
# ---------------------------------------------------------------------------

@app.route("/satellite/<path:name>")
def satellite(name: str):
    sat_dir = ROOT / "data" / "satellite"
    return send_from_directory(sat_dir, name)


# ---------------------------------------------------------------------------
# Export Excel (réutilise le module existant)
# ---------------------------------------------------------------------------

@app.route("/export")
@app.route("/export_xlsx")
def export_xlsx():
    """Serve pre-built Excel export from output directory"""
    xlsx_path = Path(__file__).resolve().parent.parent / "output" / "operators_ma.xlsx"
    if xlsx_path.exists():
        return send_from_directory(xlsx_path.parent, xlsx_path.name, as_attachment=True)
    else:
        # Fallback: try to generate it
        try:
            import export_excel
            p = export_excel.export()
            return send_from_directory(p.parent, p.name, as_attachment=True)
        except Exception as e:
            abort(500)


# ---------------------------------------------------------------------------
# Démarrage
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Cibles d'acquisition (M&A targets)
# ---------------------------------------------------------------------------

@app.route("/targets")
def targets():
    """Display top-30 independent operators ranked by M&A fit score."""
    db = get_db()
    
    # Get filters
    city_filter = request.args.get("city", "").strip()
    min_score = request.args.get("min_score", "0.30")
    try:
        min_score = float(min_score)
    except (ValueError, TypeError):
        min_score = 0.30
    
    # Query top-30 independents with all scoring details
    query = """
        SELECT 
            o.id,
            o.name,
            o.city,
            o.president,
            o.website,
            o.email,
            COALESCE(f.bus_capacity_est, 0) as cap_est,
            COALESCE(f.parking_area_m2, 0) as parking_m2,
            COALESCE(s.size_score, 0) as size_score,
            COALESCE(s.independence_score, 1.0) as independence_score,
            COALESCE(s.clarity_score, 0) as clarity_score,
            COALESCE(s.ma_fit_score, 0) as fit_score,
            COALESCE(s.rank, 0) as rank
        FROM operators o
        LEFT JOIN facility f ON o.id = f.operator_id
        LEFT JOIN scores s ON o.id = s.operator_id
        WHERE o.group_id IS NULL
          AND COALESCE(s.ma_fit_score, 0) >= ?
    """
    
    params = [min_score]
    
    if city_filter:
        query += " AND o.city LIKE ?"
        params.append(f"%{city_filter}%")
    
    query += " ORDER BY COALESCE(s.ma_fit_score, 0) DESC LIMIT 30"
    
    targets = db.execute(query, params).fetchall()
    targets = [dict(row) for row in targets]
    
    # Stats
    total_independents = db.execute(
        "SELECT COUNT(*) FROM operators WHERE group_id IS NULL"
    ).fetchone()[0]
    
    total_consolidators = db.execute(
        "SELECT COUNT(*) FROM operators WHERE group_id IS NOT NULL"
    ).fetchone()[0]
    
    avg_fleet = db.execute(
        "SELECT AVG(COALESCE(bus_capacity_est, 0)) FROM facility"
    ).fetchone()[0] or 0
    
    top3_score = db.execute(
        "SELECT MAX(ma_fit_score) FROM scores WHERE operator_id IN "
        "(SELECT id FROM operators WHERE group_id IS NULL)"
    ).fetchone()[0] or 0
    
    return render_template(
        "targets.html",
        targets=targets,
        total_independents=total_independents,
        total_consolidators=total_consolidators,
        avg_fleet=avg_fleet,
        top3_score=top3_score
    )

def main():
    init_db()
    upgrade_web_schema()
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8765"))
    print(f"Application prête : http://{host}:{port}/")
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    main()
