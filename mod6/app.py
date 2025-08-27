#!/usr/bin/env python3
"""
Full Flask app (fixed) — MongoDB Atlas version of the Pokétwo recommender.
Drop into mod6/app.py and deploy.
"""

import os
import re
import math
import statistics
from flask import Flask, render_template_string, request, Response
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from bson import json_util

# ------------------------------
# Configuration / tuning
# ------------------------------
MAX_DEV_PERCENT = 0.15
MIN_DEV_PERCENT = 0.03
MAX_MULTIPLIER = 2.0
MAX_TREND_PCT = 0.25
RECENT_WINDOW = 20

# ------------------------------
# Flask + MongoDB setup
# ------------------------------
app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable not set! Add it in Render/Env vars.")

client = MongoClient(MONGO_URI, server_api=ServerApi("1"))
db = client["auctions"]           # DB name you imported into
auctions_col = db["auctions"]     # Collection name you used

# ------------------------------
# Recommendation algorithm (same logic as before)
# ------------------------------
def get_price_recommendation(chron_prices):
    if not chron_prices:
        return {"success": False, "message": "No past sales found for these criteria."}

    nums = [int(p) for p in chron_prices if p is not None]
    n = len(nums)
    if n < 2:
        return {"success": False, "message": "Not enough numeric sales for a reliable recommendation."}

    cleaned_chron = nums[:]
    if n >= 4:
        try:
            q1, _, q3 = statistics.quantiles(nums, n=4)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            potential_cleaned = [p for p in nums if (lower <= p <= upper)]
            if len(potential_cleaned) >= 2:
                cleaned_chron = potential_cleaned
        except Exception:
            cleaned_chron = nums[:]

    m = len(cleaned_chron)
    median = statistics.median(cleaned_chron)
    stdev = statistics.stdev(cleaned_chron) if m > 1 else 0.0

    if median <= 0:
        base_dev = max(1.0, stdev, 5.0)
        conservative = max(1.0, median - base_dev)
        aggressive = median + base_dev
    else:
        rel_stdev = (stdev / median) if median else MAX_DEV_PERCENT
        pct = max(MIN_DEV_PERCENT, min(rel_stdev, MAX_DEV_PERCENT))
        conservative = median * (1.0 - pct)
        aggressive = median * (1.0 + pct)
        min_abs_floor = max(1.0, 0.02 * median)
        if (median - conservative) < min_abs_floor:
            conservative = max(1.0, median - min_abs_floor)
        if (aggressive - median) < min_abs_floor:
            aggressive = median + min_abs_floor
        aggressive = min(aggressive, median * MAX_MULTIPLIER)

    trend_info = {"slope": 0.0, "trend_pct": 0.0, "direction": "flat", "n": m}
    if m >= 6:
        x = list(range(m))
        y = cleaned_chron
        alpha = math.log(2.0) / max(1, (m - 1))
        weights = [math.exp(alpha * i) for i in x]
        w_sum = sum(weights)
        x_mean = sum(w * xi for w, xi in zip(weights, x)) / w_sum
        y_mean = sum(w * yi for w, yi in zip(weights, y)) / w_sum
        num = sum(w * (xi - x_mean) * (yi - y_mean) for w, xi, yi in zip(weights, x, y))
        den = sum(w * (xi - x_mean) ** 2 for w, xi in zip(weights, x))
        slope = (num / den) if den != 0 else 0.0
        recent_window = min(RECENT_WINDOW, m)
        trend_pct = (slope * recent_window) / median if median else 0.0
        trend_pct = max(-MAX_TREND_PCT, min(MAX_TREND_PCT, trend_pct))
        direction = "up" if trend_pct > 1e-4 else ("down" if trend_pct < -1e-4 else "flat")
        trend_info = {"slope": round(slope, 6), "trend_pct": round(trend_pct, 6), "direction": direction, "n": m}
        if trend_pct > 0:
            aggressive = min(aggressive * (1.0 + trend_pct), median * MAX_MULTIPLIER)
        elif trend_pct < 0:
            conservative *= (1.0 + trend_pct)
            aggressive *= (1.0 + trend_pct)
            conservative = max(1.0, conservative)
            aggressive = max(aggressive, conservative)

    conservative_bid = max(1, int(round(conservative)))
    aggressive_bid = max(conservative_bid, int(round(aggressive)))

    return {
        "success": True,
        "count": m,
        "original_count": n,
        "median": int(round(median)),
        "stdev": round(stdev, 2),
        "conservative_bid": conservative_bid,
        "aggressive_bid": aggressive_bid,
        "trend": trend_info
    }

# ------------------------------
# HTML template (kept from your original)
# ------------------------------
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Pokétwo Price Recommender</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial; background:#f0f2f5; color:#111; margin:0; padding:2rem; }
    .container { max-width: 960px; margin:auto; background:#fff; padding:2rem; border-radius:10px; box-shadow:0 6px 20px rgba(0,0,0,0.06); }
    h1 { color:#1877f2; margin-top:0; border-bottom: 1px solid #ddd; padding-bottom: 0.5rem; }
    .form-grid { display:grid; grid-template-columns:1fr 1fr 1fr; gap:1rem; align-items:center; margin-bottom:1rem; }
    .species-input { grid-column:1 / -1; }
    .iv-grid { display:grid; grid-template-columns: repeat(6, 1fr); gap: 0.5rem; grid-column: 1 / -1; }
    .iv-grid input { text-align: center; }
    h3 { margin-bottom: 0.5rem; color: #666; font-size: 0.9rem; }
    input, select { width:100%; padding:10px; border-radius:6px; border:1px solid #e0e3e8; box-sizing:border-box; font-size:1rem; }
    button { grid-column:1 / -1; padding:12px 16px; background:#1877f2; color:#fff; border:none; border-radius:8px; cursor:pointer; font-weight:600; font-size: 1.1rem; }
    .recommendation { background:#eaf5ff; border:1px solid #d0eaff; border-radius:8px; padding:1rem; text-align:center; margin-bottom:1rem; }
    .price { font-size:1.8rem; font-weight:700; color:#0a58d6; }
    .auction-list { list-style:none; padding:0; margin:0; }
    .auction-item { display:flex; justify-content:space-between; gap:1rem; padding:0.75rem; border-radius:8px; background:#fafafa; border:1px solid #eee; margin-bottom:0.6rem; align-items:center; }
    .trend { font-size:0.9rem; color:#444; margin-top:0.5rem; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Pokétwo Price Recommender</h1>
    <form method="post">
      <div class="form-grid">
        <input class="species-input" type="text" name="species" placeholder="Enter Pokémon name..." value="{{ form_data.species }}" required>
        <select name="shiny">
          <option value="any" {% if form_data.shiny == 'any' %}selected{% endif %}>Any Shiny Status</option>
          <option value="yes" {% if form_data.shiny == 'yes' %}selected{% endif %}>Shiny Only</option>
          <option value="no" {% if form_data.shiny == 'no' %}selected{% endif %}>Non-Shiny Only</option>
        </select>
        <select name="gender">
          <option value="any" {% if form_data.gender == 'any' %}selected{% endif %}>Any Gender</option>
          <option value="Male" {% if form_data.gender == 'Male' %}selected{% endif %}>Male</option>
          <option value="Female" {% if form_data.gender == 'Female' %}selected{% endif %}>Female</option>
        </select>
        <input type="number" step="0.01" name="min_iv_total" placeholder="Min Total IV %" value="{{ form_data.min_iv_total }}">
        
        <div class="iv-grid">
            <input type="number" name="iv_hp" min="0" max="31" placeholder="HP" value="{{ form_data.iv_hp }}">
            <input type="number" name="iv_atk" min="0" max="31" placeholder="Atk" value="{{ form_data.iv_atk }}">
            <input type="number" name="iv_def" min="0" max="31" placeholder="Def" value="{{ form_data.iv_def }}">
            <input type="number" name="iv_spatk" min="0" max="31" placeholder="SpA" value="{{ form_data.iv_spatk }}">
            <input type="number" name="iv_spdef" min="0" max="31" placeholder="SpD" value="{{ form_data.iv_spdef }}">
            <input type="number" name="iv_speed" min="0" max="31" placeholder="Spe" value="{{ form_data.iv_speed }}">
        </div>

        <button type="submit">Get Price Recommendation</button>
      </div>
    </form>

    {% if request.method == "POST" %}
      <div class="results-container">
        {% if recommendation.success %}
          <div class="recommendation">
            <h2>Recommended Price Range</h2>
            <p>Based on {{ recommendation.count }} cleaned past sales ({{ recommendation.original_count }} total examined).</p>
            <div class="price">{{ "{:,.0f}".format(recommendation.conservative_bid) }} - {{ "{:,.0f}".format(recommendation.aggressive_bid) }}</div>
            <p>Median: {{ "{:,.0f}".format(recommendation.median) }} | Stdev: {{ recommendation.stdev }}</p>
            {% if recommendation.trend.direction != 'flat' %}
              <div class="trend">Trend: {{ recommendation.trend.direction }} (slope={{ recommendation.trend.slope }}, pct={{ (recommendation.trend.trend_pct * 100) | round(2) }}%)</div>
            {% endif %}
          </div>
        {% else %}
          <div class="recommendation"><h3>{{ recommendation.message }}</h3></div>
        {% endif %}

        {% if auctions_display %}
          <h3>Past Sales Data (Highest Price First)</h3>
          <ul class="auction-list">
            {% for a in auctions_display %}
              <li class="auction-item">
                <div>
                  {% if a.shiny %}✨{% endif %} <strong>{{ a.species }}</strong>
                  (Lvl {{ a.level or '?' }}, {{ '%.1f'|format(a.iv_total|float) if a.iv_total else '?' }}% IV)
                  <small style="color: #555; display: block;">
                    IVs: {{a.iv_hp or '?'}}/{{a.iv_atk or '?'}}/{{a.iv_def or '?'}}/{{a.iv_spatk or '?'}}/{{a.iv_spdef or '?'}}/{{a.iv_speed or '?'}}
                  </small>
                </div>
                <div><strong>{{ "{:,.0f}".format(a.winning_bid) if a.winning_bid else '—' }}</strong></div>
              </li>
            {% endfor %}
          </ul>
        {% endif %}
      </div>
    {% endif %}
  </div>
</body>
</html>
"""

# ------------------------------
# Routes
# ------------------------------
@app.route("/", methods=["GET", "POST"])
def index():
    auctions_display = []
    recommendation = {}

    iv_names = ['iv_hp', 'iv_atk', 'iv_def', 'iv_spatk', 'iv_spdef', 'iv_speed']
    form_data = {
        "species": request.form.get("species", "").strip(),
        "shiny": request.form.get("shiny", "any"),
        "gender": request.form.get("gender", "any"),
        "min_iv_total": request.form.get("min_iv_total", "")
    }
    for iv in iv_names:
        form_data[iv] = request.form.get(iv, "")

    if request.method == "POST" and form_data["species"]:
        # species: escaped substring (so "Pikachu" matches "Snowman Pikachu", "Surf Pikachu", etc.)
        safe_species = re.escape(form_data['species'])
        query = {"species": {"$regex": safe_species, "$options": "i"}}

        # shiny: handle 1/0, booleans and common string forms
        if form_data["shiny"] == "yes":
            query["shiny"] = {"$in": [1, True, "1", "true", "True"]}
        elif form_data["shiny"] == "no":
            query["shiny"] = {"$in": [0, False, "0", "false", "False"]}

        # gender: case-insensitive exact
        if form_data["gender"] in ("Male", "Female"):
            query["gender"] = {"$regex": f"^{re.escape(form_data['gender'])}$", "$options": "i"}

        # min iv total
        min_iv_total_str = form_data.get("min_iv_total", "").strip()
        if min_iv_total_str:
            try:
                min_iv_val = float(min_iv_total_str)
                query["iv_total"] = {"$gte": min_iv_val}
            except ValueError:
                pass

        # individual IVs
        for iv in iv_names:
            iv_val_str = form_data.get(iv, "").strip()
            if iv_val_str:
                try:
                    iv_val = int(iv_val_str)
                    if 0 <= iv_val <= 31:
                        query[iv] = {"$gte": iv_val}
                except ValueError:
                    pass

        # Recommendation & display queries should only use docs that have a winning_bid
        rec_query = dict(query)
        rec_query["winning_bid"] = {"$ne": None}

        # Chronological for recommendation (oldest -> newest).
        # Some docs have empty "timestamp" but have "created_at", so sort by both.
        rec_auctions = list(
            auctions_col.find(rec_query, {"winning_bid": 1, "timestamp": 1, "created_at": 1, "_id": 0})
                        .sort([("timestamp", 1), ("created_at", 1)])
        )

        # Display: sorted by winning_bid desc
        auctions_display = list(
            auctions_col.find(rec_query, {"_id": 0})
                        .sort("winning_bid", -1)
                        .limit(500)
        )

        # Robust parse winning_bid into ints (skip non-numeric)
        chron_bids = []
        for a in rec_auctions:
            b = a.get("winning_bid")
            try:
                chron_bids.append(int(float(b)))
            except (TypeError, ValueError):
                continue

        recommendation = get_price_recommendation(chron_bids)

    return render_template_string(HTML_TEMPLATE,
                                  form_data=form_data,
                                  auctions_display=auctions_display,
                                  recommendation=recommendation)

# Debug endpoint to inspect collection quickly on deployed app
@app.route("/_debug/sample")
def debug_sample():
    total = auctions_col.count_documents({})
    has_bid = auctions_col.count_documents({"winning_bid": {"$exists": True, "$ne": None}})
    example = auctions_col.find_one({}, {"_id": 0})
    payload = {"total_docs": total, "docs_with_winning_bid": has_bid, "example_doc": example}
    return Response(json_util.dumps(payload, indent=2), mimetype="application/json")

# ------------------------------
# Run locally (Render uses Gunicorn)
# ------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=True)
