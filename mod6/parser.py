#!/usr/bin/env python3
"""
parser.py

Streaming, memory-safe Pokétwo auction parser that saves only '[SOLD]' auctions.
Shows a tqdm progress bar and updates sold_auctions_found as batches are committed.
Extracts sub-IVs (hp/atk/def/spatk/spdef/speed) and total IV.
"""

import argparse
import json
import re
import sqlite3
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, Optional

# progress bar
from tqdm import tqdm

# --- Logging ---
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="parser_errors.log",
    filemode="w"
)
logger = logging.getLogger(__name__)

# --- DB schema & SQL ---
AUCTIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS auctions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT UNIQUE,
    species TEXT,
    level INTEGER,
    shiny INTEGER DEFAULT 0,
    gender TEXT,
    nature TEXT,
    iv_hp INTEGER,
    iv_atk INTEGER,
    iv_def INTEGER,
    iv_spatk INTEGER,
    iv_spdef INTEGER,
    iv_speed INTEGER,
    iv_total REAL,
    winning_bid INTEGER,
    winner_id TEXT,
    seller TEXT,
    timestamp TEXT,
    title TEXT,
    raw TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_auctions_species_shiny ON auctions (species, shiny);
"""

INSERT_SQL = """
INSERT OR REPLACE INTO auctions (
    auction_id, species, level, shiny, gender, nature,
    iv_hp, iv_atk, iv_def, iv_spatk, iv_spdef, iv_speed, iv_total,
    winning_bid, winner_id, seller, timestamp, title, raw
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# ---------------- streaming JSON reader ----------------
def stream_messages_from_file(filepath: Path) -> Iterator[Dict[str, Any]]:
    """
    Memory-safe generator reading chat.json and yielding each message object found inside top-level "messages" array.
    Tolerant to small JSON irregularities (skips malformed objects).
    """
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        obj_buffer = ""
        brace_level = 0
        in_messages_array = False
        found_start = False

        while True:
            chunk = f.read(128 * 1024)
            if not chunk:
                break
            for ch in chunk:
                if not in_messages_array:
                    temp = (obj_buffer[-20:] if len(obj_buffer) > 20 else obj_buffer) + ch
                    if '"messages": [' in temp and not found_start:
                        in_messages_array = True
                        found_start = True
                        obj_buffer = ""
                    else:
                        obj_buffer = temp
                    continue

                if ch == "{":
                    if brace_level == 0:
                        obj_buffer = ch
                    else:
                        obj_buffer += ch
                    brace_level += 1
                elif ch == "}":
                    if brace_level > 0:
                        brace_level -= 1
                        obj_buffer += ch
                        if brace_level == 0 and obj_buffer:
                            try:
                                yield json.loads(obj_buffer)
                            except json.JSONDecodeError:
                                logger.warning("Skipping malformed JSON object fragment: %s", obj_buffer[:200].replace("\n"," "))
                            obj_buffer = ""
                elif brace_level > 0:
                    obj_buffer += ch

# ---------------- helpers & regexes ----------------
def clean_number(text: str) -> Optional[int]:
    if not text:
        return None
    s = re.sub(r"[^0-9]", "", str(text))
    return int(s) if s else None

AUCTION_TITLE_RE = re.compile(r"Auction\s*#\s*(\d+)", re.IGNORECASE)
LEVEL_RE = re.compile(r"Level\s*[:\s]*?(\d{1,3})", re.IGNORECASE)
SHINY_RE = re.compile(r"✨|\bshiny\b", re.IGNORECASE)
TOTAL_IV_RE = re.compile(r"Total\s*IV[:\*\s]*([0-9]+(?:\.[0-9]+)?)\s*%?", re.IGNORECASE)
WINNING_BID_RE = re.compile(r"Winning\s*Bid[^0-9\n\r]*([0-9][0-9,]*)", re.IGNORECASE)
WINNER_RE = re.compile(r"Winner[:`\s]*<@!?(\d+)>", re.IGNORECASE)

# sub-IV patterns tolerant to formatting differences
SUBIV_PATTERNS = {
    "iv_hp": re.compile(r"HP[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
    "iv_atk": re.compile(r"Attack[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
    "iv_def": re.compile(r"Defense[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
    "iv_spatk": re.compile(r"Sp\.?\s*Atk[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
    "iv_spdef": re.compile(r"Sp\.?\s*Def[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
    "iv_speed": re.compile(r"Speed[:\s]*.*?IV[:\s]*([0-9]{1,2})/31", re.IGNORECASE | re.DOTALL),
}

def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace('\u200b', '')
    s = re.sub(r"[*_`~]", "", s)
    return s.strip()

# ---------------- extraction (only [SOLD]) ----------------
def extract_auction_data(embed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process embed only if title starts with [SOLD] (case-insensitive).
    Returns dict ready for DB insertion (keys match INSERT_SQL order).
    """
    try:
        if not isinstance(embed, dict):
            return None
        title = embed.get("title") or ""
        if not isinstance(title, str):
            return None

        # Only accept embeds whose title starts with [SOLD] (case-insensitive)
        if not title.upper().strip().startswith("[SOLD]"):
            return None

        # Auction ID required
        aid_m = AUCTION_TITLE_RE.search(title)
        if not aid_m:
            return None
        auction_id = aid_m.group(1)

        # species extraction: prefer '•' split like '[SOLD] ... • Species'
        species = None
        if "•" in title:
            species_part = title.split("•")[-1]
            species = re.sub(r"Level\s*\d+", "", species_part, flags=re.IGNORECASE).strip()
            species = species.replace("✨", "").strip()
        else:
            # fallback: last segment after '-' or ':'
            species_part = re.split(r"[-:]", title)[-1].strip()
            species = re.sub(r"Level\s*\d+", "", species_part, flags=re.IGNORECASE).strip()
            species = species.replace("✨", "").strip()

        if not species:
            return None

        # shiny detection
        shiny = 1 if SHINY_RE.search(title) else 0

        # level if present
        level = int(LEVEL_RE.search(title).group(1)) if LEVEL_RE.search(title) else None

        # initialize outputs
        iv_hp = iv_atk = iv_def = iv_spatk = iv_spdef = iv_speed = iv_total = None
        winning_bid = None
        winner_id = None
        nature = None
        gender = None

        # parse fields
        for fld in embed.get("fields", []) or []:
            fname = (fld.get("name") or "").lower()
            fval_raw = fld.get("value") or ""
            fval = clean_text(fval_raw)

            # Pokémon details - sub IVs and total IV, gender, nature
            if "pokemon" in fname or "pokémon" in fname or "details" in fname:
                # total iv
                m_tot = TOTAL_IV_RE.search(fval)
                if m_tot:
                    try:
                        iv_total = float(m_tot.group(1))
                    except Exception:
                        iv_total = None

                # sub IVs
                for key, pat in SUBIV_PATTERNS.items():
                    if m := pat.search(fval):
                        try:
                            val = int(m.group(1))
                        except:
                            val = None
                        if key == "iv_hp":
                            iv_hp = val
                        elif key == "iv_atk":
                            iv_atk = val
                        elif key == "iv_def":
                            iv_def = val
                        elif key == "iv_spatk":
                            iv_spatk = val
                        elif key == "iv_spdef":
                            iv_spdef = val
                        elif key == "iv_speed":
                            iv_speed = val

                # gender / nature best-effort
                if (m := re.search(r"Gender[:\s]*([MFmf♂♀\w]+)", fval, re.IGNORECASE)):
                    gender = m.group(1).strip()
                if (m := re.search(r"Nature[:\s]*([A-Za-z\-]+)", fval, re.IGNORECASE)):
                    nature = m.group(1).strip()

            # auction/winning info
            if "auction" in fname or "details" in fname or "winning" in fname:
                # winning bid
                if m := WINNING_BID_RE.search(fval):
                    winning_bid = clean_number(m.group(1))
                else:
                    # fallback: find a number followed by "Pokécoins" or similar
                    if m2 := re.search(r"([0-9,]+)\s*Pok[eé]coins", fval, re.IGNORECASE):
                        winning_bid = clean_number(m2.group(1))

                # winner id or name
                if m := WINNER_RE.search(fval):
                    winner_id = m.group(1)
                else:
                    if mname := re.search(r"(?:Winner|Bidder)[:\s]*@?([^\n\r,]+)", fval, re.IGNORECASE):
                        winner_id = mname.group(1).strip()

        return {
            "auction_id": auction_id,
            "species": species,
            "level": level,
            "shiny": shiny,
            "gender": gender,
            "nature": nature,
            "iv_hp": iv_hp,
            "iv_atk": iv_atk,
            "iv_def": iv_def,
            "iv_spatk": iv_spatk,
            "iv_spdef": iv_spdef,
            "iv_speed": iv_speed,
            "iv_total": iv_total,
            "winning_bid": winning_bid,
            "winner_id": winner_id,
            "seller": (embed.get("author") or {}).get("name"),
            "timestamp": embed.get("timestamp"),
            "title": title,
            "raw": json.dumps(embed, ensure_ascii=False)
        }
    except Exception as e:
        logger.error("Error parsing embed: %s | title=%s", e, embed.get("title", "N/A"), exc_info=True)
        return None

# ---------------- main processing loop ----------------
def process_file(input_path: Path, db_path: Path, batch_size: int = 10000, verbose: bool = False):
    """
    Stream messages, extract SOLD auctions and insert to DB in batches.
    Shows a tqdm progress bar when verbose==True and updates postfix with sold_auctions_found.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.executescript(AUCTIONS_SCHEMA)
        cur = conn.cursor()
    except sqlite3.Error as e:
        print(f"FATAL: Database connection failed: {e}")
        return

    inserted_count = 0
    batch = []

    print(f"Starting memory-safe processing of '{input_path.name}' (SOLD auctions only).")
    message_stream = stream_messages_from_file(input_path)

    with tqdm(desc="Processing messages", unit=" messages", disable=not verbose) as pbar:
        for message in message_stream:
            pbar.update(1)
            embeds = message.get("embeds") or []
            for embed in embeds:
                if not isinstance(embed, dict):
                    continue
                auction_data = extract_auction_data(embed)
                if auction_data:
                    # order matches INSERT_SQL
                    batch.append(tuple(auction_data.get(k) for k in [
                        'auction_id', 'species', 'level', 'shiny', 'gender', 'nature',
                        'iv_hp', 'iv_atk', 'iv_def', 'iv_spatk', 'iv_spdef', 'iv_speed',
                        'iv_total', 'winning_bid', 'winner_id', 'seller', 'timestamp',
                        'title', 'raw'
                    ]))

            if len(batch) >= batch_size:
                try:
                    cur.executemany(INSERT_SQL, batch)
                    conn.commit()
                    inserted_count += len(batch)
                    pbar.set_postfix({"sold_auctions_found": inserted_count})
                except sqlite3.Error as e:
                    logger.error("Database batch insert error: %s", e, exc_info=True)
                    try:
                        conn.rollback()
                    except:
                        pass
                finally:
                    batch = []

    # final flush
    if batch:
        try:
            cur.executemany(INSERT_SQL, batch)
            conn.commit()
            inserted_count += len(batch)
        except sqlite3.Error as e:
            logger.error("Final database batch insert error: %s", e, exc_info=True)

    conn.close()
    print("\n---")
    print("✅ Processing complete.")
    print(f"   SOLD auctions saved or updated: {inserted_count}")
    print("   See 'parser_errors.log' for any issues encountered.")

# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="Parser that saves only [SOLD] auctions (streaming, memory-safe).")
    parser.add_argument("--input", "-i", required=True, type=Path, help="Path to chat.json")
    parser.add_argument("--db", "-d", required=True, type=Path, help="Path to output SQLite DB")
    parser.add_argument("--batch-size", "-b", type=int, default=10000, help="DB insert batch size")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show live progress bar")
    args = parser.parse_args()

    if not args.input.exists():
        print(f"FATAL: Input file not found at '{args.input}'")
        return

    process_file(args.input, args.db, batch_size=args.batch_size, verbose=args.verbose)

if __name__ == "__main__":
    main()
