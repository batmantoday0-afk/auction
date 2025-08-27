#!/usr/bin/env python3
"""
recommend.py - Pokemon auction price recommendation tool

Analyzes historical auction data (from parser.py output) to provide optimal buy price recommendations
based on statistical analysis of past winning bids.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AuctionAnalyzer:
    """Analyzes auction data and provides price recommendations."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._validate_database()

    def _validate_database(self):
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found: {self.db_path}")
        try:
            conn = sqlite3.connect(str(self.db_path))
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='auctions'")
            if not cur.fetchone():
                raise ValueError("Database does not contain 'auctions' table")
            conn.close()
        except Exception as e:
            raise ValueError(f"Invalid database: {e}")

    def build_query(self, filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        where: List[str] = []
        params: List[Any] = []

        if not filters.get('species'):
            raise ValueError('Species is required')
        where.append("LOWER(TRIM(species)) = LOWER(TRIM(?))")
        params.append(filters['species'])

        shiny = str(filters.get('shiny', 'any')).lower()
        if shiny in ('1', 'yes', 'y', 'true', 'shiny'):
            where.append("shiny = 1")
        elif shiny in ('0', 'no', 'n', 'false', 'normal'):
            where.append("shiny = 0")

        if filters.get('gender'):
            where.append("LOWER(TRIM(gender)) = LOWER(TRIM(?))")
            params.append(filters['gender'])

        if filters.get('min_total_iv') is not None:
            where.append("iv_total >= ?")
            params.append(float(filters['min_total_iv']))
        if filters.get('max_total_iv') is not None:
            where.append("iv_total <= ?")
            params.append(float(filters['max_total_iv']))

        if filters.get('min_level') is not None:
            where.append("level >= ?")
            params.append(int(filters['min_level']))
        if filters.get('max_level') is not None:
            where.append("level <= ?")
            params.append(int(filters['max_level']))

        if filters.get('nature'):
            where.append("LOWER(TRIM(nature)) = LOWER(TRIM(?))")
            params.append(filters['nature'])

        where.append("winning_bid IS NOT NULL")
        where.append("winning_bid > 0")

        where_clause = " AND ".join(where) if where else "1=1"

        sql = f"""
        SELECT auction_id, species, level, shiny, gender, nature,
               iv_total, winning_bid, winner_id, timestamp
        FROM auctions
        WHERE {where_clause}
        ORDER BY winning_bid ASC
        """
        return sql, params

    def get_auction_data(self, filters: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        sql, params = self.build_query(filters)
        if limit:
            sql += f" LIMIT {int(limit)}"
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def calculate_statistics(self, winning_bids: List[int]) -> Dict[str, Any]:
        if not winning_bids:
            return {}
        wb = list(map(int, winning_bids))
        wb.sort()
        if len(wb) > 3:
            mean = statistics.mean(wb)
            stdev = statistics.stdev(wb) if len(wb) > 1 else 0
            if stdev > 0:
                filtered = [x for x in wb if abs(x - mean) <= 2 * stdev]
                if len(filtered) >= max(3, int(0.7 * len(wb))):
                    wb = filtered
                    wb.sort()
        stats: Dict[str, Any] = {
            'count': len(wb),
            'min': min(wb),
            'max': max(wb),
            'mean': int(statistics.mean(wb)),
            'median': int(statistics.median(wb)),
        }
        if len(wb) >= 4:
            try:
                q = statistics.quantiles(wb, n=4, method='inclusive')
                stats.update({'q1': int(q), 'q3': int(q[2]), 'iqr': int(q[2] - q)})
            except Exception:
                pass
        return stats

    def recommend_price(self, filters: Dict[str, Any], strategy: str = 'conservative') -> Dict[str, Any]:
        try:
            auctions = self.get_auction_data(filters, limit=2000)
            if not auctions:
                return {'success': False, 'message': 'No historical auction data found for the specified criteria', 'recommendation': None}
            winning_bids = [a['winning_bid'] for a in auctions if a.get('winning_bid')]
            if not winning_bids:
                return {'success': False, 'message': 'No completed auctions found for the specified criteria', 'recommendation': None}
            stats = self.calculate_statistics(winning_bids)
            if not stats:
                return {'success': False, 'message': 'Not enough data to compute statistics', 'recommendation': None}
            if strategy == 'aggressive':
                rec = int(stats.get('q3', stats['median']))
            elif strategy == 'conservative':
                rec = max(1, (stats['median'] - int(0.25 * stats['iqr'])) if 'iqr' in stats else int(0.9 * stats['median']))
            else:
                rec = int(stats['median'])
            return {'success': True, 'recommendation': rec, 'statistics': stats, 'sample_auctions': auctions[:10], 'strategy': strategy, 'filters': filters}
        except Exception as e:
            logger.error(f"Error in price recommendation: {e}")
            return {'success': False, 'message': f'Error calculating recommendation: {str(e)}', 'recommendation': None}

    def search_auctions(self, filters: Dict[str, Any], limit: int = 50) -> List[Dict[str, Any]]:
        try:
            return self.get_auction_data(filters, limit)
        except Exception as e:
            logger.error(f"Error searching auctions: {e}")
            return []

def format_price(price: int) -> str:
    return f"{int(price):,}"

def print_recommendation_result(result: Dict[str, Any]):
    if not result.get('success'):
        print(f"❌ {result.get('message')}")
        return
    stats = result['statistics']
    filters = result['filters']
    print("\n🔍 Search Criteria:")
    print(f" Species: {filters['species']}")
    if str(filters.get('shiny','any')).lower() != 'any':
        print(f" Shiny: {'Yes' if str(filters.get('shiny')).lower() in ('1','yes','true') else 'No'}")
    if filters.get('gender'):
        print(f" Gender: {filters['gender']}")
    if filters.get('min_total_iv') is not None:
        print(f" Min Total IV: {filters['min_total_iv']}%")
    if filters.get('max_total_iv') is not None:
        print(f" Max Total IV: {filters['max_total_iv']}%")
    if filters.get('min_level') is not None:
        print(f" Min Level: {filters['min_level']}")
    if filters.get('max_level') is not None:
        print(f" Max Level: {filters['max_level']}")
    if filters.get('nature'):
        print(f" Nature: {filters['nature']}")
    print(f"\n📊 Market Analysis ({stats['count']} completed auctions):")
    print(f" Cheapest: {format_price(stats['min'])} Pokécoins")
    print(f" Most Expensive: {format_price(stats['max'])} Pokécoins")
    print(f" Average: {format_price(stats['mean'])} Pokécoins")
    print(f" Median: {format_price(stats['median'])} Pokécoins")
    if 'q1' in stats:
        print(f" 25th Percentile: {format_price(stats['q1'])} Pokécoins")
        print(f" 75th Percentile: {format_price(stats['q3'])} Pokécoins")
    print(f"\n💰 Recommended Buy Price:")
    print(f" {format_price(result['recommendation'])} Pokécoins")
    print(f" Strategy: {result['strategy'].title()}")
    print(f"\n📋 Sample of Cheapest Historical Sales:")
    for i, a in enumerate(result['sample_auctions'][:5], 1):
        shiny = "✨ " if a.get('shiny') else ""
        level = f"Lv.{a['level']} " if a.get('level') else ""
        iv = f"({a['iv_total']:.1f}% IV) " if a.get('iv_total') is not None else ""
        gender = f"{a['gender']} " if a.get('gender') else ""
        print(f" {i}. {shiny}{level}{gender}{a['species']} {iv}- {format_price(a['winning_bid'])} Pokécoins")

def main():
    ap = argparse.ArgumentParser(description='Pokemon auction price recommendation tool')
    ap.add_argument('--db', '-d', required=True, type=Path, help='Database file path')
    ap.add_argument('--species', '-s', required=True, help='Pokemon species name')
    ap.add_argument('--shiny', default='any', choices=['any','1','0','yes','no'], help='Shiny status filter')
    ap.add_argument('--gender', choices=['Male','Female'], help='Gender filter')
    ap.add_argument('--min-total-iv', dest='min_total_iv', type=float, help='Minimum total IV percentage')
    ap.add_argument('--max-total-iv', dest='max_total_iv', type=float, help='Maximum total IV percentage')
    ap.add_argument('--min-level', dest='min_level', type=int, help='Minimum level')
    ap.add_argument('--max-level', dest='max_level', type=int, help='Maximum level')
    ap.add_argument('--nature', help='Nature filter')
    ap.add_argument('--strategy', choices=['conservative','balanced','aggressive'], default='conservative', help='Bidding strategy')
    ap.add_argument('--search-only', action='store_true', help="Only search, don't recommend price")
    ap.add_argument('--limit', type=int, default=50, help='Limit for search results')
    ap.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = ap.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    analyzer = AuctionAnalyzer(args.db)
    filters: Dict[str, Any] = {
        'species': args.species,
        'shiny': args.shiny,
        'gender': args.gender,
        'min_total_iv': args.min_total_iv,
        'max_total_iv': args.max_total_iv,
        'min_level': args.min_level,
        'max_level': args.max_level,
        'nature': args.nature,
    }

    if args.search_only:
        auctions = analyzer.search_auctions(filters, args.limit)
        if not auctions:
            print('No auctions found matching your criteria.')
            return
        print(f"\n🔍 Found {len(auctions)} auctions:")
        for a in auctions:
            shiny = "✨ " if a.get('shiny') else ""
            level = f"Lv.{a['level']} " if a.get('level') else ""
            iv = f"({a['iv_total']:.1f}% IV) " if a.get('iv_total') is not None else ""
            gender = f"{a['gender']} " if a.get('gender') else ""
            price = f"{format_price(a['winning_bid'])} Pokécoins" if a.get('winning_bid') else "Current bid"
            print(f" #{a['auction_id']} - {shiny}{level}{gender}{a['species']} {iv}- {price}")
    else:
        result = analyzer.recommend_price(filters, args.strategy)
        print_recommendation_result(result)

if __name__ == '__main__':
    main()
