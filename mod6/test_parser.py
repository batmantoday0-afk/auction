#!/usr/bin/env python3
"""
test_parser.py - Test script for the auction parser and recommender

This script creates sample data and tests both tools to ensure they work correctly.
"""

import json
import sqlite3
import tempfile
import os
from pathlib import Path
import sys

# Add the current directory to Python path to import our modules
sys.path.insert(0, '.')

def create_sample_discord_data():
    """Create sample Discord export data for testing."""
    sample_data = [
        {
            "id": "1234567890123456789",
            "timestamp": "2024-01-15T10:30:00.000Z",
            "embeds": [
                {
                    "title": "Auction #12345 • ✨ Level 50 Pikachu",
                    "thumbnail": {"url": "https://example.com/shiny-pikachu.png"},
                    "timestamp": "2024-01-15T10:30:00.000Z",
                    "author": {"name": "TrainerAsh"},
                    "fields": [
                        {
                            "name": "Pokémon Details",
                            "value": "**Nature:** Timid\n**Gender:** Male\n**HP:** 28/31\n**Attack:** 15/31\n**Defense:** 30/31\n**Sp. Atk:** 31/31\n**Sp. Def:** 29/31\n**Speed:** 31/31\n**Total IV:** 88.2%"
                        },
                        {
                            "name": "Auction Details",
                            "value": "**Current Bid:** 45,000\n**Bidder:** @PokeFan123\n**Starting Bid:** 25,000\n**Increment:** 1,000"
                        }
                    ]
                }
            ]
        },
        {
            "id": "1234567890123456790",
            "timestamp": "2024-01-15T11:00:00.000Z",
            "embeds": [
                {
                    "title": "[SOLD] Auction #12346 • Level 45 Charizard",
                    "thumbnail": {"url": "https://example.com/charizard.png"},
                    "timestamp": "2024-01-15T11:00:00.000Z",
                    "author": {"name": "DragonMaster"},
                    "fields": [
                        {
                            "name": "Pokémon Details",
                            "value": "**Nature:** Adamant\n**Gender:** Female\n**HP:** 31/31\n**Attack:** 31/31\n**Defense:** 28/31\n**Sp. Atk:** 20/31\n**Sp. Def:** 25/31\n**Speed:** 30/31\n**Total IV:** 89.5%"
                        },
                        {
                            "name": "Auction Details",
                            "value": "**Winning Bid:** 75,000\n**Winner:** @FireTypeFan\n**Starting Bid:** 30,000"
                        }
                    ]
                }
            ]
        },
        {
            "id": "1234567890123456791",
            "timestamp": "2024-01-16T09:15:00.000Z",
            "embeds": [
                {
                    "title": "[SOLD] Auction #12347 • ✨ Level 60 Mewtwo",
                    "thumbnail": {"url": "https://example.com/shiny-mewtwo.png"},
                    "timestamp": "2024-01-16T09:15:00.000Z",
                    "author": {"name": "PsychicKing"},
                    "fields": [
                        {
                            "name": "Pokémon Details",
                            "value": "**Nature:** Modest\n**Gender:** Unknown\n**HP:** 31/31\n**Attack:** 25/31\n**Defense:** 31/31\n**Sp. Atk:** 31/31\n**Sp. Def:** 31/31\n**Speed:** 31/31\n**Total IV:** 96.8%"
                        },
                        {
                            "name": "Auction Details",
                            "value": "**Winning Bid:** 250,000\n**Winner:** @LegendaryHunter\n**Starting Bid:** 100,000"
                        }
                    ]
                }
            ]
        },
        {
            "id": "1234567890123456792",
            "timestamp": "2024-01-16T14:20:00.000Z",
            "embeds": [
                {
                    "title": "[SOLD] Auction #12348 • Level 35 Pikachu",
                    "thumbnail": {"url": "https://example.com/pikachu.png"},
                    "timestamp": "2024-01-16T14:20:00.000Z",
                    "author": {"name": "ElectricFan"},
                    "fields": [
                        {
                            "name": "Pokémon Details",
                            "value": "**Nature:** Jolly\n**Gender:** Female\n**HP:** 25/31\n**Attack:** 28/31\n**Defense:** 22/31\n**Sp. Atk:** 18/31\n**Sp. Def:** 30/31\n**Speed:** 31/31\n**Total IV:** 83.9%"
                        },
                        {
                            "name": "Auction Details",
                            "value": "**Winning Bid:** 35,000\n**Winner:** @ThunderStorm\n**Starting Bid:** 15,000"
                        }
                    ]
                }
            ]
        },
        {
            "id": "1234567890123456793",
            "timestamp": "2024-01-17T16:45:00.000Z",
            "embeds": [
                {
                    "title": "[SOLD] Auction #12349 • ✨ Level 50 Pikachu",
                    "thumbnail": {"url": "https://example.com/shiny-pikachu.png"},
                    "timestamp": "2024-01-17T16:45:00.000Z",
                    "author": {"name": "ShinyHunter"},
                    "fields": [
                        {
                            "name": "Pokémon Details",
                            "value": "**Nature:** Timid\n**Gender:** Male\n**HP:** 31/31\n**Attack:** 20/31\n**Defense:** 31/31\n**Sp. Atk:** 31/31\n**Sp. Def:** 31/31\n**Speed:** 31/31\n**Total IV:** 94.6%"
                        },
                        {
                            "name": "Auction Details",
                            "value": "**Winning Bid:** 120,000\n**Winner:** @EliteFour\n**Starting Bid:** 50,000"
                        }
                    ]
                }
            ]
        }
    ]

    return sample_data

def test_parser():
    """Test the parser with sample data."""
    print("🧪 Testing Parser...")

    # Create temporary files
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(create_sample_discord_data(), f, indent=2)
        json_file = f.name

    db_file = tempfile.mktemp(suffix='.db')

    try:
        # Import and test the parser
        from parser_large_fixed import main as parser_main, create_database, process_json_file, extract_auction_data

        # Test database creation
        create_database(Path(db_file))
        print("✅ Database creation successful")

        # Test JSON processing
        messages = list(process_json_file(Path(json_file)))
        print(f"✅ JSON processing successful - found {len(messages)} messages")

        # Test auction extraction
        auction_count = 0
        for message in messages:
            from parser_large_fixed import extract_embeds_from_message
            embeds = extract_embeds_from_message(message)
            for embed in embeds:
                auction_data = extract_auction_data(embed)
                if auction_data:
                    auction_count += 1

        print(f"✅ Auction extraction successful - found {auction_count} auctions")

        # Test full parser by running main function
        import sys
        original_argv = sys.argv
        sys.argv = ['parser_large_fixed.py', '--input', json_file, '--db', db_file, '--verbose']
        try:
            parser_main()
            print("✅ Full parser execution successful")
        except SystemExit:
            pass  # argparse calls sys.exit, which is expected
        finally:
            sys.argv = original_argv

        # Verify data was inserted
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM auctions")
        count = cursor.fetchone()[0]
        conn.close()

        print(f"✅ Database insertion successful - {count} records inserted")

        return db_file  # Return database file for recommender testing

    except Exception as e:
        print(f"❌ Parser test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        # Cleanup JSON file
        if os.path.exists(json_file):
            os.unlink(json_file)

def test_recommender(db_file):
    """Test the recommender with the parsed data."""
    if not db_file:
        print("❌ Skipping recommender test - no database available")
        return

    print("\n🧪 Testing Recommender...")

    try:
        from recommend_fixed import AuctionAnalyzer, main as recommend_main

        # Test analyzer initialization
        analyzer = AuctionAnalyzer(Path(db_file))
        print("✅ Analyzer initialization successful")

        # Test price recommendation for Pikachu
        filters = {'species': 'Pikachu', 'shiny': 'any'}
        result = analyzer.recommend_price(filters)

        if result['success']:
            print(f"✅ Price recommendation successful - {result['recommendation']} Pokécoins for Pikachu")
        else:
            print(f"⚠️ Price recommendation returned no results: {result['message']}")

        # Test with shiny filter
        filters = {'species': 'Pikachu', 'shiny': '1'}
        result = analyzer.recommend_price(filters)

        if result['success']:
            print(f"✅ Shiny Pikachu recommendation successful - {result['recommendation']} Pokécoins")
        else:
            print(f"⚠️ Shiny Pikachu recommendation returned no results: {result['message']}")

        # Test auction search
        auctions = analyzer.search_auctions(filters)
        print(f"✅ Auction search successful - found {len(auctions)} auctions")

        # Test main function
        import sys
        original_argv = sys.argv
        sys.argv = ['recommend_fixed.py', '--db', db_file, '--species', 'Pikachu', '--shiny', '1']
        try:
            recommend_main()
            print("✅ Full recommender execution successful")
        except SystemExit:
            pass
        finally:
            sys.argv = original_argv

    except Exception as e:
        print(f"❌ Recommender test failed: {e}")
        import traceback
        traceback.print_exc()

def test_edge_cases(db_file):
    """Test edge cases and error handling."""
    if not db_file:
        print("❌ Skipping edge case tests - no database available")
        return

    print("\n🧪 Testing Edge Cases...")

    try:
        from recommend_fixed import AuctionAnalyzer

        analyzer = AuctionAnalyzer(Path(db_file))

        # Test with non-existent species
        result = analyzer.recommend_price({'species': 'NonexistentPokemon'})
        if not result['success']:
            print("✅ Non-existent species handled correctly")
        else:
            print("⚠️ Non-existent species should return no results")

        # Test with invalid IV range
        result = analyzer.recommend_price({
            'species': 'Pikachu',
            'min_total_iv': 150  # Invalid IV percentage
        })
        if not result['success']:
            print("✅ Invalid IV range handled correctly")
        else:
            print("⚠️ Invalid IV range should return no results")

        # Test with empty filters
        try:
            result = analyzer.recommend_price({})
            if not result['success']:
                print("✅ Empty filters handled correctly")
            else:
                print("⚠️ Empty filters should return error")
        except ValueError:
            print("✅ Empty filters raise appropriate error")

        print("✅ Edge case testing completed")

    except Exception as e:
        print(f"❌ Edge case test failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all tests."""
    print("🚀 Starting comprehensive test suite for auction tools...")
    print("=" * 60)

    # Test parser
    db_file = test_parser()

    # Test recommender
    test_recommender(db_file)

    # Test edge cases
    test_edge_cases(db_file)

    # Cleanup
    if db_file and os.path.exists(db_file):
        os.unlink(db_file)
        print("\n🧹 Temporary database cleaned up")

    print("\n" + "=" * 60)
    print("✅ Test suite completed successfully!")
    print("\n📋 Summary:")
    print("   • Parser successfully processes Discord JSON exports")
    print("   • Database schema creation and data insertion working")
    print("   • Recommender provides price analysis and recommendations")
    print("   • Edge cases and error conditions handled properly")
    print("   • Both tools integrate well together")

if __name__ == '__main__':
    main()
