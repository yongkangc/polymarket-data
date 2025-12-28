"""
Research script to discover up/down markets on Polymarket.
Validates our market discovery approach.
"""
import requests
from datetime import datetime
import json


def explore_api():
    """Explore Polymarket API to understand up/down market structure"""

    print("="*70)
    print("POLYMARKET UP/DOWN MARKET RESEARCH")
    print("="*70)

    # 1. Search for up/down markets
    print("\n1. Searching for 'up down' markets...")
    search_url = "https://gamma-api.polymarket.com/events"
    params = {
        "limit": 20,
        "closed": "false",
        "order": "new"
    }

    resp = requests.get(search_url, params=params, timeout=10)
    resp.raise_for_status()
    events = resp.json()

    print(f"   Retrieved {len(events)} active events")

    # 2. Filter for up/down markets
    print("\n2. Filtering for up/down markets...")
    updown_markets = []

    for event in events:
        question = event.get("question", "").lower()
        title = event.get("title", "").lower()

        # Check if it's an up/down market
        has_updown = ("up" in question or "up" in title) and \
                     ("down" in question or "down" in title)

        if has_updown:
            # Check for our target assets
            for asset in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol"]:
                if asset in question or asset in title:
                    updown_markets.append({
                        "id": event.get("id"),
                        "question": event.get("question"),
                        "title": event.get("title"),
                        "slug": event.get("slug"),
                        "end_date": event.get("endDate"),
                        "markets": event.get("markets", [])
                    })
                    break

    print(f"   Found {len(updown_markets)} up/down markets for BTC/SOL/ETH")

    # 3. Analyze market structure
    print("\n3. Analyzing market structure...")
    if updown_markets:
        print("\n   Sample markets:")
        for i, market in enumerate(updown_markets[:5], 1):
            question_text = market['question'] or market['title'] or "Unknown"
            print(f"\n   [{i}] {question_text}")
            print(f"       ID: {market['id']}")
            print(f"       Slug: {market['slug']}")
            print(f"       End: {market['end_date']}")

            # Check if it's 15min or 1hr
            q = question_text.lower() if question_text else ""
            slug = market['slug'].lower() if market['slug'] else ""
            combined = f"{q} {slug}"

            if "15" in combined or "fifteen" in combined:
                duration = "15min"
            elif "5m" in slug or "5 m" in combined:
                duration = "5min"
            elif "1 h" in combined or "one h" in combined or "1h" in combined or "-1h-" in slug:
                duration = "1hr"
            else:
                duration = "unknown"
            print(f"       Duration: {duration}")

            # Extract token IDs
            if market['markets'] and len(market['markets']) > 0:
                m = market['markets'][0]
                print(f"       Market ID: {m.get('id')}")
                print(f"       Token IDs: {m.get('clobTokenIds')}")

    # 4. Try tag-based search
    print("\n4. Searching by tags...")

    # Known tag IDs to try (based on Polymarket structure)
    test_tags = [
        102467,  # BTC price markets
        # Need to discover up/down market tags
    ]

    for tag_id in test_tags:
        try:
            tag_url = f"https://gamma-api.polymarket.com/events?tag_id={tag_id}&limit=5&closed=false"
            resp = requests.get(tag_url, timeout=10)
            resp.raise_for_status()
            tag_events = resp.json()

            if tag_events:
                print(f"\n   Tag {tag_id}: {len(tag_events)} events")
                sample = tag_events[0]
                print(f"   Sample: {sample.get('question')}")
        except Exception as e:
            print(f"   Tag {tag_id}: Error - {e}")

    # 5. Check market URLs
    print("\n5. URL patterns:")
    if updown_markets:
        for market in updown_markets[:3]:
            slug = market['slug']
            url = f"https://polymarket.com/event/{slug}"
            print(f"   {url}")

    # 6. Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    print(f"Total up/down markets found: {len(updown_markets)}")

    # Count by asset
    asset_counts = {"BTC": 0, "SOL": 0, "ETH": 0}
    duration_counts = {"5min": 0, "15min": 0, "1hr": 0, "other": 0}

    for market in updown_markets:
        question_text = market['question'] or market['title'] or ""
        slug = market['slug'] or ""
        combined = f"{question_text} {slug}".lower()

        # Count assets
        if "bitcoin" in combined or "btc" in combined:
            asset_counts["BTC"] += 1
        if "solana" in combined or "sol" in combined:
            asset_counts["SOL"] += 1
        if "ethereum" in combined or "eth" in combined:
            asset_counts["ETH"] += 1

        # Count durations
        if "5m" in combined or "5 m" in combined:
            duration_counts["5min"] += 1
        elif "15" in combined or "fifteen" in combined:
            duration_counts["15min"] += 1
        elif "1 h" in combined or "one h" in combined or "1h" in combined or "-1h-" in slug:
            duration_counts["1hr"] += 1
        else:
            duration_counts["other"] += 1

    print(f"\nBy Asset:")
    for asset, count in asset_counts.items():
        print(f"  {asset}: {count}")

    print(f"\nBy Duration:")
    for duration, count in duration_counts.items():
        print(f"  {duration}: {count}")

    # 7. Save sample data
    print("\n6. Saving sample data...")
    with open("data/updown_markets_sample.json", "w") as f:
        json.dump(updown_markets, f, indent=2)
    print("   Saved to: data/updown_markets_sample.json")

    print("\n" + "="*70)
    print("✅ Research complete!")
    print("="*70)

    return updown_markets


if __name__ == "__main__":
    try:
        markets = explore_api()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
