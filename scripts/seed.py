import argparse
import asyncio
import random
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

EVENT_TYPES = ["pageview", "click", "form_submit", "signup", "conversion"]
EVENT_TYPE_WEIGHTS = [55, 25, 8, 7, 5]

PAGES: list[tuple[str, str]] = [
    ("https://www.pokemart.example/", "PokéMart — Gotta Shop 'Em All"),
    ("https://www.pokemart.example/trainer-club", "Join the Trainer Club"),
    ("https://www.pokemart.example/blog/kanto-starter-guide", "Kanto Starter Guide"),
    ("https://www.pokemart.example/blog/shiny-hunting-tips", "Shiny Hunting Tips"),
    ("https://www.pokemart.example/docs/type-chart", "Type Effectiveness Chart"),
    ("https://shop.pokemart.example/products/poke-ball", "Poké Ball"),
    ("https://shop.pokemart.example/products/ultra-ball", "Ultra Ball"),
    ("https://shop.pokemart.example/products/master-ball", "Master Ball"),
    ("https://shop.pokemart.example/products/hyper-potion", "Hyper Potion"),
    ("https://shop.pokemart.example/products/rare-candy", "Rare Candy"),
    ("https://shop.pokemart.example/products/porygon-plush", "Porygon Plush"),
    ("https://shop.pokemart.example/products/squirtle-plush", "Squirtle Plush"),
    ("https://shop.pokemart.example/cart", "Your Bag"),
    ("https://shop.pokemart.example/checkout", "Checkout"),
]

BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
BROWSER_WEIGHTS = [50, 25, 15, 10]
DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [55, 38, 7]
DEVICE_OS = {
    "desktop": ["macOS", "Windows", "Linux"],
    "mobile": ["iOS", "Android"],
    "tablet": ["iOS", "Android"],
}
REFERRERS = [
    "https://www.google.com/",
    "https://www.facebook.com/",
    "https://duckduckgo.com/",
    "https://dextok.com/",
    "direct",
]
UTM_SOURCES = ["google", "facebook", "newsletter", "pokegear"]
UTM_MEDIUMS = ["cpc", "social", "email", "organic"]
CAMPAIGNS = [
    "team-rocket-retargeting",
    "Safari Zone Sale",
    "gotta-catch-em-all",
    "Raid First-Aid Kit",
]
CLICK_TARGETS = [
    ("cta-hero", "Shop Poké Balls"),
    ("add-to-cart", "Add to Bag"),
    ("nav-deals", "Today's Deals"),
    ("footer-newsletter", "Join the Trainer Club"),
]
FORMS = ["trainer-club-signup", "master-support-contact", "restock-alert"]
PLANS = ["casual", "ace-trainer", "champions-club"]
SIGNUP_METHODS = ["email", "pokegear", "trainer-id"]

TRAINERS = [
    "red",
    "ethan",
    "kris",
    "lyra",
    "brendan",
    "may",
    "lucas",
    "dawn",
    "hilbert",
    "hilda",
    "nate",
    "rosa",
    "calem",
    "serena",
    "elio",
    "selene",
    "victor",
    "gloria",
    "florian",
    "juliana",
]
USER_POOL = [f"trainer_{TRAINERS[i % len(TRAINERS)]}_{i:04d}" for i in range(1, 151)]
USER_WEIGHTS = [8] * 10 + [1] * 140

# (max_age_s, min_age_s, weight) — mostly historical for stats buckets,
# with recent bands so the realtime windows (60/300/900s) have data
TIME_BANDS = [
    (14 * 86400, 86400, 70),
    (86400, 3600, 15),
    (3600, 900, 5),
    (900, 300, 4),
    (300, 60, 3),
    (60, 5, 3),
]

MAX_ATTEMPTS = 8


def _timestamp(rng: random.Random, now: datetime) -> datetime:
    max_age, min_age, _ = rng.choices(TIME_BANDS, weights=[b[2] for b in TIME_BANDS])[0]
    return now - timedelta(seconds=rng.uniform(min_age, max_age))


def _metadata(rng: random.Random, event_type: str, page_title: str) -> dict[str, Any]:
    device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS)[0]
    md: dict[str, Any] = {
        "browser": rng.choices(BROWSERS, weights=BROWSER_WEIGHTS)[0],
        "os": rng.choice(DEVICE_OS[device]),
        "device_type": device,
        "referrer": rng.choice(REFERRERS),
        "page_title": page_title,
    }
    if rng.random() < 0.6:
        md["utm_source"] = rng.choice(UTM_SOURCES)
        md["utm_medium"] = rng.choice(UTM_MEDIUMS)
        md["utm_campaign"] = rng.choice(CAMPAIGNS)
    match event_type:
        case "pageview":
            md["load_time_ms"] = rng.randint(80, 2500)
        case "click":
            md["element_id"], md["element_text"] = rng.choice(CLICK_TARGETS)
        case "form_submit":
            md["form_id"] = rng.choice(FORMS)
        case "signup":
            md["plan"] = rng.choice(PLANS)
            md["signup_method"] = rng.choice(SIGNUP_METHODS)
        case "conversion":
            md["order_value"] = rng.randint(200, 9800)
            md["currency"] = "PKD"
            md["item_count"] = rng.randint(1, 5)
    return md


def build_events(rng: random.Random, count: int, now: datetime) -> list[dict[str, Any]]:
    events = []
    for _ in range(count):
        event_type = rng.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS)[0]
        url, title = rng.choice(PAGES)
        events.append(
            {
                "event_type": event_type,
                "timestamp": _timestamp(rng, now).isoformat(),
                "user_id": rng.choices(USER_POOL, weights=USER_WEIGHTS)[0],
                "source_url": url,
                "metadata": _metadata(rng, event_type, title),
            }
        )
    return events


async def _post(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    event: dict[str, Any],
    throttled: Counter,
) -> None:
    async with sem:
        for _ in range(MAX_ATTEMPTS):
            resp = await client.post("/events", json=event)
            if resp.status_code == 202:
                return
            if resp.status_code in (429, 503):
                delay = max(int(resp.headers.get("Retry-After", "1")), 1)
                if not throttled:
                    print(
                        f"throttled ({resp.status_code}), retrying in {delay}s",
                        file=sys.stderr,
                    )
                throttled[resp.status_code] += 1
                await asyncio.sleep(delay)
                continue
            raise RuntimeError(f"POST /events -> {resp.status_code}: {resp.text}")
        raise RuntimeError(
            f"POST /events still throttled after {MAX_ATTEMPTS} attempts"
        )


async def seed(
    base_url: str, count: int, seed_value: int, concurrency: int
) -> tuple[Counter, Counter]:
    rng = random.Random(seed_value)
    events = build_events(rng, count, datetime.now(UTC))
    sem = asyncio.Semaphore(concurrency)
    throttled: Counter = Counter()
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        await asyncio.gather(*(_post(client, sem, e, throttled) for e in events))
    return Counter(e["event_type"] for e in events), throttled


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed realistic events through the real POST /events endpoint"
    )
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--concurrency", type=int, default=32)
    args = parser.parse_args()

    started = time.monotonic()
    counts, throttled = asyncio.run(
        seed(args.url, args.count, args.seed, args.concurrency)
    )
    elapsed = time.monotonic() - started

    print(f"seeded {sum(counts.values())} events in {elapsed:.1f}s")
    for event_type, n in counts.most_common():
        print(f"  {event_type:<12} {n}")
    if throttled:
        detail = ", ".join(f"{n}x {code}" for code, n in sorted(throttled.items()))
        print(f"  (throttled and retried: {detail})")
    print()
    print("try:")
    print(f"  curl '{args.url}/events?type=conversion&limit=3'")
    print(f"  curl '{args.url}/events/stats?bucket=day'")
    print(f"  curl '{args.url}/events/search?q=porygon'")
    print(f"  curl '{args.url}/events/stats/realtime?window=300'")


if __name__ == "__main__":
    main()
