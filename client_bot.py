import asyncio
import aiohttp
import re
import os
from datetime import datetime, date, timedelta
import pytz

# ── Config ──────────────────────────────────────────────────────────────────
TOKEN = os.environ["DISCORD_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
CLIENT_CHANNEL_ID = os.environ["CLIENT_CHANNEL_ID"]
CLIENT_GUILD_ID = os.environ["CLIENT_GUILD_ID"]

# Offgrid picks channel — where we read picks from
PICKS_CHANNEL_ID = "1466857635746808020"

EST = pytz.timezone("US/Eastern")
PST = pytz.timezone("US/Pacific")
CHECK_INTERVAL = 20
DISCORD_API = "https://discord.com/api/v10"
DISCORD_HEADERS = {"Authorization": f"Bot {TOKEN}", "Content-Type": "application/json"}
SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
SUPABASE_UPSERT_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}

EST_now = lambda: datetime.now(EST)
TIMEOUT = aiohttp.ClientTimeout(total=10)

# ── League emoji mapping ──────────────────────────────────────────────────────
LEAGUE_MAP = {
    "🌐": "TT CUP",
    "🇨🇿": "CZECH",
    "🇵🇱": "ELITE",
    "🇺🇦": "SETKA",
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def detect_league(text: str) -> str | None:
    """Detect league from emoji in pick text."""
    for emoji, league in LEAGUE_MAP.items():
        if emoji in text:
            return league
    return None


def detect_units(text: str) -> str:
    """Detect unit size from pick text."""
    match = re.search(r'(\d+\.?\d*)U\b', text, re.IGNORECASE)
    if match:
        return f"{match.group(1)}U"
    return "1U"


def detect_pick_type(text: str) -> str:
    """Detect OVER or UNDER from pick text."""
    if re.search(r'\bUNDER\b', text, re.IGNORECASE):
        return "UNDER"
    return "OVER"


def build_client_message(row: dict, match_dt: datetime) -> str:
    """Build the formatted message for the client's channel."""
    league = row.get("league", "TT")
    player1 = row["player1"]
    player2 = row["player2"]
    pick_text = row.get("pick", "")

    units = detect_units(pick_text)
    pick_type = detect_pick_type(pick_text)

    pst_dt = match_dt.astimezone(PST)
    est_str = match_dt.strftime("%I:%M %p EST").lstrip("0")
    pst_str = pst_dt.strftime("%I:%M %p PST").lstrip("0")

    return f"{league} – {player1} vs {player2} {pick_type} {units} @ {est_str} / {pst_str}"


# ── Supabase helpers ──────────────────────────────────────────────────────────

async def db_get_unposted_picks(session: aiohttp.ClientSession) -> list:
    """Get picks that have a league set but haven't been posted to clients yet."""
    now_utc = datetime.now(pytz.utc)
    to_utc = now_utc + timedelta(hours=24)
    from_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"{SUPABASE_URL}/rest/v1/picks"
        f"?select=*"
        f"&posted_to_clients=eq.false"
        f"&league=not.is.null"
        f"&match_time=gte.{from_str}"
        f"&match_time=lte.{to_str}"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            return []
        return await r.json()


async def db_mark_posted(session: aiohttp.ClientSession, alert_key: str, message_id: str):
    """Mark a pick as posted to clients and store the message ID."""
    url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{alert_key}"
    async with session.patch(url, headers=SUPABASE_HEADERS,
                             json={"posted_to_clients": True, "client_message_id": message_id}) as r:
        if r.status not in (200, 204):
            print(f"⚠️ Failed to mark posted: {r.status}")


async def db_get_picks_with_results(session: aiohttp.ClientSession) -> list:
    """Get picks that have been posted but now have results to update."""
    url = (
        f"{SUPABASE_URL}/rest/v1/picks"
        f"?select=*"
        f"&posted_to_clients=eq.true"
        f"&client_message_id=not.is.null"
        f"&alert_sent=eq.true"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            return []
        rows = await r.json()
        # Only return picks that have result emojis
        return [row for row in rows if any(e in row.get("pick", "") for e in ["✅", "❌", "💀"])]


# ── Discord helpers ──────────────────────────────────────────────────────────

async def post_to_client_channel(session: aiohttp.ClientSession, message: str) -> str | None:
    """Post a message to the client's channel and return the message ID."""
    url = f"{DISCORD_API}/channels/{CLIENT_CHANNEL_ID}/messages"
    async with session.post(url, headers=DISCORD_HEADERS, json={"content": message}) as r:
        if r.status in (200, 201):
            data = await r.json()
            return data["id"]
        else:
            text = await r.text()
            print(f"⚠️ Failed to post to client channel: {r.status} {text}")
            return None


async def edit_client_message(session: aiohttp.ClientSession, message_id: str, new_content: str):
    """Edit an existing message in the client's channel to add result."""
    url = f"{DISCORD_API}/channels/{CLIENT_CHANNEL_ID}/messages/{message_id}"
    async with session.patch(url, headers=DISCORD_HEADERS, json={"content": new_content}) as r:
        if r.status not in (200, 204):
            print(f"⚠️ Failed to edit client message: {r.status}")


async def get_channel_messages(session: aiohttp.ClientSession) -> list:
    """Fetch recent messages from Offgrid picks channel."""
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}/messages?limit=50"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            return []
        return await r.json()


# ── Pick parsing ──────────────────────────────────────────────────────────────

def parse_picks_for_client(text: str, post_date: date) -> list[dict]:
    """Parse picks that have a league emoji."""
    picks = []
    now = EST_now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    pattern = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(.+?)\s+vs\s+"
        r"(.+?)\s+"
        r"((?:OVER|UNDER|SPLIT|SplitDD|Split\s+DD|\w+\s+-\d+\.?\d*).*)$",
        re.IGNORECASE | re.MULTILINE,
    )

    for m in pattern.finditer(text):
        time_str = m.group(1).strip()
        player1 = m.group(2).strip()
        player2 = m.group(3).strip()
        pick = m.group(4).strip()

        # Only process picks with a league emoji
        league = detect_league(player1 + " " + pick)
        if not league:
            continue

        # Skip picks with result emojis
        if any(e in pick for e in ["✅", "❌", "💀"]):
            continue

        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = EST.localize(datetime(post_date.year, post_date.month, post_date.day, t.hour, t.minute))
            if t.hour < 6 and post_date == yesterday:
                next_day = post_date + timedelta(days=1)
                match_dt = EST.localize(datetime(next_day.year, next_day.month, next_day.day, t.hour, t.minute))
        except ValueError:
            continue

        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"

        # Clean player names (remove emojis)
        clean_player1 = re.sub(r'[^\w\s.]', '', player1).strip()
        clean_player2 = re.sub(r'[^\w\s.]', '', player2).strip()

        picks.append({
            "match_time": match_dt,
            "player1": clean_player1,
            "player2": clean_player2,
            "pick": pick,
            "league": league,
            "alert_key": alert_key,
        })

    return picks


# ── Main sync loop ────────────────────────────────────────────────────────────

async def scanner_loop():
    print("🤖 Totals_Bot starting...")

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        # Verify bot can access client channel
        url = f"{DISCORD_API}/channels/{CLIENT_CHANNEL_ID}"
        async with session.get(url, headers=DISCORD_HEADERS) as r:
            if r.status != 200:
                print(f"❌ Cannot access client channel. Status: {r.status}")
                return
        print(f"✅ Connected to client channel {CLIENT_CHANNEL_ID}")

        posted_keys: set = set()

        while True:
            try:
                now_est = EST_now()
                print(f"🔍 Scanning at {now_est.strftime('%H:%M:%S')} EST...")

                # Get unposted picks with a league from DB
                unposted = await db_get_unposted_picks(session)
                print(f"📋 {len(unposted)} unposted picks with league.")

                for row in unposted:
                    key = row["alert_key"]
                    if key in posted_keys:
                        continue

                    match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
                    message = build_client_message(row, match_dt)
                    print(f"📤 Posting: {message}")

                    message_id = await post_to_client_channel(session, message)
                    if message_id:
                        await db_mark_posted(session, key, message_id)
                        posted_keys.add(key)
                        await asyncio.sleep(1)

                # Update results
                result_picks = await db_get_picks_with_results(session)
                for row in result_picks:
                    message_id = row.get("client_message_id")
                    if not message_id:
                        continue
                    pick_text = row.get("pick", "")
                    result = ""
                    if "✅" in pick_text:
                        result = " ✅"
                    elif "❌" in pick_text:
                        result = " ❌"
                    elif "💀" in pick_text:
                        result = " 💀"

                    match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
                    league = row.get("league", "TT")
                    units = detect_units(pick_text)
                    pick_type = detect_pick_type(pick_text)
                    pst_dt = match_dt.astimezone(PST)
                    est_str = match_dt.strftime("%I:%M %p EST").lstrip("0")
                    pst_str = pst_dt.strftime("%I:%M %p PST").lstrip("0")
                    updated_msg = f"{league} – {row['player1']} vs {row['player2']} {pick_type} {units} @ {est_str} / {pst_str}{result}"

                    await edit_client_message(session, message_id, updated_msg)
                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"❌ Scanner error: {e}")

            await asyncio.sleep(CHECK_INTERVAL)


asyncio.run(scanner_loop())
