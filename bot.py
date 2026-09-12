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
BETSAPI_TOKEN = os.environ.get("BETSAPI_TOKEN")  # Optional — auto-results only work if set
PICKS_CHANNEL_ID = "1466857635746808020"
RESULTS_CHANNEL_ID = "1466857650607100027"
ADMIN_RESULTS_CHANNEL_ID = "1548045193184546816"
TT_SPORT_ID = 92  # BetsAPI sport ID for table tennis
DEFAULT_LINE = 73.5  # Default OVER/UNDER line
EST = pytz.timezone("US/Eastern")
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

# Only these roles receive TT alerts
ALERT_ROLE_IDS = {
    "1535761278151172216",  # TT_Premium
    "1486190812915175465",  # Admin
}

# League emoji mapping
LEAGUE_MAP = {
    "🌐": "TT CUP",
    "🇨🇿": "CZECH",
    "🇵🇱": "ELITE",
    "🇺🇦": "SETKA",
}


def detect_league(text: str) -> str | None:
    """Detect league from emoji in pick line."""
    for emoji, league in LEAGUE_MAP.items():
        if emoji in text:
            return league
    return None


def strip_league_emoji(text: str) -> str:
    """Remove league emojis from text."""
    for emoji in LEAGUE_MAP:
        text = text.replace(emoji, "")
    return text.strip()


# ── Result calculation ────────────────────────────────────────────────────────

def calculate_result(pick_text: str) -> tuple[int, int, int, float]:
    """
    Count wins, losses, voids and net units from pick text.
    Rule: each ✅ = +1U, each ❌ = -1U, each 💀 = void (0U)
    Emojis already reflect correct unit amounts (2U picks show ✅✅ etc.)
    Voids: a 💀 with no ✅ or ❌ in the same component = void
    """
    # Split by + or AND to get components
    components = re.split(r'\s+(?:AND|\+)\s+', pick_text, flags=re.IGNORECASE)
    total_wins = 0
    total_losses = 0
    total_voids = 0

    for component in components:
        w = component.count("✅")
        l = component.count("❌")
        v = component.count("💀")
        if v > 0 and w == 0 and l == 0:
            total_voids += v
        else:
            total_wins += w
            total_losses += l

    net_units = float(total_wins - total_losses)
    return total_wins, total_losses, total_voids, net_units


def has_result(pick_text: str) -> bool:
    return "✅" in pick_text or "❌" in pick_text or "💀" in pick_text


# ── Supabase — picks table ────────────────────────────────────────────────────

async def db_get_existing_keys(session: aiohttp.ClientSession, today: date) -> dict:
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)
    url = (
        f"{SUPABASE_URL}/rest/v1/picks"
        f"?select=alert_key,pick,alert_sent"
        f"&match_date=gte.{yesterday.isoformat()}"
        f"&match_date=lte.{tomorrow.isoformat()}"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            return {}
        rows = await r.json()
        return {row["alert_key"]: row for row in rows}


async def db_insert_pick(session: aiohttp.ClientSession, pick: dict):
    if has_result(pick.get("pick", "")):
        return
    now_utc = datetime.now(pytz.utc)
    match_utc = pick["match_time"].astimezone(pytz.utc)
    if (now_utc - match_utc).total_seconds() > 86400:
        return
    url = f"{SUPABASE_URL}/rest/v1/picks"
    payload = {
        "match_date": pick["match_time"].date().isoformat(),
        "match_time": pick["match_time"].isoformat(),
        "player1": pick["player1"],
        "player2": pick["player2"],
        "pick": pick["pick"],
        "alert_key": pick["alert_key"],
        "alert_sent": False,
        "league": pick.get("league"),
        "posted_to_clients": False,
        "client_message_id": None,
        "discord_message_id": pick.get("discord_message_id"),
    }
    async with session.post(url, headers=SUPABASE_UPSERT_HEADERS, json=payload) as r:
        if r.status not in (200, 201):
            text = await r.text()
            print(f"⚠️ DB insert failed: {r.status} {text}")


async def db_update_pick(session: aiohttp.ClientSession, alert_key: str, new_pick: str):
    url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{alert_key}"
    async with session.patch(url, headers=SUPABASE_HEADERS, json={"pick": new_pick}) as r:
        if r.status in (200, 204):
            print(f"✏️ Updated pick for {alert_key}")


async def db_get_pending_alerts(session: aiohttp.ClientSession) -> list:
    now_utc = datetime.now(pytz.utc)
    to_utc = now_utc + timedelta(hours=24)
    from_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = to_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"{SUPABASE_URL}/rest/v1/picks"
        f"?select=*"
        f"&alert_sent=eq.false"
        f"&match_time=gte.{from_str}"
        f"&match_time=lte.{to_str}"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            text = await r.text()
            print(f"⚠️ DB fetch failed: {r.status} {text}")
            return []
        return await r.json()


async def db_mark_alert_sent(session: aiohttp.ClientSession, alert_key: str):
    url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{alert_key}"
    async with session.patch(url, headers=SUPABASE_HEADERS, json={"alert_sent": True}) as r:
        if r.status not in (200, 204):
            print(f"⚠️ DB mark sent failed: {r.status}")


async def db_cleanup_old_picks(session: aiohttp.ClientSession):
    cutoff = (datetime.now(pytz.utc) - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{SUPABASE_URL}/rest/v1/picks?match_time=lt.{cutoff}"
    async with session.delete(url, headers=SUPABASE_HEADERS) as r:
        if r.status in (200, 204):
            print(f"🧹 Old picks cleaned up.")


# ── Supabase — results table ──────────────────────────────────────────────────

async def db_get_existing_result_keys(session: aiohttp.ClientSession, from_date: date) -> set:
    """Fetch alert_keys from results table for the past 7 days."""
    week_ago = from_date - timedelta(days=7)
    url = (
        f"{SUPABASE_URL}/rest/v1/results"
        f"?select=alert_key"
        f"&match_date=gte.{week_ago.isoformat()}"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            return set()
        rows = await r.json()
        return {row["alert_key"] for row in rows}


async def db_insert_result(session: aiohttp.ClientSession, pick: dict):
    """Insert a completed pick into the results table."""
    pick_text = pick.get("pick", "")
    wins, losses, voids, net_units = calculate_result(pick_text)
    url = f"{SUPABASE_URL}/rest/v1/results"
    payload = {
        "match_date": pick["match_time"].date().isoformat() if hasattr(pick["match_time"], "date") else pick["match_date"],
        "match_time": pick["match_time"].isoformat() if hasattr(pick["match_time"], "isoformat") else pick["match_time"],
        "player1": pick["player1"],
        "player2": pick["player2"],
        "pick": pick_text,
        "wins": wins,
        "losses": losses,
        "voids": voids,
        "net_units": net_units,
        "alert_key": pick["alert_key"],
    }
    async with session.post(url, headers=SUPABASE_UPSERT_HEADERS, json=payload) as r:
        if r.status not in (200, 201):
            text = await r.text()
            print(f"⚠️ Results insert failed: {r.status} {text}")
        else:
            print(f"📝 Result saved: {pick['player1']} vs {pick['player2']} ({net_units:+.1f}U)")


async def db_update_result(session: aiohttp.ClientSession, alert_key: str, pick_text: str):
    """Update an existing result with new pick text."""
    wins, losses, voids, net_units = calculate_result(pick_text)
    url = f"{SUPABASE_URL}/rest/v1/results?alert_key=eq.{alert_key}"
    payload = {"pick": pick_text, "wins": wins, "losses": losses, "voids": voids, "net_units": net_units}
    async with session.patch(url, headers=SUPABASE_HEADERS, json=payload) as r:
        if r.status in (200, 204):
            print(f"✏️ Result updated: {alert_key}")


# ── Pick parsing ─────────────────────────────────────────────────────────────

def parse_picks(text: str, post_date: date) -> list[dict]:
    """Parse picks WITHOUT result emojis for alerts."""
    picks = []
    now = EST_now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    # Process line by line to detect league emoji at start
    lines = text.split("\n")
    cleaned_lines = []
    line_leagues = []
    for line in lines:
        league = detect_league(line)
        cleaned = strip_league_emoji(line)
        cleaned_lines.append(cleaned)
        line_leagues.append(league)
    cleaned_text = "\n".join(cleaned_lines)

    pattern1 = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(.+?)\s+vs\s+"
        r"(.+?)\s+"
        r"((?:OVER|UNDER|SPLIT|SplitDD|Split\s+DD|\w+\s+-\d+\.?\d*).*)$",
        re.IGNORECASE | re.MULTILINE,
    )
    pattern2 = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(\w+(?:\s+\w+)?)\s+"
        r"(-\d+\.?\d*[^v]+?)\s+vs\s+"
        r"(\w+(?:\s+\w+)?)(.*)$",
        re.IGNORECASE | re.MULTILINE,
    )

    def resolve_match_dt(t, post_date):
        match_dt = EST.localize(datetime(post_date.year, post_date.month, post_date.day, t.hour, t.minute))
        if t.hour < 6 and post_date == yesterday:
            candidate = EST.localize(datetime(today.year, today.month, today.day, t.hour, t.minute))
            if candidate > now:
                return candidate
        return match_dt

    def get_line_league(match_start: int) -> str | None:
        """Get the league for the line containing this match position."""
        pos = 0
        for i, line in enumerate(cleaned_lines):
            if pos <= match_start <= pos + len(line):
                return line_leagues[i]
            pos += len(line) + 1
        return None

    seen_keys = set()
    for m in pattern1.finditer(cleaned_text):
        time_str, player1, player2, pick = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
        if has_result(pick):
            continue
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = resolve_match_dt(t, post_date)
        except ValueError:
            continue
        league = get_line_league(m.start())
        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"
        if alert_key not in seen_keys:
            seen_keys.add(alert_key)
            picks.append({"match_time": match_dt, "player1": player1, "player2": player2, "pick": pick, "alert_key": alert_key, "league": league})

    for m in pattern2.finditer(cleaned_text):
        time_str, player1, pick, player2 = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
        if has_result(pick):
            continue
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = resolve_match_dt(t, post_date)
        except ValueError:
            continue
        league = get_line_league(m.start())
        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"
        if alert_key not in seen_keys:
            seen_keys.add(alert_key)
            picks.append({"match_time": match_dt, "player1": player1, "player2": player2, "pick": pick, "alert_key": alert_key, "league": league})

    return picks


def parse_picks_with_results(text: str, post_date: date) -> list[dict]:
    """Parse ALL picks including those with result emojis for nightly sync."""
    picks = []
    now = EST_now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    pattern1 = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(.+?)\s+vs\s+"
        r"(.+?)\s+"
        r"((?:OVER|UNDER|SPLIT|SplitDD|Split\s+DD|\w+\s+-\d+\.?\d*).*)$",
        re.IGNORECASE | re.MULTILINE,
    )
    pattern2 = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(\w+(?:\s+\w+)?)\s+"
        r"(-\d+\.?\d*[^v]+?)\s+vs\s+"
        r"(\w+(?:\s+\w+)?)(.*)$",
        re.IGNORECASE | re.MULTILINE,
    )

    def resolve_match_dt(t, post_date):
        match_dt = EST.localize(datetime(post_date.year, post_date.month, post_date.day, t.hour, t.minute))
        if t.hour < 6 and post_date == yesterday:
            # For nightly sync always push early morning to next day
            next_day = post_date + timedelta(days=1)
            return EST.localize(datetime(next_day.year, next_day.month, next_day.day, t.hour, t.minute))
        return match_dt

    seen_keys = set()
    for m in pattern1.finditer(text):
        time_str, player1, player2, pick = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = resolve_match_dt(t, post_date)
        except ValueError:
            continue
        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"
        if alert_key not in seen_keys:
            seen_keys.add(alert_key)
            picks.append({"match_time": match_dt, "player1": player1, "player2": player2, "pick": pick, "alert_key": alert_key})

    for m in pattern2.finditer(text):
        time_str, player1, pick, player2 = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = resolve_match_dt(t, post_date)
        except ValueError:
            continue
        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"
        if alert_key not in seen_keys:
            seen_keys.add(alert_key)
            picks.append({"match_time": match_dt, "player1": player1, "player2": player2, "pick": pick, "alert_key": alert_key})

    return picks


# ── Discord helpers ──────────────────────────────────────────────────────────

def build_alert_message(pick: dict) -> str:
    if isinstance(pick["match_time"], str):
        match_dt = datetime.fromisoformat(pick["match_time"]).astimezone(EST)
    else:
        match_dt = pick["match_time"]
    return (
        f"🏓 **MATCH STARTING IN 60 SECONDS!**\n\n"
        f"{pick['player1']} vs {pick['player2']}\n"
        f"Pick: {pick['pick']}\n"
        f"Date: {match_dt.strftime('%A, %m/%d/%Y')}\n"
        f"Time: {match_dt.strftime('%I:%M %p EDT')}\n\n"
        f"Good luck! 🍀"
    )


async def get_guild_id(session: aiohttp.ClientSession) -> str | None:
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            return None
        data = await r.json()
        return data.get("guild_id")


async def get_channel_messages(session: aiohttp.ClientSession) -> list:
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}/messages?limit=50"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            return []
        return await r.json()


async def get_guild_members(session: aiohttp.ClientSession, guild_id: str) -> list:
    members = []
    after = "0"
    while True:
        url = f"{DISCORD_API}/guilds/{guild_id}/members?limit=1000&after={after}"
        async with session.get(url, headers=DISCORD_HEADERS) as r:
            batch = await r.json()
            if not batch or not isinstance(batch, list):
                break
            members.extend(batch)
            if len(batch) < 1000:
                break
            after = batch[-1]["user"]["id"]
    return members


async def send_dm(session: aiohttp.ClientSession, user_id: str, message: str):
    async with session.post(f"{DISCORD_API}/users/@me/channels",
                            headers=DISCORD_HEADERS,
                            json={"recipient_id": user_id}) as r:
        if r.status != 200:
            return
        dm = await r.json()
        dm_channel_id = dm["id"]
    async with session.post(f"{DISCORD_API}/channels/{dm_channel_id}/messages",
                            headers=DISCORD_HEADERS,
                            json={"content": message}) as r:
        if r.status == 429:
            data = await r.json()
            await asyncio.sleep(data.get("retry_after", 1))


FREE_PICK_CHANNEL_ID = "1515735232886607902"
FREE_PICK_EMOJI = "⭐"


async def post_free_pick_alert(session: aiohttp.ClientSession, row: dict, match_dt: datetime):
    """Post the free pick to #waiting-room when the alert fires."""
    pick_text = row.get("pick", "").replace(FREE_PICK_EMOJI, "").strip()
    match_time_str = match_dt.strftime("%I:%M %p EDT")
    match_date_str = match_dt.strftime("%A, %m/%d/%Y")

    message = (
        f"🎁 **FREE PICK OF THE DAY** 🎁\n\n"
        f"{row['player1']} vs {row['player2']}\n"
        f"Pick: {pick_text}\n"
        f"Date: {match_date_str}\n"
        f"Time: {match_time_str}\n\n"
        f"Want more picks like this? Subscribe now:\n"
        f"🔗 https://whop.com/offgrid-edge?a=crashacid"
    )

    url = f"{DISCORD_API}/channels/{FREE_PICK_CHANNEL_ID}/messages"
    async with session.post(url, headers=DISCORD_HEADERS, json={"content": message}) as r:
        if r.status in (200, 201):
            print(f"⭐ Free pick posted to waiting-room.")
        else:
            text = await r.text()
            print(f"⚠️ Failed to post free pick: {r.status} {text}")


# ── Message sync ─────────────────────────────────────────────────────────────

async def sync_picks_from_channel(session: aiohttp.ClientSession):
    messages = await get_channel_messages(session)
    today = EST_now().date()
    yesterday = today - timedelta(days=1)

    existing_picks = await db_get_existing_keys(session, today)
    existing_results = await db_get_existing_result_keys(session, today)

    all_picks = []
    for msg in messages:
        if msg.get("author", {}).get("bot"):
            continue
        post_dt = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")).astimezone(EST)
        post_date = post_dt.date()
        if post_date < yesterday:
            print(f"⏹ Stopping — message posted on {post_date}, too old.")
            break
        if post_date not in (today, yesterday):
            continue
        content = msg.get("content", "")
        if not content.strip():
            continue
        picks = parse_picks(content, post_date)
        # Attach message ID to each pick so we can edit the message later
        for pick in picks:
            pick["discord_message_id"] = msg["id"]
        print(f"📝 Message from {post_date}: {len(picks)} picks parsed.")
        all_picks.extend(picks)

    inserts = 0
    updates = 0
    results_saved = 0

    for pick in all_picks:
        key = pick["alert_key"]
        pick_text = pick.get("pick", "")

        if has_result(pick_text):
            if key not in existing_results:
                await db_insert_result(session, pick)
                results_saved += 1
            else:
                await db_update_result(session, key, pick_text)
            continue

        if key in existing_picks:
            row = existing_picks[key]
            if not row.get("alert_sent") and row.get("pick") != pick_text:
                await db_update_pick(session, key, pick_text)
                updates += 1
        else:
            await db_insert_pick(session, pick)
            inserts += 1

    if inserts or updates or results_saved:
        print(f"💾 Synced: {inserts} new picks, {updates} updated, {results_saved} results saved.")
    else:
        print(f"✅ No new picks to sync.")


# ── Alert sender ─────────────────────────────────────────────────────────────

alerts_in_progress: set = set()


async def db_get_active_clients(session: aiohttp.ClientSession) -> list:
    """Fetch all active client configs from Supabase."""
    try:
        url = f"{SUPABASE_URL}/rest/v1/clients?select=*&active=eq.true"
        async with session.get(url, headers=SUPABASE_HEADERS) as r:
            if r.status != 200:
                return []  # Table doesn't exist yet or no clients
            return await r.json()
    except Exception:
        return []


async def send_alert_to_client(session: aiohttp.ClientSession, client: dict, alert_msg: str):
    """Send alert DMs to all eligible members in a client's Discord server."""
    guild_id = client["guild_id"]
    role_id = client["alert_role_id"]
    server_name = client["server_name"]

    members = await get_guild_members(session, guild_id)
    eligible = [
        m for m in members
        if not m.get("user", {}).get("bot")
        and role_id in m.get("roles", [])
    ]

    semaphore = asyncio.Semaphore(5)

    async def send_one(member, msg=alert_msg):
        async with semaphore:
            user = member.get("user", {})
            try:
                await asyncio.wait_for(send_dm(session, user["id"], msg), timeout=5)
            except Exception:
                pass

    await asyncio.gather(*[send_one(m) for m in eligible])
    print(f"✅ Alert sent to {len(eligible)} members in {server_name}.")


async def send_alerts(session: aiohttp.ClientSession, guild_id: str, pending: list, now_est: datetime):
    global alerts_in_progress
    alerts_to_send = []
    for row in pending:
        match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
        seconds_until = (match_dt - now_est).total_seconds()
        print(f"⏱ {row['player1']} vs {row['player2']} in {int(seconds_until)}s")
        if seconds_until < 0:
            print(f"⏭ Skipping {row['player1']} vs {row['player2']} — already started.")
            await db_mark_alert_sent(session, row["alert_key"])
            continue
        if 50 <= seconds_until <= 110:
            if row["alert_key"] not in alerts_in_progress:
                alerts_to_send.append(row)

    if not alerts_to_send:
        return

    for row in alerts_to_send:
        alerts_in_progress.add(row["alert_key"])

    # ── Send to Offgrid members ──────────────────────────────────────────────
    members = await get_guild_members(session, guild_id)
    real_members = [
        m for m in members
        if not m.get("user", {}).get("bot")
        and any(role_id in ALERT_ROLE_IDS for role_id in m.get("roles", []))
    ]
    print(f"📨 Sending to {len(real_members)} Offgrid members.")

    # ── Fetch active clients ─────────────────────────────────────────────────
    clients = await db_get_active_clients(session)

    for row in alerts_to_send:
        match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
        print(f"🚨 Sending alert: {row['player1']} vs {row['player2']}")
        alert_msg = build_alert_message({
            "match_time": match_dt,
            "player1": row["player1"],
            "player2": row["player2"],
            "pick": row["pick"],
        })

        semaphore = asyncio.Semaphore(5)

        async def send_one(member, msg=alert_msg):
            async with semaphore:
                user = member.get("user", {})
                if not user.get("bot"):
                    try:
                        await asyncio.wait_for(send_dm(session, user["id"], msg), timeout=5)
                    except asyncio.TimeoutError:
                        pass
                    except Exception:
                        pass

        # Send to Offgrid members
        await asyncio.gather(*[send_one(m) for m in real_members])
        print(f"✅ Offgrid alert sent to {len(real_members)} members.")

        # Send to each active client
        for client in clients:
            await send_alert_to_client(session, client, alert_msg)

        await db_mark_alert_sent(session, row["alert_key"])

        # If this pick is flagged with ⭐ post it to waiting-room too
        if FREE_PICK_EMOJI in row.get("pick", ""):
            await post_free_pick_alert(session, row, match_dt)


# ── Nightly results sync ──────────────────────────────────────────────────────

async def nightly_results_sync(session: aiohttp.ClientSession):
    print("🌙 Running nightly results sync...")
    messages = await get_channel_messages(session)
    today = EST_now().date()
    yesterday = today - timedelta(days=1)

    existing_results = await db_get_existing_result_keys(session, today)
    saved = 0
    updated = 0

    for msg in messages:
        if msg.get("author", {}).get("bot"):
            continue
        post_dt = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")).astimezone(EST)
        post_date = post_dt.date()
        if post_date < yesterday:
            break
        if post_date not in (today, yesterday):
            continue
        content = msg.get("content", "")
        if not content.strip():
            continue

        picks = parse_picks_with_results(content, post_date)

        for pick in picks:
            if not has_result(pick.get("pick", "")):
                continue
            key = pick["alert_key"]
            if key not in existing_results:
                await db_insert_result(session, pick)
                saved += 1
            else:
                await db_update_result(session, key, pick["pick"])
                updated += 1

    print(f"🌙 Nightly sync complete: {saved} new results, {updated} updated.")


# ── Weekly report ─────────────────────────────────────────────────────────────

async def fetch_week_results(session: aiohttp.ClientSession, monday: date, sunday: date) -> list:
    url = (
        f"{SUPABASE_URL}/rest/v1/results"
        f"?select=*"
        f"&match_date=gte.{monday.isoformat()}"
        f"&match_date=lte.{sunday.isoformat()}"
        f"&order=match_time.asc"
        f"&limit=1000"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch week results: {r.status}")
            return []
        return await r.json()


async def post_weekly_report(session: aiohttp.ClientSession):
    now = EST_now()
    last_monday = now.date() - timedelta(days=7)
    last_sunday = last_monday + timedelta(days=6)

    print(f"📊 Building weekly report for {last_monday} to {last_sunday}...")
    results = await fetch_week_results(session, last_monday, last_sunday)

    if not results:
        print("⚠️ No results found for this week.")
        return

    total_wins = total_losses = total_voids = 0
    daily = {}

    for row in results:
        wins = int(row.get("wins", 0))
        losses = int(row.get("losses", 0))
        voids = int(row.get("voids", 0))
        match_date = row.get("match_date")

        total_wins += wins
        total_losses += losses
        total_voids += voids

        if match_date not in daily:
            daily[match_date] = 0
        daily[match_date] += wins - losses

    week_total = total_wins - total_losses
    sign = "+" if week_total >= 0 else ""

    report = (
        f"🏓 **WEEKLY PERFORMANCE REPORT**\n"
        f"📅 {last_monday.strftime('%b %d')} — {last_sunday.strftime('%b %d, %Y')}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Wins: **{total_wins}**\n"
        f"❌ Losses: **{total_losses}**\n"
        f"💀 Voids: **{total_voids}**\n"
        f"📊 Week Total: **{sign}{week_total}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Daily Breakdown:**\n"
    )

    for day_str in sorted(daily.keys()):
        day_date = date.fromisoformat(day_str)
        day_net = daily[day_str]
        sign_d = "+" if day_net >= 0 else ""
        emoji = "🟢" if day_net > 0 else "🔴" if day_net < 0 else "⚪"
        report += f"{emoji} {day_date.strftime('%A %b %d')}: {sign_d}{day_net}\n"

    report += f"\n@everyone"

    url = f"{DISCORD_API}/channels/{RESULTS_CHANNEL_ID}/messages"
    async with session.post(url, headers=DISCORD_HEADERS, json={"content": report}) as r:
        if r.status in (200, 201):
            print(f"✅ Weekly report posted successfully.")
        else:
            text = await r.text()
            print(f"⚠️ Failed to post weekly report: {r.status} {text}")


async def fetch_month_results(session: aiohttp.ClientSession, first_day: date, last_day: date) -> list:
    url = (
        f"{SUPABASE_URL}/rest/v1/results"
        f"?select=*"
        f"&match_date=gte.{first_day.isoformat()}"
        f"&match_date=lte.{last_day.isoformat()}"
        f"&order=match_time.asc"
        f"&limit=5000"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch month results: {r.status}")
            return []
        return await r.json()


async def post_monthly_report(session: aiohttp.ClientSession):
    now = EST_now()
    # Previous month
    first_day = (now.date().replace(day=1) - timedelta(days=1)).replace(day=1)
    last_day = now.date().replace(day=1) - timedelta(days=1)

    print(f"📊 Building monthly report for {first_day} to {last_day}...")
    results = await fetch_month_results(session, first_day, last_day)

    if not results:
        print("⚠️ No results found for this month.")
        return

    total_wins = total_losses = total_voids = 0
    weekly = {}

    for row in results:
        wins = int(row.get("wins", 0))
        losses = int(row.get("losses", 0))
        voids = int(row.get("voids", 0))
        match_date = date.fromisoformat(row.get("match_date"))

        total_wins += wins
        total_losses += losses
        total_voids += voids

        # Group by week starting Monday
        week_start = match_date - timedelta(days=match_date.weekday())
        week_end = week_start + timedelta(days=6)
        # Clamp to month boundaries
        week_start_clamped = max(week_start, first_day)
        week_end_clamped = min(week_end, last_day)
        week_key = week_start_clamped.isoformat()

        if week_key not in weekly:
            weekly[week_key] = {"net": 0, "start": week_start_clamped, "end": week_end_clamped}
        weekly[week_key]["net"] += wins - losses

    month_total = total_wins - total_losses
    sign = "+" if month_total >= 0 else ""

    report = (
        f"🏓 **MONTHLY PERFORMANCE REPORT**\n"
        f"📅 {first_day.strftime('%B %Y')}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Wins: **{total_wins}**\n"
        f"❌ Losses: **{total_losses}**\n"
        f"💀 Voids: **{total_voids}**\n"
        f"📊 Month Total: **{sign}{month_total}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Weekly Breakdown:**\n"
    )

    for week_key in sorted(weekly.keys()):
        w = weekly[week_key]
        net = w["net"]
        sign_w = "+" if net >= 0 else ""
        emoji = "🟢" if net > 0 else "🔴" if net < 0 else "⚪"
        start_str = w["start"].strftime("%b %d")
        end_str = w["end"].strftime("%b %d")
        report += f"{emoji} Week {start_str}-{end_str}: {sign_w}{net}\n"

    report += f"\n@everyone"

    url = f"{DISCORD_API}/channels/{RESULTS_CHANNEL_ID}/messages"
    async with session.post(url, headers=DISCORD_HEADERS, json={"content": report}) as r:
        if r.status in (200, 201):
            print(f"✅ Monthly report posted successfully.")
        else:
            text = await r.text()
            print(f"⚠️ Failed to post monthly report: {r.status} {text}")


WELCOME_MESSAGE = """**Welcome to Offgrid Edge!** 🏓

Premium sports picks — Eastern European Table Tennis and more.

**Choose your membership:**
👑 **Full Access** — All channels + TT picks & DM alerts
⚡ **All Sports** — All channels, no TT picks

🔗 https://whop.com/offgrid-edge?a=crashacid

Subscribe on Whop and claim your access there. Let's get it! 🔒"""


async def send_welcome_dm(session: aiohttp.ClientSession, user_id: str):
    """Send welcome DM to a new member."""
    try:
        async with session.post(f"{DISCORD_API}/users/@me/channels",
                                headers=DISCORD_HEADERS,
                                json={"recipient_id": user_id}) as r:
            if r.status != 200:
                return
            dm = await r.json()
            dm_channel_id = dm["id"]
        async with session.post(f"{DISCORD_API}/channels/{dm_channel_id}/messages",
                                headers=DISCORD_HEADERS,
                                json={"content": WELCOME_MESSAGE}) as r:
            if r.status in (200, 201):
                print(f"👋 Welcome DM sent to user {user_id}")
            else:
                print(f"⚠️ Failed to send welcome DM: {r.status}")
    except Exception as e:
        print(f"⚠️ Welcome DM error: {e}")


async def check_new_members(session: aiohttp.ClientSession, guild_id: str, welcomed: set) -> set:
    """Check for new members and send welcome DMs to anyone not yet welcomed."""
    members = await get_guild_members(session, guild_id)
    now_utc = datetime.now(pytz.utc)

    for member in members:
        user = member.get("user", {})
        if user.get("bot"):
            continue
        user_id = user.get("id")
        if user_id in welcomed:
            continue

        # Check if member joined in the last 10 minutes
        joined_at = member.get("joined_at")
        if joined_at:
            joined_dt = datetime.fromisoformat(joined_at.replace("Z", "+00:00"))
            minutes_since_join = (now_utc - joined_dt).total_seconds() / 60
            if minutes_since_join <= 10:
                print(f"👋 New member detected: {user.get('username', user_id)}")
                await send_welcome_dm(session, user_id)
                welcomed.add(user_id)

    return welcomed


# ── BetsAPI result tracking ───────────────────────────────────────────────────

def extract_line(pick_text: str) -> float:
    """Extract the OVER/UNDER line from pick text. Default 73.5."""
    match = re.search(r'(\d+\.?\d+)', pick_text)
    if match:
        return float(match.group(1))
    return DEFAULT_LINE


async def betsapi_search_event(session: aiohttp.ClientSession, player1: str, player2: str, match_dt: datetime) -> dict | None:
    """Search BetsAPI using last name only for better matching."""
    if not BETSAPI_TOKEN:
        return None
    try:
        def get_last_name(name: str) -> str:
            parts = name.replace(".", "").strip().split()
            surnames = [p for p in parts if len(p) > 1]
            return surnames[0] if surnames else name

        home = get_last_name(player1)
        away = get_last_name(player2)
        time_unix = int(match_dt.timestamp())

        async def do_search(h, a):
            url = (
                f"https://api.betsapi.com/v1/events/search"
                f"?token={BETSAPI_TOKEN}"
                f"&sport_id={TT_SPORT_ID}"
                f"&home={h}"
                f"&away={a}"
                f"&time={time_unix}"
            )
            print(f"Searching BetsAPI: {h} vs {a} at {match_dt.strftime('%H:%M EST')}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    print(f"BetsAPI HTTP error: {r.status}")
                    return None
                data = await r.json()
                results = data.get("results", [])
                if results:
                    found_home = results[0].get("home", {}).get("name", "")
                    found_away = results[0].get("away", {}).get("name", "")
                    print(f"BetsAPI found: {found_home} vs {found_away}")
                    return results[0]
                return None

        result = await do_search(home, away)
        if not result:
            print(f"BetsAPI no results for {home} vs {away}")
        return result
    except Exception as e:
        print(f"BetsAPI search error: {e}")
    return None


async def betsapi_get_odds(session: aiohttp.ClientSession, event_id: str) -> dict | None:
    """Get Over/Under odds and line from BetsAPI for a table tennis event."""
    if not BETSAPI_TOKEN:
        return None
    try:
        url = (
            f"https://api.b365api.com/v2/event/odds"
            f"?token={BETSAPI_TOKEN}"
            f"&event_id={event_id}"
            f"&odds_market=3"  # Over/Under market
        )
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            results = data.get("results", {})

            # Look for the O/U market in bet365 data
            for market_key, market_data in results.items():
                if "_3" in market_key:  # Over/Under market
                    odds_list = market_data.get("odds", [])
                    for odds_entry in odds_list:
                        handicap = odds_entry.get("handicap")
                        over_odds = odds_entry.get("home_od")  # Over odds
                        under_odds = odds_entry.get("away_od")  # Under odds
                        if handicap and over_odds:
                            return {
                                "line": float(handicap),
                                "over_odds": float(over_odds),
                                "under_odds": float(under_odds) if under_odds else None
                            }
    except Exception as e:
        print(f"⚠️ BetsAPI odds error: {e}")
    return None


async def db_save_odds(session: aiohttp.ClientSession, alert_key: str, line: float, over_odds: float, under_odds: float):
    """Save odds data to picks table."""
    url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{alert_key}"
    payload = {
        "betting_line": line,
        "over_odds": over_odds,
        "under_odds": under_odds
    }
    async with session.patch(url, headers=SUPABASE_HEADERS, json=payload) as r:
        if r.status not in (200, 204):
            print(f"⚠️ Failed to save odds: {r.status}")
    """Get the final score for an event. Returns (home_total_pts, away_total_pts) or None."""
    if not BETSAPI_TOKEN:
        return None
    try:
        url = f"https://api.betsapi.com/v1/event/view?token={BETSAPI_TOKEN}&event_id={event_id}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json()
            results = data.get("results", [])
            if not results:
                return None
            event = results[0]

            # time_status 3 = ended
            if event.get("time_status") != "3":
                return None

            # Try to get individual set scores from 'scores' field
            scores_data = event.get("scores", {})
            home_total = 0
            away_total = 0

            if scores_data:
                # scores is a dict like {"1": {"home": "11", "away": "9"}, "2": {...}}
                for set_num, set_score in scores_data.items():
                    try:
                        home_total += int(set_score.get("home", 0))
                        away_total += int(set_score.get("away", 0))
                    except (ValueError, TypeError):
                        pass
                print(f"📊 Set scores: {scores_data} → Total: {home_total}+{away_total}={home_total+away_total}")
                return home_total, away_total

            # Fallback to ss field if scores not available
            ss = event.get("ss", "")
            if ss:
                print(f"⚠️ No set scores available, falling back to ss: {ss}")
                # ss format for TT is usually sets won like "3-2"
                # This is unreliable for points, return None
                return None

    except Exception as e:
        print(f"⚠️ BetsAPI score error: {e}")
    return None


async def db_get_pending_results(session: aiohttp.ClientSession) -> list:
    """Get picks that have started but don't have auto results yet."""
    now_utc = datetime.now(pytz.utc)
    # Matches that started more than 15 minutes ago but less than 3 hours ago
    from_utc = (now_utc - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    to_utc = (now_utc - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"{SUPABASE_URL}/rest/v1/picks"
        f"?select=*"
        f"&alert_sent=eq.true"
        f"&auto_result=is.null"
        f"&match_time=gte.{from_utc}"
        f"&match_time=lte.{to_utc}"
    )
    async with session.get(url, headers=SUPABASE_HEADERS) as r:
        if r.status != 200:
            return []
        return await r.json()


async def db_save_auto_result(session: aiohttp.ClientSession, alert_key: str, result: str):
    """Save auto result to picks table."""
    url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{alert_key}"
    async with session.patch(url, headers=SUPABASE_HEADERS, json={"auto_result": result}) as r:
        if r.status not in (200, 204):
            print(f"⚠️ Failed to save auto result: {r.status}")


async def get_channel_message(session: aiohttp.ClientSession, message_id: str) -> dict | None:
    """Fetch a specific message from the picks channel."""
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}/messages/{message_id}"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            return None
        return await r.json()


async def post_admin_result(session: aiohttp.ClientSession, pick: dict, total_pts: int, home_pts: int, away_pts: int, line: float, result: str, match_dt: datetime, roi: float | None = None):
    """Post result to admin-only channel."""
    league = pick.get("league", "")
    league_str = f"{league}\n" if league else ""
    pick_type = "UNDER" if re.search(r'UND+ER', pick.get("pick", ""), re.IGNORECASE) else "OVER"
    result_label = "✅ HIT" if result == "✅" else "❌ MISS"
    roi_str = f"\nROI: **{'+' if roi and roi > 0 else ''}{roi:.2f}U**" if roi is not None else ""

    message = (
        f"📊 **RESULT: {league_str}**"
        f"{pick['player1']} vs {pick['player2']}\n"
        f"Pick: **{pick_type}** (line {line})\n"
        f"Score: {home_pts}+{away_pts} = **{total_pts} pts**\n"
        f"Time: {match_dt.strftime('%I:%M %p EST')}\n"
        f"Result: **{result_label}**{roi_str}"
    )

    url = f"{DISCORD_API}/channels/{ADMIN_RESULTS_CHANNEL_ID}/messages"
    async with session.post(url, headers=DISCORD_HEADERS, json={"content": message}) as r:
        if r.status in (200, 201):
            print(f"📬 Admin result posted: {pick['player1']} vs {pick['player2']} → {result_label}")
        else:
            print(f"⚠️ Failed to post admin result: {r.status}")
    """Find the pick line in a Discord message and append the result emoji."""
    try:
        # Fetch the current message
        url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}/messages/{message_id}"
        async with session.get(url, headers=DISCORD_HEADERS) as r:
            if r.status != 200:
                return
            msg = await r.json()

        content = msg.get("content", "")
        lines = content.split("\n")
        updated_lines = []
        edited = False

        for line in lines:
            # Find the line with this match
            if player1.lower() in line.lower() and player2.lower() in line.lower():
                # Only add result if line doesn't already have one
                if not any(e in line for e in ["✅", "❌", "💀"]):
                    line = line.rstrip() + f" {result}"
                    edited = True
            updated_lines.append(line)

        if edited:
            new_content = "\n".join(updated_lines)
            async with session.patch(url, headers=DISCORD_HEADERS, json={"content": new_content}) as r:
                if r.status in (200, 204):
                    print(f"✏️ Discord message updated: {player1} vs {player2} → {result}")
                else:
                    print(f"⚠️ Failed to edit Discord message: {r.status}")
    except Exception as e:
        print(f"⚠️ Edit message error: {e}")


async def auto_track_results(session: aiohttp.ClientSession):
    """Check BetsAPI for results of pending picks and update Discord messages."""
    if not BETSAPI_TOKEN:
        return

    pending = await db_get_pending_results(session)
    if not pending:
        return

    print(f"🔍 Auto-tracking {len(pending)} pending results...")

    for pick in pending:
        try:
            match_dt = datetime.fromisoformat(pick["match_time"]).astimezone(EST)

            event = await betsapi_search_event(session, pick["player1"], pick["player2"], match_dt)
            if not event:
                print(f"⚠️ No BetsAPI match found: {pick['player1']} vs {pick['player2']}")
                continue

            event_id = str(event.get("id"))

            # Fetch odds before getting score (while event may still be live)
            odds_data = await betsapi_get_odds(session, event_id)
            if odds_data:
                print(f"💰 Odds found: line={odds_data['line']} over={odds_data['over_odds']} under={odds_data.get('under_odds')}")
                await db_save_odds(session, pick["alert_key"], odds_data["line"], odds_data["over_odds"], odds_data.get("under_odds", 0))

            score = await betsapi_get_score(session, event_id)
            if not score:
                continue

            home_pts, away_pts = score
            total_pts = home_pts + away_pts
            pick_text = pick.get("pick", "").upper()

            is_under = bool(re.search(r'UND+ER', pick_text, re.IGNORECASE))
            line = extract_line(pick_text)

            if is_under:
                result = "✅" if total_pts < line else "❌"
            else:
                result = "✅" if total_pts > line else "❌"

            # Calculate ROI if odds available
            roi = None
            if odds_data:
                is_under_pick = bool(re.search(r'UND+ER', pick.get("pick", ""), re.IGNORECASE))
                odds = odds_data.get("under_odds") if is_under_pick else odds_data.get("over_odds")
                if odds and result == "✅":
                    roi = round((float(odds) - 1) * 1, 2)  # 1U bet
                elif result == "❌":
                    roi = -1.0

            print(f"📊 {pick['player1']} vs {pick['player2']}: {home_pts}+{away_pts}={total_pts} pts (line {line}) → {result} ROI: {roi}")

            # Save to DB
            await db_save_auto_result(session, pick["alert_key"], result)

            # Post to admin results channel
            await post_admin_result(session, pick, total_pts, home_pts, away_pts, line, result, match_dt, roi)

            # Edit the Discord message
            discord_message_id = pick.get("discord_message_id")
            if discord_message_id:
                await edit_discord_pick_message(session, discord_message_id, pick["player1"], pick["player2"], result)

        except Exception as e:
            print(f"⚠️ Auto-result error for {pick.get('player1')}: {e}")

        await asyncio.sleep(0.5)


# ── Main loop ────────────────────────────────────────────────────────────────

async def scanner_loop():
    print("🤖 TT Bot starting...")

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:
        guild_id = await get_guild_id(session)
        if guild_id is None:
            print("❌ Could not get guild ID.")
            return
        print(f"✅ Connected to guild ID: {guild_id}")

        last_cleanup_date = None
        weekly_report_sent = None
        nightly_sync_done = None
        monthly_report_sent = None
        welcomed_members: set = set()
        last_member_check = None
        last_result_check = None

        while True:
            try:
                now_est = EST_now()
                today = now_est.date()
                is_monday = now_est.weekday() == 0
                is_first_of_month = today.day == 1
                print(f"🔍 Scanning at {now_est.strftime('%H:%M:%S')} EST...")

                if last_cleanup_date != today:
                    await db_cleanup_old_picks(session)
                    last_cleanup_date = today

                # Nightly results sync — 11:30pm EST every day
                is_nightly_time = now_est.hour == 23 and now_est.minute == 30
                if is_nightly_time and nightly_sync_done != today:
                    await nightly_results_sync(session)
                    nightly_sync_done = today

                # Auto-track results via BetsAPI every 5 minutes
                if BETSAPI_TOKEN and (last_result_check is None or (now_est - last_result_check).total_seconds() >= 300):
                    await auto_track_results(session)
                    last_result_check = now_est

                # Check for new members every 5 minutes
                if last_member_check is None or (now_est - last_member_check).total_seconds() >= 300:
                    welcomed_members = await check_new_members(session, guild_id, welcomed_members)
                    last_member_check = now_est

                # Monthly report — 1st of each month at 8:00am EST
                is_report_time = now_est.hour == 8 and now_est.minute < 1
                if is_first_of_month and is_report_time and monthly_report_sent != today:
                    await post_monthly_report(session)
                    monthly_report_sent = today

                # Weekly report — Monday at 8:00am EST (skip if already posted monthly)
                if is_monday and not is_first_of_month and is_report_time and weekly_report_sent != today:
                    await post_weekly_report(session)
                    weekly_report_sent = today

                await asyncio.wait_for(
                    sync_picks_from_channel(session),
                    timeout=30
                )

                pending = await db_get_pending_alerts(session)
                print(f"📋 {len(pending)} pending alerts in database.")

                await asyncio.wait_for(
                    send_alerts(session, guild_id, pending, now_est),
                    timeout=60
                )

            except asyncio.TimeoutError:
                print("⚠️ Scan cycle timed out — restarting loop.")
            except Exception as e:
                print(f"❌ Scanner error: {e}")

            await asyncio.sleep(CHECK_INTERVAL)


async def main():
    await scanner_loop()


asyncio.run(main())

# ── Multi-client infrastructure ───────────────────────────────────────────────
# Clients table in Supabase stores config for each external Discord server
# Schema:
# CREATE TABLE clients (
#     id SERIAL PRIMARY KEY,
#     server_name TEXT NOT NULL,
#     guild_id TEXT NOT NULL UNIQUE,
#     alert_role_id TEXT NOT NULL,
#     active BOOLEAN DEFAULT TRUE,
#     created_at TIMESTAMPTZ DEFAULT NOW()
# );
