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
PICKS_CHANNEL_ID = "1466857635746808020"
EST = pytz.timezone("US/Eastern")
CHECK_INTERVAL = 20  # seconds between scans
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


# ── Supabase helpers ─────────────────────────────────────────────────────────

async def db_upsert_pick(session: aiohttp.ClientSession, pick: dict):
    check_url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{pick['alert_key']}&select=id,pick,alert_sent"
    async with session.get(check_url, headers=SUPABASE_HEADERS) as r:
        if r.status == 200:
            existing = await r.json()
            if existing:
                row = existing[0]
                if row.get("alert_sent"):
                    return
                if row.get("pick") != pick["pick"]:
                    patch_url = f"{SUPABASE_URL}/rest/v1/picks?alert_key=eq.{pick['alert_key']}"
                    async with session.patch(patch_url, headers=SUPABASE_HEADERS, json={"pick": pick["pick"]}) as pr:
                        if pr.status in (200, 204):
                            print(f"✏️ Updated pick: {pick['player1']} vs {pick['player2']} → {pick['pick']}")
                return

    url = f"{SUPABASE_URL}/rest/v1/picks"
    payload = {
        "match_date": pick["match_time"].date().isoformat(),
        "match_time": pick["match_time"].isoformat(),
        "player1": pick["player1"],
        "player2": pick["player2"],
        "pick": pick["pick"],
        "alert_key": pick["alert_key"],
        "alert_sent": False
    }
    async with session.post(url, headers=SUPABASE_UPSERT_HEADERS, json=payload) as r:
        if r.status not in (200, 201):
            text = await r.text()
            print(f"⚠️ DB insert failed: {r.status} {text}")


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
        else:
            print(f"⚠️ DB cleanup failed: {r.status}")


# ── Pick parsing ─────────────────────────────────────────────────────────────

def parse_picks(text: str, post_date: date) -> list[dict]:
    picks = []
    pattern = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+"
        r"(.+?)\s+vs\s+"
        r"(.+?)\s+"
        r"((?:OVER|UNDER|SPLIT|SplitDD|Split\s+DD|\w+\s+-\d+\.?\d*).+?)$",
        re.IGNORECASE | re.MULTILINE,
    )
    now = EST_now()
    today = now.date()
    yesterday = today - timedelta(days=1)

    for m in pattern.finditer(text):
        time_str = m.group(1).strip()
        player1 = m.group(2).strip()
        player2 = m.group(3).strip()
        pick = m.group(4).strip()
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = EST.localize(datetime(post_date.year, post_date.month, post_date.day, t.hour, t.minute))

            if t.hour < 6:
                if post_date == yesterday:
                    # Overnight post from yesterday — assign to today
                    next_day = post_date + timedelta(days=1)
                    match_dt = EST.localize(datetime(next_day.year, next_day.month, next_day.day, t.hour, t.minute))
                elif post_date == today and match_dt < now:
                    # Posted today but time already passed — assign to tomorrow
                    next_day = today + timedelta(days=1)
                    match_dt = EST.localize(datetime(next_day.year, next_day.month, next_day.day, t.hour, t.minute))

        except ValueError:
            continue

        alert_key = f"{match_dt.strftime('%Y%m%d')}-{match_dt.strftime('%H%M')}-{player1.lower().replace(' ', '')}v{player2.lower().replace(' ', '')}"
        picks.append({
            "match_time": match_dt,
            "player1": player1,
            "player2": player2,
            "pick": pick,
            "alert_key": alert_key,
        })
    return picks


# ── Discord helpers ──────────────────────────────────────────────────────────

def build_alert_message(pick: dict) -> str:
    if isinstance(pick["match_time"], str):
        match_dt = datetime.fromisoformat(pick["match_time"]).astimezone(EST)
    else:
        match_dt = pick["match_time"]
    match_time_str = match_dt.strftime("%I:%M %p EDT")
    match_date_str = match_dt.strftime("%A, %m/%d/%Y")
    return (
        f"🏓 **MATCH STARTING IN 90 SECONDS!**\n\n"
        f"{pick['player1']} vs {pick['player2']}\n"
        f"Pick: {pick['pick']}\n"
        f"Date: {match_date_str}\n"
        f"Time: {match_time_str}\n\n"
        f"Good luck! 🍀"
    )


async def get_guild_id(session: aiohttp.ClientSession) -> str | None:
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch channel info: {r.status}")
            return None
        data = await r.json()
        return data.get("guild_id")


async def get_channel_messages(session: aiohttp.ClientSession) -> list:
    url = f"{DISCORD_API}/channels/{PICKS_CHANNEL_ID}/messages?limit=20"
    async with session.get(url, headers=DISCORD_HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch messages: {r.status}")
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
            retry_after = data.get("retry_after", 1)
            print(f"⏳ Rate limited, waiting {retry_after}s")
            await asyncio.sleep(retry_after)


# ── Message sync ─────────────────────────────────────────────────────────────

async def sync_picks_from_channel(session: aiohttp.ClientSession):
    messages = await get_channel_messages(session)
    today = EST_now().date()
    yesterday = today - timedelta(days=1)
    count = 0

    for msg in messages:
        if msg.get("author", {}).get("bot"):
            continue

        post_dt = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")).astimezone(EST)
        post_date = post_dt.date()

        if msg.get("edited_timestamp"):
            edit_dt = datetime.fromisoformat(msg["edited_timestamp"].replace("Z", "+00:00")).astimezone(EST)
            effective_date = edit_dt.date()
        else:
            effective_date = post_date

        if post_date not in (today, yesterday) and effective_date not in (today, yesterday):
            continue

        content = msg.get("content", "")
        if not content.strip():
            continue

        use_date = effective_date if effective_date in (today, yesterday) else post_date
        picks = parse_picks(content, use_date)

        for pick in picks:
            await db_upsert_pick(session, pick)
            count += 1

    if count:
        print(f"💾 Synced {count} picks to database.")


# ── Alert sender ─────────────────────────────────────────────────────────────

async def send_alerts(session: aiohttp.ClientSession, guild_id: str, pending: list, now_est: datetime):
    alerts_to_send = []
    for row in pending:
        match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
        seconds_until = (match_dt - now_est).total_seconds()
        print(f"⏱ {row['player1']} vs {row['player2']} in {int(seconds_until)}s")
        if 60 <= seconds_until <= 180:
            alerts_to_send.append(row)

    if not alerts_to_send:
        return

    members = await get_guild_members(session, guild_id)
    real_members = [m for m in members if not m.get("user", {}).get("bot")]

    for row in alerts_to_send:
        match_dt = datetime.fromisoformat(row["match_time"]).astimezone(EST)
        print(f"🚨 Sending alert: {row['player1']} vs {row['player2']}")
        alert_msg = build_alert_message({
            "match_time": match_dt,
            "player1": row["player1"],
            "player2": row["player2"],
            "pick": row["pick"],
        })
        count = 0
        for member in real_members:
            user = member.get("user", {})
            await send_dm(session, user["id"], alert_msg)
            count += 1
            await asyncio.sleep(0.5)
        print(f"✅ Alert sent to {count} members.")
        await db_mark_alert_sent(session, row["alert_key"])


# ── Main loop ────────────────────────────────────────────────────────────────

async def scanner_loop():
    print("🤖 TT Bot starting...")

    async with aiohttp.ClientSession() as session:
        guild_id = await get_guild_id(session)
        if guild_id is None:
            print("❌ Could not get guild ID. Check token and channel ID.")
            return
        print(f"✅ Connected to guild ID: {guild_id}")

        last_cleanup_date = None

        while True:
            try:
                now_est = EST_now()
                today = now_est.date()
                print(f"🔍 Scanning at {now_est.strftime('%H:%M:%S')} EST...")

                if last_cleanup_date != today:
                    await db_cleanup_old_picks(session)
                    last_cleanup_date = today

                await sync_picks_from_channel(session)

                pending = await db_get_pending_alerts(session)
                print(f"📋 {len(pending)} pending alerts in database.")

                await send_alerts(session, guild_id, pending, now_est)

            except Exception as e:
                print(f"❌ Scanner error: {e}")

            await asyncio.sleep(CHECK_INTERVAL)


asyncio.run(scanner_loop())
