import asyncio
import aiohttp
import json
import re
import os
from datetime import datetime, date, timedelta
import pytz
 
# ── Config ──────────────────────────────────────────────────────────────────
TOKEN = os.environ["DISCORD_TOKEN"]
PICKS_CHANNEL_ID = "1466857635746808020"
EST = pytz.timezone("US/Eastern")
CHECK_INTERVAL = 60
API = "https://discord.com/api/v10"
HEADERS = {"Authorization": f"Bot {TOKEN}", "Content-Type": "application/json"}
 
sent_alerts: set[str] = set()
last_reset_date: date = None
 
 
def parse_picks(text: str, today: date) -> list[dict]:
    picks = []
    pattern = re.compile(
        r"(\d{1,2}:\d{2}\s*(?:am|pm))\s+(.+?)\s+vs\s+(.+?)\s+((?:OVER|UNDER|SplitDD|Split DD).+?)$",
        re.IGNORECASE | re.MULTILINE,
    )
    for m in pattern.finditer(text):
        time_str = m.group(1).strip()
        player1 = m.group(2).strip()
        player2 = m.group(3).strip()
        pick = m.group(4).strip()
        try:
            t = datetime.strptime(time_str.replace(" ", "").upper(), "%I:%M%p")
            match_dt = EST.localize(datetime(today.year, today.month, today.day, t.hour, t.minute))
        except ValueError:
            continue
        alert_key = f"{today.strftime('%m/%d/%Y')}-{match_dt.strftime('%H:%M')}-{player1.lower()}v{player2.lower()}"
        picks.append({
            "match_time": match_dt,
            "player1": player1,
            "player2": player2,
            "pick": pick,
            "alert_key": alert_key,
        })
    return picks
 
 
def build_alert_message(pick: dict) -> str:
    match_time_str = pick["match_time"].strftime("%I:%M %p EDT")
    match_date_str = pick["match_time"].strftime("%A, %m/%d/%Y")
    return (
        f"🏓 **MATCH STARTING IN 2 MINUTES!**\n\n"
        f"{pick['player1']} vs {pick['player2']}\n"
        f"Pick: {pick['pick']}\n"
        f"Date: {match_date_str}\n"
        f"Time: {match_time_str}\n\n"
        f"Good luck! 🍀"
    )
 
 
async def get_guild_members(session: aiohttp.ClientSession, guild_id: str) -> list:
    members = []
    after = "0"
    while True:
        url = f"{API}/guilds/{guild_id}/members?limit=1000&after={after}"
        async with session.get(url, headers=HEADERS) as r:
            batch = await r.json()
            if not batch or not isinstance(batch, list):
                break
            members.extend(batch)
            if len(batch) < 1000:
                break
            after = batch[-1]["user"]["id"]
    return members
 
 
async def send_dm(session: aiohttp.ClientSession, user_id: str, message: str):
    # Create DM channel
    async with session.post(f"{API}/users/@me/channels",
                            headers=HEADERS,
                            json={"recipient_id": user_id}) as r:
        if r.status != 200:
            return
        dm = await r.json()
        dm_channel_id = dm["id"]
 
    # Send message
    async with session.post(f"{API}/channels/{dm_channel_id}/messages",
                            headers=HEADERS,
                            json={"content": message}) as r:
        if r.status == 429:
            data = await r.json()
            retry_after = data.get("retry_after", 1)
            print(f"⏳ Rate limited, waiting {retry_after}s")
            await asyncio.sleep(retry_after)
 
 
async def get_channel_messages(session: aiohttp.ClientSession) -> list:
    url = f"{API}/channels/{PICKS_CHANNEL_ID}/messages?limit=20"
    async with session.get(url, headers=HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch messages: {r.status}")
            return []
        return await r.json()
 
 
async def get_guild_id(session: aiohttp.ClientSession) -> str | None:
    url = f"{API}/channels/{PICKS_CHANNEL_ID}"
    async with session.get(url, headers=HEADERS) as r:
        if r.status != 200:
            print(f"⚠️ Failed to fetch channel info: {r.status}")
            return None
        data = await r.json()
        return data.get("guild_id")
 
 
async def find_relevant_message(messages: list, today: date):
    yesterday = today - timedelta(days=1)
    best = None
    best_ts = None
 
    for msg in messages:
        if msg.get("author", {}).get("bot"):
            continue
 
        post_dt = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00")).astimezone(EST)
        post_date = post_dt.date()
 
        edit_date = None
        if msg.get("edited_timestamp"):
            edit_dt = datetime.fromisoformat(msg["edited_timestamp"].replace("Z", "+00:00")).astimezone(EST)
            edit_date = edit_dt.date()
            last_activity = edit_dt
        else:
            last_activity = post_dt
 
        is_today = (post_date == today) or (edit_date == today)
        is_yesterday = (post_date == yesterday) or (edit_date == yesterday)
 
        if is_today:
            if best_ts is None or last_activity > best_ts:
                best = msg
                best_ts = last_activity
        elif is_yesterday and best is None:
            if best_ts is None or last_activity > best_ts:
                best = msg
                best_ts = last_activity
 
    return best
 
 
async def scanner_loop():
    global last_reset_date, sent_alerts
    print("🤖 Scanner loop started!")
 
    async with aiohttp.ClientSession() as session:
        # Get guild ID once
        guild_id = await get_guild_id(session)
        if guild_id is None:
            print("❌ Could not get guild ID. Check token and channel ID.")
            return
        print(f"✅ Connected to guild ID: {guild_id}")
 
        while True:
            try:
                now_est = datetime.now(EST)
                today = now_est.date()
 
                if last_reset_date != today:
                    sent_alerts.clear()
                    last_reset_date = today
                    print(f"🔄 New day ({today}) — alert history cleared.")
 
                print(f"🔍 Scanning at {now_est.strftime('%H:%M:%S')} EST...")
                messages = await get_channel_messages(session)
                msg = await find_relevant_message(messages, today)
 
                if msg is None:
                    print("📭 No relevant message found for today.")
                else:
                    content = msg.get("content", "")
                    print(f"📨 Found message: {content[:80]}")
                    picks = parse_picks(content, today)
                    print(f"📋 Parsed {len(picks)} picks.")
 
                    for pick in picks:
                        if pick["alert_key"] in sent_alerts:
                            continue
                        seconds_until = (pick["match_time"] - now_est).total_seconds()
                        print(f"⏱ {pick['player1']} vs {pick['player2']} in {int(seconds_until)}s")
 
                        if 90 <= seconds_until <= 150:
                            print(f"🚨 Sending alert: {pick['player1']} vs {pick['player2']}")
                            alert_msg = build_alert_message(pick)
                            members = await get_guild_members(session, guild_id)
                            count = 0
                            for member in members:
                                user = member.get("user", {})
                                if user.get("bot"):
                                    continue
                                await send_dm(session, user["id"], alert_msg)
                                count += 1
                                await asyncio.sleep(0.5)
                            print(f"✅ Alert sent to {count} members.")
                            sent_alerts.add(pick["alert_key"])
 
            except Exception as e:
                print(f"❌ Scanner error: {e}")
 
            await asyncio.sleep(CHECK_INTERVAL)
 
 
asyncio.run(scanner_loop())
