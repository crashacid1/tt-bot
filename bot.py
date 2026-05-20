import discord
import asyncio
import re
import os
from datetime import datetime, date, timedelta
import pytz

# ── Config ──────────────────────────────────────────────────────────────────
TOKEN = os.environ["DISCORD_TOKEN"]
PICKS_CHANNEL_NAME = "table-tennis-picks-🍞🧈"
EST = pytz.timezone("US/Eastern")
CHECK_INTERVAL = 60  # seconds between scans

# ── Bot setup ────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
client = discord.Client(intents=intents)

# Tracks which alerts have already been sent: key = "MM/DD/YYYY-HH:MM-player1vplayer2"
sent_alerts: set[str] = set()
last_reset_date: date = None


def parse_picks(text: str, today: date) -> list[dict]:
    """
    Parse lines like:
      10:40am sturma vs Cecotka OVER and SplitDD
      01:40pm Krupnik vs Zika OVER

    Match times are always treated as TODAY's date.
    This supports overnight posts (posted yesterday for today's matches).
    """
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


async def send_dm_to_all_members(guild: discord.Guild, message: str):
    count = 0
    for member in guild.members:
        if member.bot:
            continue
        try:
            await member.send(message)
            count += 1
            await asyncio.sleep(0.5)  # avoid rate limits
        except discord.Forbidden:
            pass  # member has DMs disabled
        except Exception as e:
            print(f"Could not DM {member.name}: {e}")
    print(f"✅ Alert sent to {count} members.")


async def get_picks_channel(guild: discord.Guild):
    for channel in guild.text_channels:
        if channel.name == PICKS_CHANNEL_NAME:
            return channel
    return None


async def find_relevant_message(channel, today: date):
    """
    Scan the last 20 messages and find the best candidate:
    - Priority 1: a message posted OR edited today
    - Priority 2: a message posted yesterday (overnight post for today's matches)
    We pick the most recently active one.
    """
    yesterday = today - timedelta(days=1)
    best = None
    best_ts = None

    try:
        messages = [msg async for msg in channel.history(limit=20)]
    except Exception as e:
        print(f"Error reading channel: {e}")
        return None

    for msg in messages:
        post_date = msg.created_at.astimezone(EST).date()
        edit_date = msg.edited_at.astimezone(EST).date() if msg.edited_at else None

        # Most recent activity timestamp for this message
        last_activity = msg.edited_at if msg.edited_at else msg.created_at

        is_today = (post_date == today) or (edit_date == today)
        is_yesterday = (post_date == yesterday) or (edit_date == yesterday)

        if is_today:
            if best_ts is None or last_activity > best_ts:
                best = msg
                best_ts = last_activity
        elif is_yesterday and best is None:
            # Only use yesterday's message as fallback if nothing from today found
            if best_ts is None or last_activity > best_ts:
                best = msg
                best_ts = last_activity

    return best


async def scanner_loop():
    await client.wait_until_ready()
    global last_reset_date, sent_alerts

    print("🤖 Bot is running and scanning for picks...")

    while not client.is_closed():
        now_est = datetime.now(EST)
        today = now_est.date()

        # Reset sent alerts at midnight each day
        if last_reset_date != today:
            sent_alerts.clear()
            last_reset_date = today
            print(f"🔄 New day ({today}) — alert history cleared.")

        for guild in client.guilds:
            channel = await get_picks_channel(guild)
            if channel is None:
                print(f"⚠️  Could not find '{PICKS_CHANNEL_NAME}' in {guild.name}")
                continue

            msg = await find_relevant_message(channel, today)
            if msg is None:
                continue

            picks = parse_picks(msg.content, today)
            print(f"📋 Found {len(picks)} picks in message.")

            for pick in picks:
                if pick["alert_key"] in sent_alerts:
                    continue

                seconds_until = (pick["match_time"] - now_est).total_seconds()

                # Alert window: 90–150 seconds before match (centered on 2 min)
                if 90 <= seconds_until <= 150:
                    print(f"🚨 Alerting: {pick['player1']} vs {pick['player2']} at {pick['match_time'].strftime('%I:%M %p')}")
                    alert_msg = build_alert_message(pick)
                    await send_dm_to_all_members(guild, alert_msg)
                    sent_alerts.add(pick["alert_key"])

        await asyncio.sleep(CHECK_INTERVAL)


@client.event
async def on_ready():
    print(f"✅ Logged in as {client.user} (ID: {client.user.id})")
    client.loop.create_task(scanner_loop())


client.run(TOKEN)