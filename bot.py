import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from html import unescape
from urllib.parse import urlencode, urljoin

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "").strip()
BOT_DESCRIPTION = "made by dolev the goat himself"


def env_int(name, default=0):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


SCORES_CHANNEL_ID = env_int("SCORES_CHANNEL_ID")
SCORES_CHANNEL_NAME = os.getenv("SCORES_CHANNEL_NAME", "").strip().lstrip("#")
FOLLOWED_GAMES_FILE = os.path.join(os.path.dirname(__file__), "followed_games.json")
SERVER_CHANNELS_FILE = os.path.join(os.path.dirname(__file__), "server_channels.json")
SUBSCRIBER_ROLES_FILE = os.path.join(os.path.dirname(__file__), "subscriber_roles.json")
HELP_PUBLIC_POSTS_FILE = os.path.join(os.path.dirname(__file__), "help_public_posts.json")
HELP_PUBLIC_COOLDOWN_SECONDS = 24 * 60 * 60
LIVE_NOTIFICATIONS_ROLE_NAME = (
    os.getenv("LIVE_NOTIFICATIONS_ROLE_NAME", "Live Notifications (LiveScoreHBS)").strip()
    or "Live Notifications (LiveScoreHBS)"
)
LEGACY_LIVE_NOTIFICATIONS_ROLE_NAMES = ("Live Notifications",)
REQUIRED_CHANNEL_PERMISSIONS = {
    "view_channel": "View Channel",
    "send_messages": "Send Messages",
    "embed_links": "Embed Links",
    "read_message_history": "Read Message History",
}
REQUIRED_GUILD_PERMISSIONS = {
    "manage_roles": "Manage Roles",
}
REQUIRED_BOT_PERMISSIONS = {**REQUIRED_CHANNEL_PERMISSIONS, **REQUIRED_GUILD_PERMISSIONS}

BEER_SHEVA_365 = 579
MACCABI_HAIFA_365 = 562
HAPOEL_TEL_AVIV_365 = 567
LEAGUE_365 = 42
API_365 = "https://webws.365scores.com/web"
BASE_PARAMS_365 = {"appTypeId": 5, "langId": 9, "timezoneName": "UTC"}
EXTRA_LIVE_MATCHES = [
    {
        "name": "Hapoel Tel Aviv vs Maccabi Haifa",
        "competitor_ids": {HAPOEL_TEL_AVIV_365, MACCABI_HAIFA_365},
        "active_through": "2026-04-30",
    }
]
LEAGUE_TEAMS = [
    {"id": 559, "name": "Beitar Jerusalem", "aliases": ["beitar", "jerusalem"]},
    {"id": 579, "name": "Hapoel Beer Sheva", "aliases": ["beer sheva", "be'er sheva", "hbs"]},
    {"id": 566, "name": "Maccabi Tel Aviv", "aliases": ["maccabi tel aviv", "mta"]},
    {"id": 567, "name": "Hapoel Tel Aviv", "aliases": ["hapoel tel aviv", "hta"]},
    {"id": 562, "name": "Maccabi Haifa", "aliases": ["haifa", "mha"]},
    {"id": 571, "name": "Hapoel Petah Tikva", "aliases": ["petah tikva", "hpt"]},
    {"id": 560, "name": "Maccabi Netanya", "aliases": ["netanya"]},
    {"id": 561, "name": "Bnei Sakhnin", "aliases": ["sakhnin"]},
    {"id": 563, "name": "Kiryat Shmona", "aliases": ["shmona"]},
    {"id": 575, "name": "Hapoel Haifa", "aliases": ["hapoel haifa", "hha"]},
    {"id": 614, "name": "Hapoel Jerusalem", "aliases": ["hapoel jerusalem", "hje"]},
    {"id": 569, "name": "SC Ashdod", "aliases": ["ashdod"]},
    {"id": 606, "name": "Ironi Tiberias", "aliases": ["tiberias"]},
    {"id": 45617, "name": "Maccabi Bnei Reineh", "aliases": ["bnei reineh", "bnei raina", "reineh"]},
]

BETEXPLORER_BASE_URL = "https://www.betexplorer.com"
BETEXPLORER_TEAM_FIXTURES_URL = f"{BETEXPLORER_BASE_URL}/football/team/h-beer-sheva/EXAD1YZP/fixtures/"
BETEXPLORER_TEAM_RESULTS_URL = f"{BETEXPLORER_BASE_URL}/football/team/h-beer-sheva/EXAD1YZP/results/"

HEADERS_365 = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.365scores.com",
    "Referer": "https://www.365scores.com/",
}

ODDS_HEADERS = {
    "User-Agent": HEADERS_365["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BETEXPLORER_BASE_URL,
}

live_message_id = None
last_goals_count = -1
last_red_cards_count = -1
tracked_live_game_id = None
notified_goal_keys = set()
notified_red_card_keys = set()
notified_match_state_keys = set()
scores_channel_error_logged = False
bot_description_synced = False
followed_games = {}
live_game_states = {}
follow_match_cache = {"fetched_at": 0, "games": []}
server_channels = {}
subscriber_roles = {}
help_public_posts = {}

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)


def get_required_bot_permissions():
    permissions = discord.Permissions.none()
    for attr in REQUIRED_BOT_PERMISSIONS:
        setattr(permissions, attr, True)
    return permissions


BOT_REQUIRED_PERMISSIONS = get_required_bot_permissions()


def get_invite_client_id():
    if re.fullmatch(r"\d{15,25}", DISCORD_CLIENT_ID):
        return DISCORD_CLIENT_ID
    return str(bot.user.id) if bot.user else ""


def get_bot_invite_url(guild_id=None):
    client_id = get_invite_client_id()
    if not client_id:
        return None

    params = {
        "client_id": client_id,
        "permissions": str(BOT_REQUIRED_PERMISSIONS.value),
        "scope": "bot applications.commands",
        "integration_type": "0",
    }
    if guild_id:
        params["guild_id"] = str(guild_id)
        params["disable_guild_select"] = "true"

    return "https://discord.com/oauth2/authorize?" + urlencode(params)


async def sync_bot_description():
    global bot_description_synced

    if bot_description_synced or not DISCORD_TOKEN:
        return

    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(
                "https://discord.com/api/v10/applications/@me",
                headers={
                    "Authorization": f"Bot {DISCORD_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"description": BOT_DESCRIPTION},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                if response.status == 200:
                    bot_description_synced = True
                    print("✅ Bot bio synced")
                    return

                error_text = await response.text()
                print(f"[discord] Could not sync bot bio ({response.status}): {error_text[:200]}")
    except Exception as exc:
        print(f"[discord] Could not sync bot bio: {exc}")


async def fetch_365_json(session, path, params=None):
    try:
        async with session.get(
            f"{API_365}{path}",
            headers=HEADERS_365,
            params={**BASE_PARAMS_365, **(params or {})},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            if response.status != 200:
                print(f"[365] {response.status} on {path}")
                return None
            return await response.json(content_type=None)
    except Exception as exc:
        print(f"[365] {exc}")
        return None


async def fetch_365_games(session, endpoint, competitor_id=BEER_SHEVA_365):
    data = await fetch_365_json(
        session,
        f"/games/{endpoint}/",
        {"competitors": competitor_id},
    )
    if not data:
        return []
    return data.get("games", [])


async def fetch_watched_365_games(session, endpoint):
    competitor_ids = [BEER_SHEVA_365]
    if has_active_extra_live_match():
        for match in EXTRA_LIVE_MATCHES:
            competitor_ids.extend(match["competitor_ids"])

    games = []
    seen = set()
    for competitor_id in dict.fromkeys(competitor_ids):
        for game in await fetch_365_games(session, endpoint, competitor_id):
            key = get_game_key(game)
            if key in seen:
                continue
            seen.add(key)
            games.append(game)

    return games


async def get_365_game(session, game_id):
    if not game_id:
        return None
    data = await fetch_365_json(session, "/game/", {"gameId": game_id})
    if not data:
        return None
    return data.get("game")


async def get_standings(session):
    data = await fetch_365_json(session, "/standings/", {"competitions": LEAGUE_365})
    if not data:
        return None
    standings = data.get("standings", [])
    return standings[0] if standings else None


async def get_league_stats(session):
    return await fetch_365_json(session, "/stats/", {"competitions": LEAGUE_365})


async def fetch_league_games(session, endpoint):
    data = await fetch_365_json(session, f"/games/{endpoint}/", {"competitions": LEAGUE_365})
    if not data:
        return []

    games = data.get("games", [])
    reverse = endpoint == "results"
    return sorted(games, key=game_sort_key, reverse=reverse)


def is_beer_sheva_365(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return (
        home.get("id") == BEER_SHEVA_365
        or away.get("id") == BEER_SHEVA_365
        or "Hapoel Beer Sheva" in home.get("name", "")
        or "Hapoel Beer Sheva" in away.get("name", "")
    )


def has_active_extra_live_match():
    today = datetime.now(timezone.utc).date()
    for match in EXTRA_LIVE_MATCHES:
        try:
            active_through = datetime.fromisoformat(match["active_through"]).date()
        except Exception:
            continue
        if today <= active_through:
            return True
    return False


def is_extra_live_match(game):
    if not has_active_extra_live_match():
        return False

    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    competitor_ids = {home.get("id"), away.get("id")}
    return any(competitor_ids == match["competitor_ids"] for match in EXTRA_LIVE_MATCHES)


def is_watched_live_game(game):
    return is_beer_sheva_365(game) or is_extra_live_match(game)


def is_live(game):
    return game.get("statusGroup") == 3


def is_done(game):
    return game.get("statusGroup") == 4


def is_upcoming(game):
    return game.get("statusGroup") == 2


def score_as_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def fmt_score(value):
    try:
        return str(int(float(value)))
    except Exception:
        return "-"


def dts(unix_ts, fmt="F"):
    return f"<t:{int(unix_ts)}:{fmt}>"


def start_to_unix(start_str):
    try:
        normalized = str(start_str).replace("Z", "+00:00")
        start = datetime.fromisoformat(normalized)
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        return int(start.astimezone(timezone.utc).timestamp())
    except Exception:
        return None


def game_sort_key(game):
    return start_to_unix(game.get("startTime", "")) or 0


def get_game_key(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return str(
        game.get("id")
        or f"{home.get('id')}:{away.get('id')}:{game.get('startTime', '')}"
    )


def new_live_state():
    return {
        "live_message_id": None,
        "last_goals_count": -1,
        "last_red_cards_count": -1,
        "notified_goal_keys": set(),
        "notified_red_card_keys": set(),
        "notified_match_state_keys": set(),
        "initialized": False,
    }


def get_live_state(game_key):
    return live_game_states.setdefault(game_key, new_live_state())


def get_channel_live_state(game, channel):
    return get_live_state(f"{get_game_key(game)}:{channel.id}")


def remove_live_states_for_game(game_key):
    for state_key in list(live_game_states):
        if state_key == game_key or state_key.startswith(f"{game_key}:"):
            live_game_states.pop(state_key, None)


def load_followed_games():
    global followed_games

    try:
        with open(FOLLOWED_GAMES_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        followed_games = {}
        return
    except Exception as exc:
        print(f"[follow] Could not read followed games: {exc}")
        followed_games = {}
        return

    if isinstance(data, dict):
        followed_games = data
    else:
        followed_games = {}


def save_followed_games():
    try:
        with open(FOLLOWED_GAMES_FILE, "w", encoding="utf-8") as file:
            json.dump(followed_games, file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[follow] Could not save followed games: {exc}")


def load_server_channels():
    global server_channels

    try:
        with open(SERVER_CHANNELS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        server_channels = {}
        return
    except Exception as exc:
        print(f"[discord] Could not read server channels: {exc}")
        server_channels = {}
        return

    server_channels = data if isinstance(data, dict) else {}


def save_server_channels():
    try:
        with open(SERVER_CHANNELS_FILE, "w", encoding="utf-8") as file:
            json.dump(server_channels, file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[discord] Could not save server channels: {exc}")


def load_subscriber_roles():
    global subscriber_roles

    try:
        with open(SUBSCRIBER_ROLES_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        subscriber_roles = {}
        return
    except Exception as exc:
        print(f"[discord] Could not read subscriber roles: {exc}")
        subscriber_roles = {}
        return

    subscriber_roles = data if isinstance(data, dict) else {}


def save_subscriber_roles():
    try:
        with open(SUBSCRIBER_ROLES_FILE, "w", encoding="utf-8") as file:
            json.dump(subscriber_roles, file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[discord] Could not save subscriber roles: {exc}")


def load_help_public_posts():
    global help_public_posts

    try:
        with open(HELP_PUBLIC_POSTS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        help_public_posts = {}
        return
    except Exception as exc:
        print(f"[discord] Could not read help public posts: {exc}")
        help_public_posts = {}
        return

    help_public_posts = data if isinstance(data, dict) else {}


def save_help_public_posts():
    try:
        with open(HELP_PUBLIC_POSTS_FILE, "w", encoding="utf-8") as file:
            json.dump(help_public_posts, file, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"[discord] Could not save help public posts: {exc}")


def get_help_public_key(interaction):
    if interaction.guild and interaction.channel:
        return f"{interaction.guild.id}:{interaction.channel.id}"
    if interaction.guild:
        return str(interaction.guild.id)
    return None


def should_send_help_publicly(interaction):
    key = get_help_public_key(interaction)
    if not key:
        return False

    now_ts = int(datetime.now(timezone.utc).timestamp())
    try:
        last_post_ts = int(help_public_posts.get(key, 0))
    except (TypeError, ValueError):
        last_post_ts = 0

    if now_ts - last_post_ts < HELP_PUBLIC_COOLDOWN_SECONDS:
        return False

    help_public_posts[key] = now_ts
    save_help_public_posts()
    return True


def remember_subscriber_role(guild, role):
    guild_id = str(guild.id)
    role_id = str(role.id)
    if str(subscriber_roles.get(guild_id, "")) == role_id:
        return

    subscriber_roles[guild_id] = role_id
    save_subscriber_roles()


def get_subscriber_role(guild):
    if not guild:
        return None

    role = None
    role_id = subscriber_roles.get(str(guild.id))
    if role_id:
        try:
            role = guild.get_role(int(role_id))
        except (TypeError, ValueError):
            role = None

    if not role:
        role = discord.utils.get(guild.roles, name=LIVE_NOTIFICATIONS_ROLE_NAME)

    if not role:
        for legacy_name in LEGACY_LIVE_NOTIFICATIONS_ROLE_NAMES:
            role = discord.utils.get(guild.roles, name=legacy_name)
            if role:
                break

    if role:
        remember_subscriber_role(guild, role)
    return role


def get_bot_guild_member(guild):
    if not guild:
        return None
    if guild.me:
        return guild.me
    if bot.user:
        return guild.get_member(bot.user.id)
    return None


def get_interaction_member(interaction):
    if isinstance(interaction.user, discord.Member):
        return interaction.user
    if interaction.guild:
        return interaction.guild.get_member(interaction.user.id)
    return None


def bot_can_manage_subscriber_role(guild, role):
    me = get_bot_guild_member(guild)
    return bool(me and me.guild_permissions.manage_roles and role < me.top_role)


async def rename_subscriber_role_if_needed(guild, role):
    changes = {}
    if role.name != LIVE_NOTIFICATIONS_ROLE_NAME:
        changes["name"] = LIVE_NOTIFICATIONS_ROLE_NAME
    if not role.mentionable:
        changes["mentionable"] = True

    if not changes:
        return role, None

    try:
        role = await role.edit(
            **changes,
            reason="Update live notification subscriber role",
        )
    except discord.Forbidden:
        return (
            None,
            f"I can see `{role.name}`, but I cannot update it. Move my role above it.",
        )
    except discord.HTTPException as exc:
        return None, f"Discord would not let me update `{role.name}`: {exc}"

    remember_subscriber_role(guild, role)
    return role, None


async def ensure_subscriber_role(guild):
    me = get_bot_guild_member(guild)
    if not me or not me.guild_permissions.manage_roles:
        return (
            None,
            "I need the Manage Roles permission first. Use `/permissions`, then re-add or update the bot permissions.",
        )

    role = get_subscriber_role(guild)
    if role:
        if not bot_can_manage_subscriber_role(guild, role):
            return (
                None,
                f"I found the `{role.name}` role, but my Discord role must be above it in Server Settings > Roles.",
            )

        role, error = await rename_subscriber_role_if_needed(guild, role)
        if error:
            return None, error

        remember_subscriber_role(guild, role)
        return role, None

    try:
        role = await guild.create_role(
            name=LIVE_NOTIFICATIONS_ROLE_NAME,
            mentionable=True,
            reason="Live notification subscribers",
        )
    except discord.Forbidden:
        return (
            None,
            "I could not create the live notification role. I need Manage Roles, and my role must be high enough.",
        )
    except discord.HTTPException as exc:
        return None, f"Discord would not let me create the live notification role: {exc}"

    remember_subscriber_role(guild, role)
    return role, None


async def sync_subscriber_roles():
    for guild in bot.guilds:
        role = get_subscriber_role(guild)
        if not role or role.name == LIVE_NOTIFICATIONS_ROLE_NAME:
            continue
        if not bot_can_manage_subscriber_role(guild, role):
            print(
                f"[discord] Could not rename subscriber role in {guild.name}: "
                "bot role must be above it."
            )
            continue

        updated_role, error = await rename_subscriber_role_if_needed(guild, role)
        if error:
            print(f"[discord] Could not rename subscriber role in {guild.name}: {error}")
            continue
        print(f"[discord] Subscriber role renamed in {guild.name}: {updated_role.name}")


def get_live_notification_role(channel):
    guild = getattr(channel, "guild", None)
    if not guild:
        return None
    return get_subscriber_role(guild)


async def send_live_notification(channel, message):
    role = get_live_notification_role(channel)
    if not role:
        await channel.send(message)
        return

    allowed_mentions = discord.AllowedMentions(
        everyone=False,
        users=False,
        roles=[role],
        replied_user=False,
    )
    await channel.send(f"{role.mention} {message}", allowed_mentions=allowed_mentions)


def followed_game_record(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return {
        "id": game.get("id"),
        "home_id": home.get("id"),
        "away_id": away.get("id"),
        "home_name": home.get("name", "?"),
        "away_name": away.get("name", "?"),
        "startTime": game.get("startTime", ""),
        "competitionDisplayName": game.get("competitionDisplayName", "Premier League"),
    }


def followed_game_label(record, *, discord_timestamp=False):
    start_ts = start_to_unix(record.get("startTime", ""))
    if not start_ts:
        date_text = "TBD"
    elif discord_timestamp:
        date_text = dts(start_ts, "f")
    else:
        date_text = datetime.fromtimestamp(start_ts, timezone.utc).strftime("%d/%m %H:%M UTC")
    return f"{record.get('home_name', '?')} vs {record.get('away_name', '?')} ({date_text})"


def get_total_goals(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return score_as_int(home.get("score"), 0) + score_as_int(away.get("score"), 0)


def get_total_red_cards(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return score_as_int(home.get("redCards"), 0) + score_as_int(away.get("redCards"), 0)


def get_game_minute(game):
    for value in (game.get("gameTime"), game.get("gameTimeDisplay")):
        if value is None:
            continue
        match = re.search(r"\d+", str(value))
        if match:
            return int(match.group(0))
    return None


def get_status_texts(game):
    texts = []
    for key in (
        "statusText",
        "shortStatusText",
        "statusTextShort",
        "gameTimeDisplay",
        "statusName",
    ):
        value = game.get(key)
        if value:
            texts.append(str(value))

    status = game.get("status")
    if isinstance(status, dict):
        for key in ("name", "shortName", "displayName"):
            value = status.get(key)
            if value:
                texts.append(str(value))

    return texts


def has_status_text(game, *needles):
    status_text = " ".join(get_status_texts(game)).lower()
    return any(needle.lower() in status_text for needle in needles)


def is_half_time_status(game):
    status_text = " ".join(get_status_texts(game)).lower()
    return (
        has_status_text(game, "half time", "halftime", "half-time")
        or re.search(r"\bht\b", status_text) is not None
    )


def is_second_half_status(game):
    minute = get_game_minute(game)
    if minute is not None and 46 <= minute <= 60:
        return True
    return has_status_text(game, "2nd half", "second half", "2h")


def format_scoreline_team(name, own_score, opponent_score):
    own = score_as_int(own_score, None)
    opponent = score_as_int(opponent_score, None)
    if own is not None and opponent is not None and own > opponent:
        return f"**{name}**"
    return name


def get_beer_sheva_row(standings):
    if not standings:
        return None
    for row in standings.get("rows", []):
        competitor = row.get("competitor", {})
        if competitor.get("id") == BEER_SHEVA_365:
            return row
    return None


async def get_last_365_game(session):
    games = await fetch_365_games(session, "current")
    finished_games = sorted(
        [game for game in games if is_beer_sheva_365(game) and is_done(game)],
        key=game_sort_key,
        reverse=True,
    )

    for game in finished_games:
        details = await get_365_game(session, game.get("id"))
        if details:
            return details

    standings = await get_standings(session)
    row = get_beer_sheva_row(standings)
    if not row:
        return None

    for game in row.get("detailedRecentForm", []):
        if game.get("statusGroup") == 4:
            details = await get_365_game(session, game.get("id"))
            if details:
                return details

    return None


def get_member_lookup(game):
    lookup = {}
    for member in game.get("members", []):
        member_id = member.get("id")
        if member_id is not None:
            lookup[member_id] = member.get("name") or member.get("shortName") or "Unknown"
    return lookup


def get_competitor_label(game, competitor_id):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    if competitor_id == home.get("id"):
        return home.get("symbolicName") or home.get("name", "Home")
    if competitor_id == away.get("id"):
        return away.get("symbolicName") or away.get("name", "Away")
    return "?"


def get_stage_score(game, short_name):
    for stage in game.get("stages", []):
        if stage.get("shortName") == short_name:
            return stage.get("homeCompetitorScore"), stage.get("awayCompetitorScore")
    return None, None


def event_sort_key(event):
    return (event.get("gameTime", 0), event.get("addedTime", 0), event.get("order", 0))


def get_sorted_events(game):
    return sorted(game.get("events", []), key=event_sort_key)


def format_event_minute(minute):
    text = str(minute or "?")
    if text != "?" and not text.endswith("'"):
        return f"{text}'"
    return text


def get_event_player_name(game, event):
    return get_member_lookup(game).get(event.get("playerId"), "Unknown")


def format_goal_label(game, event):
    subtype = event.get("eventType", {}).get("subTypeName", "")
    player = get_event_player_name(game, event)
    label = player

    if subtype == "Penalty":
        label += " (pen)"
    elif subtype == "Own Goal":
        label += " (og)"

    if subtype != "Own Goal":
        assist_ids = event.get("extraPlayers", [])
        if assist_ids:
            assist = get_member_lookup(game).get(assist_ids[0])
            if assist:
                label += f" (ast. {assist})"

    return label


def format_red_card_player(game, event):
    subtype = event.get("eventType", {}).get("subTypeName", "")
    player = get_event_player_name(game, event)
    card_prefix = "🟥"

    if "Second Yellow" in subtype:
        card_prefix = "🟨🟨🟥"
        player += " (second yellow)"
    elif subtype:
        player += f" ({subtype.lower()})"

    return card_prefix, player


def live_event_key(event):
    for key in ("id", "eventId", "eventID"):
        if event.get(key) is not None:
            return str(event.get(key))

    event_type = event.get("eventType", {})
    parts = [
        event_type.get("name", ""),
        event_type.get("subTypeName", ""),
        event.get("gameTime", ""),
        event.get("gameTimeDisplay", ""),
        event.get("addedTime", ""),
        event.get("order", ""),
        event.get("playerId", ""),
        event.get("competitorId", ""),
    ]
    return "|".join(str(part) for part in parts)


def get_live_events_by_type(game, event_type_name):
    return [
        event
        for event in get_sorted_events(game)
        if event.get("eventType", {}).get("name") == event_type_name
    ]


def is_red_card_event(event):
    event_type = event.get("eventType", {})
    name = event_type.get("name", "")
    subtype = event_type.get("subTypeName", "")
    return name == "Red Card" or "Red Card" in subtype or "Second Yellow" in subtype


def get_live_red_card_events(game):
    return [event for event in get_sorted_events(game) if is_red_card_event(event)]


def format_live_goal_notification(game, event):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_name = home.get("name", "?")
    away_name = away.get("name", "?")
    home_goals = fmt_score(home.get("score", 0))
    away_goals = fmt_score(away.get("score", 0))
    minute = format_event_minute(event.get("gameTimeDisplay", "?"))
    team = get_competitor_label(game, event.get("competitorId"))
    label = format_goal_label(game, event)

    return (
        f"⚽ **GOAL!** `{minute}` {label} [{team}] · "
        f"{home_name} **{home_goals}** - **{away_goals}** {away_name}"
    )


def format_live_red_card_notification(game, event):
    minute = format_event_minute(event.get("gameTimeDisplay", "?"))
    team = get_competitor_label(game, event.get("competitorId"))
    card_prefix, player = format_red_card_player(game, event)
    return f"{card_prefix} **RED CARD** `{minute}` {player} [{team}]"


def get_match_name(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return f"{home.get('name', '?')} vs {away.get('name', '?')}"


def get_match_scoreline(game, bold_winner=False):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_name = home.get("name", "?")
    away_name = away.get("name", "?")
    home_score = home.get("score")
    away_score = away.get("score")
    home_goals = fmt_score(home_score)
    away_goals = fmt_score(away_score)

    if bold_winner:
        home_name = format_scoreline_team(home_name, home_score, away_score)
        away_name = format_scoreline_team(away_name, away_score, home_score)

    return f"{home_name} **{home_goals}** - **{away_goals}** {away_name}"


def format_generic_goal_notification(game):
    minute = format_event_minute(game.get("gameTimeDisplay", "") or score_as_int(game.get("gameTime"), 0))
    return f"⚽ **GOAL!** {get_match_scoreline(game)} · {minute}"


def format_generic_red_card_notification(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    return (
        f"🟥 **RED CARD** {home.get('name', '?')}: {home.get('redCards', 0)} | "
        f"{away.get('name', '?')}: {away.get('redCards', 0)}"
    )


def format_match_state_notification(game, state):
    if state == "kickoff_soon":
        start_ts = start_to_unix(game.get("startTime", ""))
        start_text = f" · {dts(start_ts, 't')}" if start_ts else ""
        return f"⏰ **Kickoff in 15 minutes** {get_match_name(game)}{start_text}"
    if state == "started":
        return f"🟢 **Kickoff!** {get_match_scoreline(game)}"
    if state == "half_time":
        return f"🟡 **Half time** {get_match_scoreline(game)}"
    if state == "second_half":
        return f"▶️ **Second half started** {get_match_scoreline(game)}"
    if state == "full_time":
        return f"🏁 **Full time** {get_match_scoreline(game, bold_winner=True)}"
    return None


def missing_bot_channel_permissions(channel):
    guild = getattr(channel, "guild", None)
    me = getattr(guild, "me", None)
    if not me:
        return []

    permissions = channel.permissions_for(me)
    return [
        label
        for attr, label in REQUIRED_CHANNEL_PERMISSIONS.items()
        if not getattr(permissions, attr, False)
    ]


async def get_configured_scores_channels():
    channels = []
    stale_guild_ids = []
    seen = set()

    for guild_id, channel_id in list(server_channels.items()):
        try:
            channel_id = int(channel_id)
        except (TypeError, ValueError):
            stale_guild_ids.append(guild_id)
            continue

        channel = bot.get_channel(channel_id)
        if not channel:
            try:
                channel = await bot.fetch_channel(channel_id)
            except Exception as exc:
                print(f"[discord] Cannot fetch configured channel {channel_id}: {exc}")
                stale_guild_ids.append(guild_id)
                continue

        if missing_bot_channel_permissions(channel):
            print(
                f"[discord] Missing permissions for #{channel.name} "
                f"({channel.id}): {', '.join(missing_bot_channel_permissions(channel))}"
            )
            continue

        if channel.id not in seen:
            seen.add(channel.id)
            channels.append(channel)

    for guild_id in stale_guild_ids:
        server_channels.pop(str(guild_id), None)
    if stale_guild_ids:
        save_server_channels()

    return channels


async def get_scores_channel():
    global scores_channel_error_logged

    if not SCORES_CHANNEL_ID:
        channel = find_scores_channel_by_name()
        if channel:
            return channel
        log_scores_channel_help("SCORES_CHANNEL_ID is not set")
        return None

    channel = bot.get_channel(SCORES_CHANNEL_ID)
    if channel:
        return channel

    try:
        return await bot.fetch_channel(SCORES_CHANNEL_ID)
    except Exception as exc:
        channel = find_scores_channel_by_name()
        if channel:
            if not scores_channel_error_logged:
                print(
                    f"[discord] SCORES_CHANNEL_ID={SCORES_CHANNEL_ID} failed ({exc}); "
                    f"using #{channel.name} from SCORES_CHANNEL_NAME instead."
                )
                scores_channel_error_logged = True
            return channel

        log_scores_channel_help(f"Cannot fetch SCORES_CHANNEL_ID={SCORES_CHANNEL_ID}: {exc}")
        return None


async def get_scores_channels():
    channels = await get_configured_scores_channels()
    if channels:
        return channels

    fallback_channel = await get_scores_channel()
    return [fallback_channel] if fallback_channel else []


def get_accessible_text_channels():
    channels = []
    for guild in bot.guilds:
        me = guild.me
        for channel in guild.text_channels:
            try:
                permissions = channel.permissions_for(me) if me else None
                if permissions and not (permissions.view_channel and permissions.send_messages):
                    continue
            except Exception:
                pass
            channels.append(channel)
    return channels


def find_scores_channel_by_name():
    if not SCORES_CHANNEL_NAME:
        return None

    target = SCORES_CHANNEL_NAME.lower()
    for channel in get_accessible_text_channels():
        if channel.name.lower() == target:
            return channel

    return None


def log_scores_channel_help(reason):
    global scores_channel_error_logged

    if scores_channel_error_logged:
        return

    scores_channel_error_logged = True
    print(f"[discord] {reason}")
    print("[discord] Fix: run /setup in the Discord channel where live notifications should appear.")
    print("[discord] Fallback: set SCORES_CHANNEL_ID in .env to a text channel ID the bot can access.")
    print("[discord] Optional fallback: set SCORES_CHANNEL_NAME=channel-name in .env.")

    channels = get_accessible_text_channels()
    if not channels:
        print("[discord] The bot cannot see any text channels. Check server invite and channel permissions.")
        return

    print("[discord] Text channels the bot can send to:")
    for channel in channels[:30]:
        print(f"[discord] - {channel.guild.name} / #{channel.name}: {channel.id}")

    if len(channels) > 30:
        print(f"[discord] ...and {len(channels) - 30} more")


async def send_match_state_once(channel, game, state, live_state=None):
    global notified_match_state_keys

    game_key = get_game_key(game)
    notification_key = f"{game_key}:{state}"
    state_keys = (
        live_state["notified_match_state_keys"]
        if live_state is not None
        else notified_match_state_keys
    )
    if notification_key in state_keys:
        return False

    message = format_match_state_notification(game, state)
    if not message:
        return False

    await send_live_notification(channel, message)
    state_keys.add(notification_key)
    return True


def find_current_watched_game(games, tracked_game_key=None):
    watched_games = [game for game in games if is_watched_live_game(game)]
    if not watched_games:
        return None

    live_games = sorted([game for game in watched_games if is_live(game)], key=game_sort_key)
    if live_games:
        return live_games[0]

    if tracked_game_key:
        for game in watched_games:
            if get_game_key(game) == tracked_game_key and (is_upcoming(game) or is_done(game)):
                return game

    upcoming_games = sorted([game for game in watched_games if is_upcoming(game)], key=game_sort_key)
    if upcoming_games:
        return upcoming_games[0]

    now_ts = int(datetime.now(timezone.utc).timestamp())
    finished_games = sorted(
        [game for game in watched_games if is_done(game)],
        key=game_sort_key,
        reverse=True,
    )
    for game in finished_games:
        start_ts = start_to_unix(game.get("startTime", ""))
        if start_ts and 0 <= now_ts - start_ts <= 6 * 60 * 60:
            return game

    return None


def get_game_goals_and_red_cards(game):
    goals = []
    cards = []

    for event in get_sorted_events(game):
        event_type = event.get("eventType", {}).get("name", "")
        minute = format_event_minute(event.get("gameTimeDisplay", "?"))
        competitor_id = event.get("competitorId")
        team = get_competitor_label(game, competitor_id)

        if event_type == "Goal":
            goals.append(
                {
                    "minute": minute,
                    "label": format_goal_label(game, event),
                    "team": team,
                    "competitor_id": competitor_id,
                }
            )

        elif is_red_card_event(event):
            card_prefix, player = format_red_card_player(game, event)
            cards.append(f"{minute} {card_prefix} {player} [{team}]")

    return goals, cards


def get_stadium_name(game):
    return game.get("venue", {}).get("name")


def normalize_team_name(name):
    normalized = unicodedata.normalize("NFKD", name or "")
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("no away supporters", " ")
    normalized = re.sub(r"\b(fc|cf|sc|ac|afc)\b", " ", normalized)
    normalized = normalized.replace("be'er", "beer")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


TEAM_NAME_ALIASES = {
    "hapoel beer sheva": {"hapoel beer sheva", "h beer sheva", "h beer sheva fc"},
    "maccabi tel aviv": {"maccabi tel aviv", "maccabi tel aviv fc"},
    "beitar jerusalem": {"beitar jerusalem"},
    "hapoel tel aviv": {"hapoel tel aviv"},
    "maccabi haifa": {"maccabi haifa"},
    "hapoel petah tikva": {"hapoel petah tikva"},
    "sc ashdod": {"sc ashdod", "ashdod"},
    "bnei sakhnin": {"bnei sakhnin", "sakhnin"},
    "kiryat shmona": {"kiryat shmona"},
    "ironi tiberias": {"ironi tiberias"},
    "hapoel haifa": {"hapoel haifa"},
    "maccabi bnei reineh": {"maccabi bnei reineh", "maccabi bnei raina"},
    "hapoel jerusalem": {"hapoel jerusalem"},
    "bnei yehuda": {"bnei yehuda", "yehuda"},
}


def get_team_aliases(name):
    normalized = normalize_team_name(name)
    aliases = {normalized} if normalized else set()

    for canonical, known_aliases in TEAM_NAME_ALIASES.items():
        if normalized == canonical or normalized in known_aliases:
            return set(known_aliases) | {canonical}

    return aliases


def teams_match(left, right):
    left_aliases = get_team_aliases(left)
    right_aliases = get_team_aliases(right)
    return bool(left_aliases and right_aliases and left_aliases.intersection(right_aliases))


def team_search_text(team):
    parts = [team["name"], *team.get("aliases", [])]
    return " ".join(normalize_team_name(part) for part in parts)


def resolve_league_team(value):
    normalized = normalize_team_name(value)
    if not normalized:
        return None

    for team in LEAGUE_TEAMS:
        if normalized == normalize_team_name(team["name"]):
            return team
        if normalized in {normalize_team_name(alias) for alias in team.get("aliases", [])}:
            return team

    matches = [team for team in LEAGUE_TEAMS if normalized in team_search_text(team)]
    return matches[0] if len(matches) == 1 else None


async def team_autocomplete(interaction: discord.Interaction, current: str):
    normalized = normalize_team_name(current)
    current_tokens = normalized.split()

    scored = []
    for team in LEAGUE_TEAMS:
        search_text = team_search_text(team)
        if not current_tokens or all(token in search_text for token in current_tokens):
            score = 0 if search_text.startswith(normalized) else 1
            scored.append((score, team["name"], team))

    scored.sort(key=lambda item: (item[0], item[1]))
    return [
        app_commands.Choice(name=team["name"], value=team["name"])
        for _score, _name, team in scored[:25]
    ]


async def followed_game_autocomplete(interaction: discord.Interaction, current: str):
    normalized = normalize_team_name(current)
    choices = []

    for game_id, record in followed_games.items():
        label = followed_game_label(record)
        if normalized and normalized not in normalize_team_name(label):
            continue
        choices.append(app_commands.Choice(name=label[:100], value=str(game_id)))

    return choices[:25]


def format_follow_match_label(game):
    home = game.get("homeCompetitor", {}).get("name", "?")
    away = game.get("awayCompetitor", {}).get("name", "?")
    start_ts = start_to_unix(game.get("startTime", ""))
    if start_ts:
        date_text = datetime.fromtimestamp(start_ts, timezone.utc).astimezone().strftime("%d/%m %H:%M")
    else:
        date_text = "TBD"
    status = "LIVE" if is_live(game) else date_text
    return f"{home} vs {away} - {status}"


def game_search_text(game):
    home = game.get("homeCompetitor", {}).get("name", "")
    away = game.get("awayCompetitor", {}).get("name", "")
    competition = game.get("competitionDisplayName", "")
    parts = [home, away, competition]

    for team in LEAGUE_TEAMS:
        if team["id"] in {
            game.get("homeCompetitor", {}).get("id"),
            game.get("awayCompetitor", {}).get("id"),
        }:
            parts.extend(team.get("aliases", []))

    return " ".join(normalize_team_name(part) for part in parts)


async def get_follow_match_candidates(session, refresh=False):
    now_ts = int(datetime.now(timezone.utc).timestamp())
    if (
        not refresh
        and follow_match_cache["games"]
        and now_ts - follow_match_cache["fetched_at"] < 60
    ):
        return follow_match_cache["games"]

    games_by_key = {}
    for endpoint in ("current", "fixtures"):
        for game in await fetch_league_games(session, endpoint):
            if not (is_upcoming(game) or is_live(game)):
                continue
            games_by_key[get_game_key(game)] = game

    games = sorted(games_by_key.values(), key=game_sort_key)
    follow_match_cache["fetched_at"] = now_ts
    follow_match_cache["games"] = games
    return games


def filter_follow_matches(games, query):
    tokens = normalize_team_name(query).split()
    if not tokens:
        return games

    return [
        game
        for game in games
        if all(token in game_search_text(game) for token in tokens)
    ]


async def match_autocomplete(interaction: discord.Interaction, current: str):
    async with aiohttp.ClientSession() as session:
        games = await get_follow_match_candidates(session)

    matches = filter_follow_matches(games, current)
    return [
        app_commands.Choice(name=format_follow_match_label(game)[:100], value=str(game.get("id")))
        for game in matches[:25]
        if game.get("id") is not None
    ]


async def resolve_follow_match(session, query):
    games = await get_follow_match_candidates(session, refresh=True)

    if str(query).isdigit():
        detail = await get_365_game(session, int(query))
        if detail and (is_upcoming(detail) or is_live(detail)):
            return detail, []
        for game in games:
            if str(game.get("id")) == str(query):
                return game, []

    matches = filter_follow_matches(games, query)
    if len(matches) == 1:
        detail = await get_365_game(session, matches[0].get("id"))
        return detail or matches[0], []

    return None, matches[:5]


def is_match_between_team_ids(game, first_id, second_id):
    home_id = game.get("homeCompetitor", {}).get("id")
    away_id = game.get("awayCompetitor", {}).get("id")
    return {home_id, away_id} == {first_id, second_id}


async def find_followable_games(session, first_id, second_id):
    games = []
    seen = set()

    for endpoint in ("current", "fixtures"):
        for game in await fetch_365_games(session, endpoint, first_id):
            if not is_match_between_team_ids(game, first_id, second_id) or is_done(game):
                continue
            key = get_game_key(game)
            if key in seen:
                continue
            seen.add(key)
            games.append(game)

    return sorted(games, key=game_sort_key)


async def get_poll_games(session):
    games_by_key = {}

    for game in await fetch_watched_365_games(session, "current"):
        games_by_key[get_game_key(game)] = game

    for record in list(followed_games.values()):
        game = await get_365_game(session, record.get("id"))
        if game:
            games_by_key[get_game_key(game)] = game

    return sorted(games_by_key.values(), key=game_sort_key)


def html_to_lines(html):
    text = re.sub(r"(?is)<(script|style).*?</\1>", " ", html)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(?:a|p|div|li|tr|td|th|h[1-6])>", "\n", text)
    text = re.sub(r"(?i)<[^>]+>", " ", text)
    text = unescape(text)

    lines = []
    for line in text.splitlines():
        cleaned = re.sub(r"\s+", " ", line).strip()
        if cleaned:
            lines.append(cleaned)
    return lines


def html_fragment_to_text(fragment):
    return " ".join(html_to_lines(fragment))


def extract_1x2_prices(text):
    pairs = re.findall(
        r"(?i)(?:^|\s)(1|X|2)\s*(?:-|:)?\s*((?:\d+[.,]\d+)|(?:\d+/\d+))(?=\s|$)",
        text,
    )
    prices = {}
    for label, price in pairs:
        prices[label.upper()] = price.replace(",", ".")

    if all(key in prices for key in ("1", "X", "2")):
        return {"home": prices["1"], "draw": prices["X"], "away": prices["2"]}
    return None


def format_odd_value(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        if 1.01 <= float(value) <= 100:
            return f"{float(value):.2f}".rstrip("0").rstrip(".")
        return None

    cleaned = str(value).strip().replace(",", ".")
    if re.fullmatch(r"\d+(?:\.\d+)?", cleaned):
        if 1.01 <= float(cleaned) <= 100:
            return cleaned
    if re.fullmatch(r"\d+/\d+", cleaned):
        return cleaned
    return None


def parse_betexplorer_rows(html):
    for match in re.finditer(r"(?is)<tr\b[^>]*data-ttid=.*?</tr>", html):
        row_html = match.group(0)
        cells = re.findall(r"(?is)<td\b[^>]*>(.*?)</td>", row_html)
        if not cells:
            continue

        team_names = []
        for cell in cells:
            text = html_fragment_to_text(cell)
            if text and ("/football/team/" in cell or "<strong>" in cell):
                team_names.append(text)

        if len(team_names) < 2:
            continue

        detail_match = re.search(r'(?i)<a\s+href="([^"]+)">details</a>', row_html)
        odds_values = [format_odd_value(value) for value in re.findall(r'data-odd="([0-9]+(?:\.[0-9]+)?)"', row_html)]
        odds_values = [value for value in odds_values if value][:3]

        yield {
            "home_team": team_names[0],
            "away_team": team_names[1],
            "odds": odds_values if len(odds_values) == 3 else None,
            "detail_url": urljoin(BETEXPLORER_BASE_URL, detail_match.group(1)) if detail_match else None,
            "date_text": html_fragment_to_text(cells[-1]),
        }


def find_betexplorer_row_for_game(html, game):
    home_name = game.get("homeCompetitor", {}).get("name", "Home")
    away_name = game.get("awayCompetitor", {}).get("name", "Away")

    for row in parse_betexplorer_rows(html):
        same_order = teams_match(home_name, row["home_team"]) and teams_match(away_name, row["away_team"])
        same_swapped = teams_match(home_name, row["away_team"]) and teams_match(away_name, row["home_team"])
        if same_order or same_swapped:
            return row, same_order

    return None, False


def build_betexplorer_odds(game, values, same_order=True):
    home_odd, draw_odd, away_odd = values
    if not same_order:
        home_odd, away_odd = away_odd, home_odd

    return {
        "source": "BetExplorer",
        "home_team": game.get("homeCompetitor", {}).get("name", "Home"),
        "away_team": game.get("awayCompetitor", {}).get("name", "Away"),
        "home": home_odd,
        "draw": draw_odd,
        "away": away_odd,
    }


def get_betexplorer_event_id(detail_url):
    if not detail_url:
        return None
    return detail_url.rstrip("/").rsplit("/", 1)[-1]


def parse_betexplorer_match_odds(odds_html):
    average_values = [
        format_odd_value(value)
        for value in re.findall(r'oddsComparisonAll__average_text[^>]*data-odd="([0-9]+(?:\.[0-9]+)?)"', odds_html)
    ]
    average_values = [value for value in average_values if value]
    if len(average_values) >= 3:
        return average_values[:3]

    for row in re.findall(r"(?is)<tr\b[^>]*data-bid=.*?</tr>", odds_html):
        row_values = [
            format_odd_value(value)
            for value in re.findall(r'data-odd="([0-9]+(?:\.[0-9]+)?)"', row)
        ]
        row_values = [value for value in row_values if value]
        if len(row_values) >= 3:
            return row_values[:3]

    all_values = [
        format_odd_value(value)
        for value in re.findall(r'data-odd="([0-9]+(?:\.[0-9]+)?)"', odds_html)
    ]
    all_values = [value for value in all_values if value]
    return all_values[:3] if len(all_values) >= 3 else None


async def fetch_public_odds_page(session, url, *, referer=None, expect_json=False):
    headers = dict(ODDS_HEADERS)
    if referer:
        headers["Referer"] = referer
    if expect_json:
        headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
        headers["X-Requested-With"] = "XMLHttpRequest"

    try:
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=12),
        ) as response:
            if response.status != 200:
                print(f"[BetExplorer odds] {response.status} on {url}")
                return None
            if expect_json:
                return await response.json(content_type=None)
            return await response.text()
    except Exception as exc:
        print(f"[BetExplorer odds] {exc}")
        return None


async def fetch_betexplorer_detail_odds(session, detail_url):
    event_id = get_betexplorer_event_id(detail_url)
    if not event_id:
        return None

    odds_url = f"{BETEXPLORER_BASE_URL}/match-odds/{event_id}/1/1x2/odds/?lang=en"
    data = await fetch_public_odds_page(session, odds_url, referer=detail_url, expect_json=True)
    if not isinstance(data, dict):
        return None

    odds_html = data.get("odds")
    if not odds_html:
        return None
    return parse_betexplorer_match_odds(odds_html)


async def get_public_odds_for_game(game):
    pages = [BETEXPLORER_TEAM_RESULTS_URL] if is_done(game) else [BETEXPLORER_TEAM_FIXTURES_URL]
    pages.append(BETEXPLORER_TEAM_FIXTURES_URL if is_done(game) else BETEXPLORER_TEAM_RESULTS_URL)

    async with aiohttp.ClientSession() as session:
        for page_url in pages:
            html = await fetch_public_odds_page(session, page_url)
            if not html:
                continue

            row, same_order = find_betexplorer_row_for_game(html, game)
            if not row:
                continue

            if row.get("odds"):
                return build_betexplorer_odds(game, row["odds"], same_order)

            detail_odds = await fetch_betexplorer_detail_odds(session, row.get("detail_url"))
            if detail_odds:
                return build_betexplorer_odds(game, detail_odds, same_order)

    return None


def tag_team(name):
    return name


def build_live_embed(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_name = home.get("name", "?")
    away_name = away.get("name", "?")
    home_goals = fmt_score(home.get("score", "-"))
    away_goals = fmt_score(away.get("score", "-"))
    minute = game.get("gameTimeDisplay", "") or str(int(game.get("gameTime", 0) or 0))

    if is_live(game):
        minute_text = f" {minute}'" if minute and minute != "0" else ""
        home_label = format_scoreline_team(home_name, home.get("score"), away.get("score"))
        away_label = format_scoreline_team(away_name, away.get("score"), home.get("score"))
        title = f"🔴 LIVE{minute_text} · {home_label} {home_goals} - {away_goals} {away_label}"
        color = discord.Color.red()
    elif is_done(game):
        home_label = format_scoreline_team(home_name, home.get("score"), away.get("score"))
        away_label = format_scoreline_team(away_name, away.get("score"), home.get("score"))
        title = f"✅ FT · {home_label} {home_goals} - {away_goals} {away_label}"
        color = discord.Color.green()
    else:
        title = f"🗓️ {tag_team(home_name)} vs {tag_team(away_name)}"
        color = discord.Color.greyple()

    embed = discord.Embed(title=title, color=color)
    home_red = home.get("redCards", 0)
    away_red = away.get("redCards", 0)
    if home_red or away_red:
        embed.add_field(name="🟥 Red Cards", value=f"{home_name}: {home_red} | {away_name}: {away_red}", inline=True)

    league = game.get("competitionDisplayName", "Israeli Premier League")
    embed.set_footer(text=f"🇮🇱 Hapoel Be'er Sheva · {league}")
    embed.timestamp = datetime.now(timezone.utc)
    return embed


def get_goal_lines_for_competitor(goals, competitor_id):
    return [
        f"`{goal['minute']}` ⚽ {goal['label']}"
        for goal in goals
        if goal.get("competitor_id") == competitor_id
    ]


def add_goal_fields(embed, game, goals):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_id = home.get("id")
    away_id = away.get("id")
    home_name = home.get("name", "Home")
    away_name = away.get("name", "Away")

    home_lines = get_goal_lines_for_competitor(goals, home_id)
    away_lines = get_goal_lines_for_competitor(goals, away_id)
    known_ids = {home_id, away_id}
    other_lines = [
        f"`{goal['minute']}` ⚽ {goal['label']} ({goal['team']})"
        for goal in goals
        if goal.get("competitor_id") not in known_ids
    ]

    embed.add_field(
        name=f"Goals - {home_name}",
        value="\n".join(home_lines) if home_lines else "No goals",
        inline=True,
    )
    embed.add_field(
        name=f"Goals - {away_name}",
        value="\n".join(away_lines) if away_lines else "No goals",
        inline=True,
    )
    if other_lines:
        embed.add_field(name="Other Goals", value="\n".join(other_lines), inline=False)


def build_last_result_embed(game, goals, cards, odds=None):
    home_team = game.get("homeCompetitor", {}).get("name", "?")
    away_team = game.get("awayCompetitor", {}).get("name", "?")
    home_goals = fmt_score(game.get("homeCompetitor", {}).get("score", "-"))
    away_goals = fmt_score(game.get("awayCompetitor", {}).get("score", "-"))
    start_ts = start_to_unix(game.get("startTime", ""))
    stadium = get_stadium_name(game)

    home_score = game.get("homeCompetitor", {}).get("score")
    away_score = game.get("awayCompetitor", {}).get("score")
    home_label = format_scoreline_team(home_team, home_score, away_score)
    away_label = format_scoreline_team(away_team, away_score, home_score)
    title = f"FT | {home_label} {home_goals} - {away_goals} {away_label}"
    embed = discord.Embed(title=title, color=discord.Color.green())

    if start_ts:
        embed.add_field(name="Match Date", value=dts(start_ts), inline=False)
    if stadium:
        embed.add_field(name="Stadium", value=stadium, inline=False)

    ht_home, ht_away = get_stage_score(game, "HT")
    if ht_home is not None and ht_away is not None:
        embed.add_field(name="Half Time", value=f"{ht_home} - {ht_away}", inline=True)

    if goals:
        add_goal_fields(embed, game, goals)
    if cards:
        embed.add_field(name="🟥 Red Cards", value="\n".join(cards), inline=False)

    odds_text = format_odds(odds)
    if odds_text:
        source = odds.get("source", "Bookmaker")
        embed.add_field(name=f"{source} Odds (1X2)", value=odds_text, inline=False)

    league = game.get("competitionDisplayName", "Israeli Premier League")
    embed.set_footer(text=f"🇮🇱 Hapoel Be'er Sheva · {league} · 365Scores")
    return embed


def format_odds(odds):
    if not odds:
        return None

    lines = [
        f"{odds['home_team']}: `{odds['home']}`",
        f"Draw: `{odds['draw']}`" if odds.get("draw") is not None else None,
        f"{odds['away_team']}: `{odds['away']}`",
    ]
    return "\n".join(line for line in lines if line)


def build_next_embed(game, odds=None):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_name = home.get("name", "?")
    away_name = away.get("name", "?")
    start_ts = start_to_unix(game.get("startTime", ""))
    stadium = get_stadium_name(game)

    embed = discord.Embed(
        title=f"🗓️ {tag_team(home_name)} vs {tag_team(away_name)}",
        color=discord.Color.greyple(),
    )

    if start_ts:
        embed.add_field(name="Kick Off", value=f"{dts(start_ts)} · {dts(start_ts, 'R')}", inline=False)
    if stadium:
        embed.add_field(name="Stadium", value=stadium, inline=False)

    odds_text = format_odds(odds)
    if odds_text:
        source = odds.get("source", "Bookmaker")
        embed.add_field(name=f"{source} Odds (1X2)", value=odds_text, inline=False)

    league = game.get("competitionDisplayName", "Israeli Premier League")
    embed.set_footer(text=f"🇮🇱 Hapoel Be'er Sheva · {league}")
    return embed


def build_table_embed(standings):
    embed = discord.Embed(title="🇮🇱 Israeli Premier League - Table", color=discord.Color.blue())
    embed.timestamp = datetime.now(timezone.utc)

    rows = standings.get("rows", [])
    groups = standings.get("groups", [])
    group_names = {group.get("num"): group.get("name", f"Group {group.get('num')}") for group in groups}
    rows_by_group = {}

    for row in rows:
        rows_by_group.setdefault(row.get("groupNum", 0), []).append(row)

    for group_num, group_rows in rows_by_group.items():
        lines = []
        for row in group_rows:
            competitor = row.get("competitor", {})
            position = row.get("position", "?")
            team = competitor.get("name", "?")
            points = row.get("points", 0)
            played = row.get("gamePlayed", 0)
            goal_difference = row.get("ratio", 0)
            gd_text = f"+{goal_difference}" if isinstance(goal_difference, (int, float)) and goal_difference > 0 else str(goal_difference)
            lines.append(f"`{position:>2}.` **{team}** - **{points}** pts | {played} GP | GD {gd_text}")

        field_name = group_names.get(group_num, "Standings")
        embed.add_field(name=field_name, value="\n".join(lines), inline=False)

    embed.add_field(
        name="Position Guide",
        value=(
            "`1st` Champions League qualifying\n"
            "`2nd-3rd` Conference League qualifying\n"
            "`State Cup winner` Europa League qualifying\n"
            "`13th-14th` Relegated to Liga Leumit"
        ),
        inline=False,
    )
    embed.set_footer(text="Israeli Premier League | 365Scores")
    return embed


def get_stats_competitor_lookup(stats_data):
    return {
        competitor.get("id"): competitor.get("name", "?")
        for competitor in stats_data.get("competitors", [])
    }


def get_athlete_stat(stats_data, stat_name):
    athletes_stats = stats_data.get("stats", {}).get("athletesStats", [])
    for stat in athletes_stats:
        if stat.get("name", "").lower() == stat_name.lower():
            return stat
    return None


def get_primary_stat_value(row):
    stats = row.get("stats", [])
    if not stats:
        return "-"
    return stats[0].get("value", "-")


def build_top_players_embed(stats_data, stat_name, title, emoji, page=0, per_page=5):
    stat = get_athlete_stat(stats_data, stat_name)
    embed = discord.Embed(title=f"{emoji} {title}", color=discord.Color.blue())
    embed.timestamp = datetime.now(timezone.utc)

    if not stat:
        embed.description = "Stats unavailable right now."
        return embed

    competitor_lookup = get_stats_competitor_lookup(stats_data)
    rows = stat.get("rows", [])
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    page = min(max(page, 0), total_pages - 1)
    start = page * per_page
    page_rows = rows[start : start + per_page]

    for index, row in enumerate(page_rows, start=start + 1):
        player = row.get("entity", {})
        player_name = player.get("name") or player.get("shortName") or "Unknown"
        team = competitor_lookup.get(player.get("competitorId"), "?")
        value = get_primary_stat_value(row)
        embed.add_field(
            name=f"{index}. {player_name}",
            value=f"{team}\n{stat_name}: **{value}**",
            inline=False,
        )

    if not page_rows:
        embed.description = "No players found."

    embed.set_footer(text=f"Page {page + 1}/{total_pages} | Israeli Premier League | 365Scores")
    return embed


def format_fixture_line(game):
    home = game.get("homeCompetitor", {}).get("name", "?")
    away = game.get("awayCompetitor", {}).get("name", "?")
    start_ts = start_to_unix(game.get("startTime", ""))
    time_text = dts(start_ts, "f") if start_ts else "TBD"
    competition = game.get("competitionDisplayName", "Premier League")
    return f"{time_text}\n**{home}** vs **{away}**\n_{competition}_"


def format_result_line(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_name = format_scoreline_team(home.get("name", "?"), home.get("score"), away.get("score"))
    away_name = format_scoreline_team(away.get("name", "?"), away.get("score"), home.get("score"))
    home_score = fmt_score(home.get("score", "-"))
    away_score = fmt_score(away.get("score", "-"))
    start_ts = start_to_unix(game.get("startTime", ""))
    time_text = dts(start_ts, "d") if start_ts else "Date unavailable"
    competition = game.get("competitionDisplayName", "Premier League")
    return f"{time_text}\n{home_name} **{home_score}** - **{away_score}** {away_name}\n_{competition}_"


def get_beer_sheva_result_marker(game):
    home = game.get("homeCompetitor", {})
    away = game.get("awayCompetitor", {})
    home_score = score_as_int(home.get("score"), None)
    away_score = score_as_int(away.get("score"), None)

    if home_score is None or away_score is None or home_score == away_score:
        return "🟰"

    beer_sheva_is_home = home.get("id") == BEER_SHEVA_365
    beer_sheva_won = (beer_sheva_is_home and home_score > away_score) or (
        not beer_sheva_is_home and away_score > home_score
    )
    return "🟢" if beer_sheva_won else "🔴"


class LeagueGamesView(discord.ui.View):
    def __init__(self, games, title, mode, user_id, per_page=5):
        super().__init__(timeout=180)
        self.games = games
        self.title = title
        self.mode = mode
        self.user_id = user_id
        self.per_page = per_page
        self.page = 0
        self.update_buttons()

    @property
    def total_pages(self):
        return max(1, (len(self.games) + self.per_page - 1) // self.per_page)

    async def interaction_check(self, interaction):
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message("Only the person who opened this can change pages.", ephemeral=True)
        return False

    def page_games(self):
        start = self.page * self.per_page
        return self.games[start : start + self.per_page]

    def build_embed(self):
        embed = discord.Embed(title=self.title, color=discord.Color.blue())
        embed.timestamp = datetime.now(timezone.utc)

        formatter = format_fixture_line if self.mode == "fixtures" else format_result_line
        for index, game in enumerate(self.page_games(), start=self.page * self.per_page + 1):
            marker = get_beer_sheva_result_marker(game) if self.mode == "results" else ""
            prefix = f"{marker} " if marker else ""
            embed.add_field(name=f"{prefix}Game {index}", value=formatter(game), inline=False)

        embed.set_footer(text=f"Page {self.page + 1}/{self.total_pages} | Israeli Premier League | 365Scores")
        return embed

    def update_buttons(self):
        for item in self.children:
            if getattr(item, "custom_id", "") == "league_games_previous":
                item.disabled = self.page <= 0
            elif getattr(item, "custom_id", "") == "league_games_next":
                item.disabled = self.page >= self.total_pages - 1

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="league_games_previous")
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, custom_id="league_games_next")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.total_pages - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


class LeagueStatsView(discord.ui.View):
    def __init__(self, stats_data, stat_name, title, emoji, user_id, per_page=5):
        super().__init__(timeout=180)
        self.stats_data = stats_data
        self.stat_name = stat_name
        self.title = title
        self.emoji = emoji
        self.user_id = user_id
        self.per_page = per_page
        self.page = 0
        stat = get_athlete_stat(stats_data, stat_name)
        self.rows = stat.get("rows", []) if stat else []
        self.update_buttons()

    @property
    def total_pages(self):
        return max(1, (len(self.rows) + self.per_page - 1) // self.per_page)

    async def interaction_check(self, interaction):
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message("Only the person who opened this can change pages.", ephemeral=True)
        return False

    def build_embed(self):
        return build_top_players_embed(
            self.stats_data,
            self.stat_name,
            self.title,
            self.emoji,
            page=self.page,
            per_page=self.per_page,
        )

    def update_buttons(self):
        for item in self.children:
            if getattr(item, "custom_id", "") == "league_stats_previous":
                item.disabled = self.page <= 0
            elif getattr(item, "custom_id", "") == "league_stats_next":
                item.disabled = self.page >= self.total_pages - 1

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary, custom_id="league_stats_previous")
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, custom_id="league_stats_next")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.total_pages - 1, self.page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


async def process_polled_game(channel, game):
    game_key = get_game_key(game)
    live_state = get_channel_live_state(game, channel)
    goals_now = get_total_goals(game)
    red_cards_now = get_total_red_cards(game)
    goal_events = get_live_events_by_type(game, "Goal")
    red_card_events = get_live_red_card_events(game)
    minute = get_game_minute(game)

    if not live_state["initialized"]:
        live_state["initialized"] = True

        if is_upcoming(game):
            live_state["last_goals_count"] = 0
            live_state["last_red_cards_count"] = 0
        elif is_live(game) and (minute is None or minute <= 10):
            live_state["last_goals_count"] = 0
            live_state["last_red_cards_count"] = 0
        else:
            live_state["last_goals_count"] = goals_now
            live_state["last_red_cards_count"] = red_cards_now
            live_state["notified_goal_keys"] = {live_event_key(event) for event in goal_events}
            live_state["notified_red_card_keys"] = {live_event_key(event) for event in red_card_events}
            if is_live(game):
                live_state["notified_match_state_keys"].add(f"{game_key}:started")
                if minute is not None and minute > 55:
                    live_state["notified_match_state_keys"].add(f"{game_key}:second_half")
            if is_done(game):
                live_state["notified_match_state_keys"].add(f"{game_key}:full_time")

    if is_upcoming(game):
        start_ts = start_to_unix(game.get("startTime", ""))
        now_ts = int(datetime.now(timezone.utc).timestamp())
        if start_ts and 0 < start_ts - now_ts <= 15 * 60:
            await send_match_state_once(channel, game, "kickoff_soon", live_state)
        live_state["live_message_id"] = None
        return

    if is_live(game):
        if (
            minute is None
            or minute <= 10
            or f"{game_key}:kickoff_soon" in live_state["notified_match_state_keys"]
        ):
            await send_match_state_once(channel, game, "started", live_state)

        if is_half_time_status(game):
            await send_match_state_once(channel, game, "half_time", live_state)
        elif is_second_half_status(game):
            await send_match_state_once(channel, game, "second_half", live_state)

        score_increased = (
            live_state["last_goals_count"] >= 0
            and goals_now > live_state["last_goals_count"]
        )
        goal_delta = goals_now - live_state["last_goals_count"] if score_increased else 0
        new_goal_events = []
        for event in goal_events:
            event_key = live_event_key(event)
            if event_key not in live_state["notified_goal_keys"]:
                new_goal_events.append((event_key, event))

        if score_increased:
            for event_key, event in new_goal_events:
                await send_live_notification(channel, format_live_goal_notification(game, event))
                live_state["notified_goal_keys"].add(event_key)

            missing_goal_notifications = max(0, goal_delta - len(new_goal_events))
            for _ in range(missing_goal_notifications):
                await send_live_notification(channel, format_generic_goal_notification(game))
        else:
            for event_key, _event in new_goal_events:
                live_state["notified_goal_keys"].add(event_key)

        red_cards_increased = (
            live_state["last_red_cards_count"] >= 0
            and red_cards_now > live_state["last_red_cards_count"]
        )
        red_card_delta = red_cards_now - live_state["last_red_cards_count"] if red_cards_increased else 0
        new_red_card_events = []
        for event in red_card_events:
            event_key = live_event_key(event)
            if event_key not in live_state["notified_red_card_keys"]:
                new_red_card_events.append((event_key, event))

        if red_cards_increased:
            for event_key, event in new_red_card_events:
                await send_live_notification(channel, format_live_red_card_notification(game, event))
                live_state["notified_red_card_keys"].add(event_key)

            missing_red_card_notifications = max(0, red_card_delta - len(new_red_card_events))
            for _ in range(missing_red_card_notifications):
                await send_live_notification(channel, format_generic_red_card_notification(game))
        else:
            for event_key, _event in new_red_card_events:
                live_state["notified_red_card_keys"].add(event_key)

        live_state["last_goals_count"] = goals_now
        live_state["last_red_cards_count"] = red_cards_now
        embed = build_live_embed(game)

        if live_state["live_message_id"]:
            try:
                message = await channel.fetch_message(live_state["live_message_id"])
                await message.edit(embed=embed)
            except discord.NotFound:
                live_state["live_message_id"] = None

        if not live_state["live_message_id"]:
            message = await channel.send(embed=embed)
            live_state["live_message_id"] = message.id

        return

    if is_done(game):
        await send_match_state_once(channel, game, "full_time", live_state)
        live_state["live_message_id"] = None
        live_state["last_goals_count"] = goals_now
        live_state["last_red_cards_count"] = red_cards_now

        if game_key in followed_games:
            followed_games.pop(game_key, None)
            remove_live_states_for_game(game_key)
            save_followed_games()


@tasks.loop(seconds=30)
async def poll_live():
    channels = await get_scores_channels()
    if not channels:
        return

    async with aiohttp.ClientSession() as session:
        games = await get_poll_games(session)

    for game in games:
        for channel in channels:
            await process_polled_game(channel, game)


def build_help_embed():
    embed = discord.Embed(
        title="LiveScoreHBS Help",
        description=(
            "Hapoel Beer Sheva scores, fixtures, results, odds, league info, "
            "and live Discord notifications."
        ),
        color=discord.Color.blue(),
    )
    embed.timestamp = datetime.now(timezone.utc)

    embed.add_field(
        name="First Setup",
        value=(
            "`/permissions` - Get the bot invite link with every permission it needs.\n"
            "`/setup` - Admin command. Use the current text channel for live notifications.\n"
            "Needed permissions: View Channel, Send Messages, Embed Links, "
            "Read Message History, Manage Roles."
        ),
        inline=False,
    )
    embed.add_field(
        name="Live Alerts",
        value=(
            "`/subscribe` - Get the `Live Notifications (LiveScoreHBS)` role and be mentioned on alerts.\n"
            "`/unsubscribe` - Remove yourself from live alert mentions.\n"
            "Alerts include goals, red cards, kickoff in 15 minutes, kickoff, "
            "half time, second half start, and full time. The live poll runs every 30 seconds."
        ),
        inline=False,
    )
    embed.add_field(
        name="Match Commands",
        value=(
            "`/score` - Current live score for watched matches.\n"
            "`/next` - Next Hapoel Beer Sheva fixture, with odds when available.\n"
            "`/last` - Last Hapoel Beer Sheva result with goals, red cards, and odds.\n"
            "`/follow` - Search for an upcoming/live match and follow it for notifications.\n"
            "`/following` - Show matches currently followed.\n"
            "`/unfollow` - Stop following a selected match."
        ),
        inline=False,
    )
    embed.add_field(
        name="League Commands",
        value=(
            "`/table` - Israeli Premier League table, including Europe/relegation notes.\n"
            "`/top_scorers` - League scorers, 5 per page.\n"
            "`/top_assists` - League assist leaders, 5 per page.\n"
            "`/fixtures` - Upcoming Hapoel Beer Sheva fixtures, 5 per page.\n"
            "`/results` - Recent Hapoel Beer Sheva results, 5 per page."
        ),
        inline=False,
    )
    embed.add_field(
        name="Useful Notes",
        value=(
            "For `/follow`, start typing part of a team or matchup, like `beit` or `haifa`, "
            "then pick the game from autocomplete.\n"
            "`/permissions` gives an invite/re-authorization link with the permissions the bot needs. "
            "Use it when adding the bot to a server or when Discord says it is missing permissions; "
            "then run `/setup` in the channel where live notifications should be sent.\n"
            "For `/subscribe` to work, the bot needs `Manage Roles`. In Server Settings > Roles, "
            "drag the bot's role above `Live Notifications (LiveScoreHBS)`. Discord only lets bots "
            "create, edit, or give roles that are below their own highest role.\n"
            "`/help` posts publicly only once per channel every 24 hours; "
            "extra uses are shown privately."
        ),
        inline=False,
    )
    embed.set_footer(text="Made by dolev the goat himself")
    return embed


@bot.tree.command(name="help", description="Show all bot commands and setup info")
async def cmd_help(interaction: discord.Interaction):
    public_help = should_send_help_publicly(interaction)
    await interaction.response.send_message(embed=build_help_embed(), ephemeral=not public_help)


@bot.tree.command(name="score", description="Be'er Sheva's live score")
async def cmd_score(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        games = await get_poll_games(session)

    live_games = [game for game in games if is_live(game)]
    if not live_games:
        await interaction.followup.send("🟡 No watched match is live right now.")
        return

    await interaction.followup.send(embed=build_live_embed(live_games[0]))


@bot.tree.command(name="setup", description="Use this channel for live notifications")
@app_commands.default_permissions(manage_guild=True)
async def cmd_setup(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this inside a server text channel.", ephemeral=True)
        return

    missing_permissions = missing_bot_channel_permissions(interaction.channel)
    if missing_permissions:
        await interaction.response.send_message(
            "I need these permissions in this channel first: "
            + ", ".join(missing_permissions),
            ephemeral=True,
        )
        return

    server_channels[str(interaction.guild.id)] = interaction.channel.id
    save_server_channels()
    await interaction.response.send_message(
        f"✅ Live notifications will now be sent in {interaction.channel.mention}.",
        ephemeral=True,
    )


@bot.tree.command(name="subscribe", description="Get mentioned on live match notifications")
async def cmd_subscribe(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Run this inside a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    member = get_interaction_member(interaction)
    if not member:
        await interaction.followup.send("I could not find your server member profile.")
        return

    role, error = await ensure_subscriber_role(interaction.guild)
    if error:
        await interaction.followup.send(error)
        return

    if role in member.roles:
        await interaction.followup.send(f"You're already subscribed to `{role.name}`.")
        return

    try:
        await member.add_roles(role, reason="Subscribed to live match notifications")
    except discord.Forbidden:
        await interaction.followup.send(
            f"I could not give you `{role.name}`. My Discord role must be above it in Server Settings > Roles."
        )
        return
    except discord.HTTPException as exc:
        await interaction.followup.send(f"Discord would not let me add `{role.name}`: {exc}")
        return

    await interaction.followup.send(
        f"✅ Subscribed to `{role.name}`. You'll be mentioned on live goals, cards, and match updates."
    )


@bot.tree.command(name="unsubscribe", description="Stop live match notification mentions")
async def cmd_unsubscribe(interaction: discord.Interaction):
    if not interaction.guild:
        await interaction.response.send_message("Run this inside a server.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)
    member = get_interaction_member(interaction)
    if not member:
        await interaction.followup.send("I could not find your server member profile.")
        return

    role = get_subscriber_role(interaction.guild)
    if not role:
        await interaction.followup.send("There is no live notification role in this server yet.")
        return

    if role not in member.roles:
        await interaction.followup.send(f"You're not subscribed to `{role.name}`.")
        return

    if not bot_can_manage_subscriber_role(interaction.guild, role):
        await interaction.followup.send(
            f"I can see `{role.name}`, but my Discord role must be above it in Server Settings > Roles."
        )
        return

    try:
        await member.remove_roles(role, reason="Unsubscribed from live match notifications")
    except discord.Forbidden:
        await interaction.followup.send(
            f"I could not remove `{role.name}`. My Discord role must be above it in Server Settings > Roles."
        )
        return
    except discord.HTTPException as exc:
        await interaction.followup.send(f"Discord would not let me remove `{role.name}`: {exc}")
        return

    await interaction.followup.send(f"✅ Unsubscribed from `{role.name}`.")


@bot.tree.command(name="permissions", description="Get the bot invite link with required permissions")
async def cmd_permissions(interaction: discord.Interaction):
    guild_id = interaction.guild.id if interaction.guild else None
    invite_url = get_bot_invite_url(guild_id)
    if not invite_url:
        await interaction.response.send_message(
            "I could not build the invite link yet. Add DISCORD_CLIENT_ID to .env or try again after startup.",
            ephemeral=True,
        )
        return

    client_id_note = (
        "\n\nNote: `DISCORD_CLIENT_ID` in `.env` is not a valid numeric client ID, "
        "so I used the logged-in bot ID for this link. You can fix `.env` later by "
        "putting the Application ID from the Discord Developer Portal there."
        if DISCORD_CLIENT_ID and not re.fullmatch(r"\d{15,25}", DISCORD_CLIENT_ID)
        else ""
    )
    server_note = (
        "\n\nThe link preselects this server."
        if interaction.guild
        else ""
    )
    await interaction.response.send_message(
        "Use this link to add the bot with the required permissions:\n"
        f"{invite_url}\n\n"
        "`Manage Roles` is included so `/subscribe` can create and assign the live notification role."
        f"{server_note}"
        f"{client_id_note}",
        ephemeral=True,
    )


@bot.tree.command(name="next", description="Be'er Sheva's next fixture")
async def cmd_next(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        games = await fetch_365_games(session, "current")
        upcoming_games = sorted(
            [game for game in games if is_beer_sheva_365(game) and is_upcoming(game)],
            key=game_sort_key,
        )

        if not upcoming_games:
            await interaction.followup.send("No upcoming fixture found.")
            return

        game = await get_365_game(session, upcoming_games[0].get("id")) or upcoming_games[0]

    odds = await get_public_odds_for_game(game)
    await interaction.followup.send(embed=build_next_embed(game, odds))


@bot.tree.command(name="last", description="Be'er Sheva's last result with goalscorers")
async def cmd_last(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        game = await get_last_365_game(session)
        if not game:
            await interaction.followup.send("No recent result found.")
            return
        goals, cards = get_game_goals_and_red_cards(game)

    odds = await get_public_odds_for_game(game)
    await interaction.followup.send(embed=build_last_result_embed(game, goals, cards, odds))


@bot.tree.command(name="table", description="Israeli Premier League table")
async def cmd_table(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        standings = await get_standings(session)

    if not standings:
        await interaction.followup.send("❌ Standings unavailable right now.")
        return

    await interaction.followup.send(embed=build_table_embed(standings))


@bot.tree.command(name="top_scorers", description="Israeli Premier League scorers")
async def cmd_top_scorers(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        stats_data = await get_league_stats(session)

    if not stats_data:
        await interaction.followup.send("❌ Top scorers unavailable right now.")
        return

    view = LeagueStatsView(stats_data, "Goals", "Top Scorers", "⚽", interaction.user.id)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="top_assists", description="Israeli Premier League assists")
async def cmd_top_assists(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        stats_data = await get_league_stats(session)

    if not stats_data:
        await interaction.followup.send("❌ Top assists unavailable right now.")
        return

    view = LeagueStatsView(stats_data, "Assists", "Top Assists", "🎯", interaction.user.id)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="fixtures", description="Israeli Premier League fixtures")
async def cmd_fixtures(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        games = [
            game
            for game in await fetch_365_games(session, "fixtures")
            if is_beer_sheva_365(game) and is_upcoming(game)
        ]

    if not games:
        await interaction.followup.send("No upcoming Be'er Sheva fixtures found.")
        return

    view = LeagueGamesView(games, "🗓️ Hapoel Be'er Sheva Fixtures", "fixtures", interaction.user.id)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="results", description="Israeli Premier League results")
async def cmd_results(interaction: discord.Interaction):
    await interaction.response.defer()
    async with aiohttp.ClientSession() as session:
        games = [
            game
            for game in await fetch_365_games(session, "results")
            if is_beer_sheva_365(game) and is_done(game)
        ]

    if not games:
        await interaction.followup.send("No recent Be'er Sheva results found.")
        return

    view = LeagueGamesView(games, "✅ Hapoel Be'er Sheva Results", "results", interaction.user.id)
    await interaction.followup.send(embed=view.build_embed(), view=view)


@bot.tree.command(name="follow", description="Follow a match for live notifications")
@app_commands.describe(match="Start typing a team or matchup")
@app_commands.autocomplete(match=match_autocomplete)
async def cmd_follow(interaction: discord.Interaction, match: str):
    await interaction.response.defer()

    async with aiohttp.ClientSession() as session:
        game, matches = await resolve_follow_match(session, match)

    if not game:
        if matches:
            lines = [f"- {format_follow_match_label(candidate)}" for candidate in matches]
            await interaction.followup.send(
                "I found more than one match. Pick the exact match from autocomplete:\n"
                + "\n".join(lines)
            )
            return

        await interaction.followup.send(
            "No upcoming or live match found. Try typing part of a team name, like `beit` or `haifa`."
        )
        return

    game_key = get_game_key(game)
    followed_games[game_key] = followed_game_record(game)
    save_followed_games()

    embed = build_live_embed(game) if is_live(game) else build_next_embed(game)
    await interaction.followup.send(f"✅ Following {get_match_name(game)}.", embed=embed)


@bot.tree.command(name="following", description="Show matches currently followed")
async def cmd_following(interaction: discord.Interaction):
    await interaction.response.defer()

    if not followed_games:
        await interaction.followup.send("No followed matches right now.")
        return

    embed = discord.Embed(title="🔔 Followed Matches", color=discord.Color.blue())
    embed.timestamp = datetime.now(timezone.utc)
    for index, record in enumerate(followed_games.values(), start=1):
        start_ts = start_to_unix(record.get("startTime", ""))
        start_text = dts(start_ts, "F") if start_ts else "Kickoff TBD"
        competition = record.get("competitionDisplayName", "Premier League")
        embed.add_field(
            name=f"{index}. {record.get('home_name', '?')} vs {record.get('away_name', '?')}",
            value=f"{start_text}\n_{competition}_",
            inline=False,
        )

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="unfollow", description="Stop following a match")
@app_commands.describe(game="Start typing a followed match")
@app_commands.autocomplete(game=followed_game_autocomplete)
async def cmd_unfollow(interaction: discord.Interaction, game: str):
    await interaction.response.defer()

    record = followed_games.pop(str(game), None)
    if not record:
        await interaction.followup.send("I couldn't find that followed match.")
        return

    remove_live_states_for_game(str(game))
    save_followed_games()
    await interaction.followup.send(f"✅ Unfollowed {followed_game_label(record, discord_timestamp=True)}.")


@bot.event
async def on_ready():
    load_followed_games()
    load_server_channels()
    load_subscriber_roles()
    load_help_public_posts()
    print(f"✅ Logged in as {bot.user}")
    await sync_subscriber_roles()
    await sync_bot_description()
    invite_url = get_bot_invite_url()
    if invite_url:
        print(f"✅ Invite URL with required permissions: {invite_url}")
    synced = await bot.tree.sync()
    print(f"✅ Synced commands: {[command.name for command in synced]}")
    if not poll_live.is_running():
        poll_live.start()
        print("✅ Live polling active (30s)")
    else:
        print("✅ Live polling already active")


bot.run(DISCORD_TOKEN)
