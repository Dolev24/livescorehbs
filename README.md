# 🇮🇱 Israeli Premier League Discord Bot

A Discord bot that tracks **Ligat ha'Al** (Israeli Premier League) live scores, standings, and fixtures using [API-Football](https://www.api-football.com/) via RapidAPI.

---

## Features

| Command | Description |
|---|---|
| `/scores` | Today's match scores (live or finished) |
| `/standings` | Current league table |
| `/next` | Next 5 upcoming fixtures |
| Auto-posts | Live match updates every 60 seconds to your scores channel |
| Auto-posts | Today's fixtures every morning at 07:00 UTC |

---

## Setup (Step by Step)

### 1. Get a Discord Bot Token

1. Go to [discord.com/developers/applications](https://discord.com/developers/applications)
2. Click **New Application** → give it a name
3. Go to **Bot** → click **Add Bot**
4. Under **Token**, click **Reset Token** and copy it
5. Scroll down and enable **Message Content Intent**
6. Go to **OAuth2 → URL Generator**:
   - Scopes: `bot`, `applications.commands`
   - Bot Permissions: `Send Messages`, `Read Message History`, `Embed Links`
7. Open the generated URL and invite the bot to your server

### 2. Get a RapidAPI Key (Free)

1. Sign up at [rapidapi.com](https://rapidapi.com)
2. Search for **API-Football** and subscribe to the **Basic (free)** plan
3. Copy your API key from the dashboard
4. Free tier = **100 requests/day** (enough for light use)

### 3. Get Your Channel ID

1. In Discord, go to **Settings → Advanced → Enable Developer Mode**
2. Right-click the channel you want scores posted in
3. Click **Copy Channel ID**

### 4. Configure the Bot

```bash
cp .env.example .env
```

Edit `.env` and fill in your three values:

```
DISCORD_TOKEN=...
RAPIDAPI_KEY=...
SCORES_CHANNEL_ID=...
```

### 5. Install & Run

```bash
pip install -r requirements.txt
python bot.py
```

You should see:
```
✅ Logged in as YourBot#1234
✅ Synced 3 slash commands
✅ Background tasks started
```

---

## API Usage Notes

- The bot polls for live scores **every 60 seconds**
- Today's fixtures are auto-posted at **07:00 UTC**
- The free plan gives **100 requests/day** — the bot uses ~2-3 per live match polling cycle
- During a typical match day with 3-4 games: ~150-200 requests → consider upgrading to the $10/mo plan for daily use

## Hosting for Free / Cheap

| Platform | Cost | Notes |
|---|---|---|
| [Railway](https://railway.app) | ~$5/mo | Easiest, 1-click deploy |
| [Fly.io](https://fly.io) | Free tier | Slightly more setup |
| [Raspberry Pi](https://www.raspberrypi.com) | One-time | Run at home 24/7 |
| VPS (Hetzner/DigitalOcean) | ~$4/mo | Full control |

---

## League Info

- **League ID**: `271`
- **Season format**: `2024` (for 2024/25 season)
- **Teams**: 14 clubs compete each season
- **Season**: August → May
