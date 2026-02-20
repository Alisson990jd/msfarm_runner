#!/usr/bin/env python3
import discord
from discord.ext import commands, tasks
import asyncio, aiohttp, json, os, base64, logging, signal, sys
from datetime import datetime, timedelta

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID") or "0")
GITHUB_TOKEN = os.environ.get("GH_API_TOKEN")
GITHUB_REPO = "Alisson990jd/apiss"
GITHUB_BRANCH = "main"
GITHUB_PATH = "msfarm"

if not DISCORD_TOKEN: print("❌ DISCORD_TOKEN!"); sys.exit(1)
if not CHANNEL_ID: print("❌ DISCORD_CHANNEL_ID!"); sys.exit(1)
if not GITHUB_TOKEN: print("❌ GH_API_TOKEN!"); sys.exit(1)

MAX_CONCURRENT = 4
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EMAILS_FILE = os.path.join(SCRIPT_DIR, "emails.txt")
USAGE_FILE = os.path.join(SCRIPT_DIR, "emails_usage.json")
YT_SCRIPT = os.path.join(SCRIPT_DIR, "yt.py")

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler('cerebro.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

class GitHubManager:
    def __init__(s, token, repo, branch, path):
        s.token, s.repo, s.branch, s.path = token, repo, branch, path
        s.base_url = f"https://api.github.com/repos/{repo}/contents/{path}"
        s.headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    async def get_file(s, filename):
        url = f"{s.base_url}/{filename}?ref={s.branch}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=s.headers) as r:
                if r.status == 200:
                    data = await r.json()
                    return base64.b64decode(data['content']).decode('utf-8'), data['sha']
                return None, None
    
    async def update_file(s, filename, content, sha=None, msg="Update"):
        url = f"{s.base_url}/{filename}"
        if sha is None: _, sha = await s.get_file(filename)
        payload = {"message": msg, "content": base64.b64encode(content.encode()).decode(), "branch": s.branch}
        if sha: payload["sha"] = sha
        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=s.headers, json=payload) as r:
                if r.status in [200, 201]: return (await r.json())['content']['sha']
                raise Exception(f"Erro: {r.status}")

class EmailManager:
    def __init__(s, gh):
        s.gh, s.emails, s.usage, s.emails_sha, s.usage_sha, s.lock = gh, [], {}, None, None, asyncio.Lock()
    
    async def sync_from_github(s):
        async with s.lock:
            try:
                c, sha = await s.gh.get_file("emails.txt")
                if c: s.emails, s.emails_sha = [e.strip() for e in c.split('\n') if e.strip()], sha
                c, sha = await s.gh.get_file("emails_usage.json")
                if c: s.usage, s.usage_sha = json.loads(c), sha
                logger.info(f"Sincronizado: {len(s.emails)} emails")
                return True
            except Exception as e:
                logger.error(f"Erro sync: {e}")
                return False
    
    async def sync_to_github(s, upd_emails=True, upd_usage=True):
        async with s.lock:
            try:
                if upd_emails: s.emails_sha = await s.gh.update_file("emails.txt", '\n'.join(s.emails), s.emails_sha)
                if upd_usage: s.usage_sha = await s.gh.update_file("emails_usage.json", json.dumps(s.usage, indent=2), s.usage_sha)
                return True
            except Exception as e:
                logger.error(f"Erro sync GH: {e}")
                return False
    
    async def add_email(s, email):
        async with s.lock:
            if email not in s.emails: s.emails.append(email); return True
            return False
    
    async def remove_email(s, email):
        async with s.lock:
            if email in s.emails: s.emails.remove(email); return True
            return False
    
    async def mark_executed(s, email, success=True, err=None):
        async with s.lock:
            today = datetime.now().strftime('%Y-%m-%d')
            if email not in s.usage: s.usage[email] = {"executions": [], "total_success": 0, "total_failed": 0}
            s.usage[email]["executions"].append({"date": today, "time": datetime.now().strftime('%H:%M:%S'), "success": success, "error": err})
            s.usage[email]["total_success" if success else "total_failed"] += 1
            s.usage[email]["executions"] = s.usage[email]["executions"][-30:]
    
    def get_pending_today(s):
        today = datetime.now().strftime('%Y-%m-%d')
        return [e for e in s.emails if not any(x.get("date")==today and x.get("success") for x in s.usage.get(e,{}).get("executions",[]))]

class ExecutionManager:
    def __init__(s, em, bot):
        s.em, s.bot, s.sem, s.running, s.active, s.dc, s.dl = em, bot, asyncio.Semaphore(MAX_CONCURRENT), False, {}, 0, asyncio.Lock()
    
    async def get_display(s):
        async with s.dl: s.dc += 1; return s.dc + 99
    
    async def execute(s, email, disp):
        start = datetime.now()
        try:
            await s.notify(f"🔄 **Iniciando**\n📧 `{email}`\n🖥️ :{disp}")
            cmd = f"export DISPLAY=:{disp} && Xvfb :{disp} -screen 0 1920x1080x24 >/dev/null 2>&1 & sleep 2 && cd {SCRIPT_DIR} && python3 {YT_SCRIPT} -email {email}"
            proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await proc.communicate()
            dur = f"{int((datetime.now()-start).total_seconds()//60)}m"
            if proc.returncode == 0:
                await s.em.mark_executed(email, True); await s.em.sync_to_github(False, True)
                await s.notify(f"✅ **OK**\n📧 `{email}`\n⏱️ {dur}")
                return True
            else:
                err = stderr.decode()[-300:] if stderr else "Erro"
                await s.em.mark_executed(email, False, err); await s.em.sync_to_github(False, True)
                await s.notify(f"❌ **Falha**\n📧 `{email}`\n```{err[:200]}```")
                return False
        except Exception as e:
            await s.em.mark_executed(email, False, str(e))
            await s.notify(f"❌ **Erro**\n📧 `{email}`\n```{str(e)[:200]}```")
            return False
    
    async def worker(s, email):
        async with s.sem:
            d = await s.get_display()
            try: await s.execute(email, d)
            finally: s.active.pop(email, None)
    
    async def run_batch(s):
        await s.em.sync_from_github()
        pending = s.em.get_pending_today()
        if not pending: await s.notify("ℹ️ **Nenhum pendente!**"); return
        await s.notify(f"🚀 **Batch**\n📊 {len(pending)} emails")
        s.running, s.dc = True, 0
        tasks = [asyncio.create_task(s.worker(e)) for e in pending]
        for e, t in zip(pending, tasks): s.active[e] = t
        await asyncio.gather(*tasks, return_exceptions=True)
        s.running = False
        ok = sum(1 for e in pending if s.em.usage.get(e,{}).get("executions",[{}])[-1].get("success"))
        await s.notify(f"🏁 **Fim!**\n✅ {ok}/{len(pending)}")
    
    async def notify(s, msg):
        try:
            ch = s.bot.get_channel(CHANNEL_ID)
            if ch: await ch.send(msg)
        except: pass

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
gh = em = ex = None

@bot.event
async def on_ready():
    global gh, em, ex
    logger.info(f"Bot: {bot.user}")
    gh = GitHubManager(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_PATH)
    em = EmailManager(gh)
    ex = ExecutionManager(em, bot)
    await em.sync_from_github()
    ch = bot.get_channel(CHANNEL_ID)
    if ch:
        e = discord.Embed(title="🧠 Bot Online!", color=0x00ff00, timestamp=datetime.now())
        e.add_field(name="📧", value=str(len(em.emails)))
        await ch.send(embed=e)
    daily.start()

@bot.command(name='add')
async def c_add(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID or '@' not in email: return
    if await em.add_email(email): await em.sync_to_github(True, False); await ctx.send(f"✅ `{email}`")
    else: await ctx.send("⚠️ Existe")

@bot.command(name='remove')
async def c_rm(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID: return
    if await em.remove_email(email): await em.sync_to_github(True, False); await ctx.send(f"✅ Removido")
    else: await ctx.send("⚠️ Não existe")

@bot.command(name='list')
async def c_ls(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    if not em.emails: await ctx.send("📭"); return
    await ctx.send(embed=discord.Embed(title="📧", description="\n".join(f"• `{e}`" for e in em.emails)[:4000], color=0x0099ff))

@bot.command(name='status')
async def c_st(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    e = discord.Embed(title="📊", color=0x9900ff)
    e.add_field(name="Total", value=str(len(em.emails)))
    e.add_field(name="Pend", value=str(len(em.get_pending_today())))
    e.add_field(name="Ativo", value=str(len(ex.active)))
    await ctx.send(embed=e)

@bot.command(name='pending')
async def c_pend(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    p = em.get_pending_today()
    if not p: await ctx.send("✅ OK")
    else: await ctx.send(embed=discord.Embed(title="⏳", description="\n".join(f"• `{e}`" for e in p[:50])[:4000], color=0xff9900))

@bot.command(name='run')
async def c_run(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    if ex.running: await ctx.send("⚠️ Rodando"); return
    await ctx.send("🚀"); asyncio.create_task(ex.run_batch())

@bot.command(name='sync')
async def c_sync(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    await ctx.send("🔄")
    if await em.sync_from_github(): await ctx.send(f"✅ {len(em.emails)}")
    else: await ctx.send("⚠️")

@bot.command(name='stop')
async def c_stop(ctx):
    if ctx.channel.id != CHANNEL_ID: return
    for t in ex.active.values(): t.cancel()
    ex.running = False
    await ctx.send("🛑")

@tasks.loop(hours=24)
async def daily():
    await asyncio.sleep(5)
    now = datetime.now()
    target = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now > target: target += timedelta(days=1)
    await asyncio.sleep((target - now).total_seconds())
    ch = bot.get_channel(CHANNEL_ID)
    if ch: await ch.send("⏰ **Auto**")
    await ex.run_batch()

@daily.before_loop
async def bd(): await bot.wait_until_ready()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s,f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s,f: sys.exit(0))
    logger.info(f"Init... CH: {CHANNEL_ID}")
    bot.run(DISCORD_TOKEN)
