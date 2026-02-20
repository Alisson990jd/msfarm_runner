#!/usr/bin/env python3
"""
Cérebro Central - Versão GitHub Actions
"""

import discord
from discord.ext import commands, tasks
import asyncio
import aiohttp
import json
import os
import base64
from datetime import datetime, timedelta
from collections import deque
import logging
import signal
import sys

# ==================== CONFIGURAÇÕES ====================

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID") or "0")
GITHUB_TOKEN = os.environ.get("GH_API_TOKEN")
GITHUB_REPO = "Alisson990jd/apiss"
GITHUB_BRANCH = "main"
GITHUB_PATH = "msfarm"

if not DISCORD_TOKEN:
    print("❌ DISCORD_TOKEN não definido!")
    sys.exit(1)
if not CHANNEL_ID:
    print("❌ DISCORD_CHANNEL_ID não definido!")
    sys.exit(1)
if not GITHUB_TOKEN:
    print("❌ GH_API_TOKEN não definido!")
    sys.exit(1)

MAX_CONCURRENT = 4
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EMAILS_FILE = os.path.join(SCRIPT_DIR, "emails.txt")
USAGE_FILE = os.path.join(SCRIPT_DIR, "emails_usage.json")
YT_SCRIPT = os.path.join(SCRIPT_DIR, "yt.py")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler('cerebro.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class GitHubManager:
    def __init__(self, token, repo, branch, path):
        self.token = token
        self.repo = repo
        self.branch = branch
        self.path = path
        self.base_url = f"https://api.github.com/repos/{repo}/contents/{path}"
        self.headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    async def get_file(self, filename):
        url = f"{self.base_url}/{filename}?ref={self.branch}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    content = base64.b64decode(data['content']).decode('utf-8')
                    return content, data['sha']
                return None, None
    
    async def update_file(self, filename, content, sha=None, message="Update"):
        url = f"{self.base_url}/{filename}"
        if sha is None:
            _, sha = await self.get_file(filename)
        payload = {"message": message, "content": base64.b64encode(content.encode()).decode(), "branch": self.branch}
        if sha:
            payload["sha"] = sha
        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=self.headers, json=payload) as response:
                if response.status in [200, 201]:
                    return (await response.json())['content']['sha']
                raise Exception(f"Erro: {response.status}")


class EmailManager:
    def __init__(self, github):
        self.github = github
        self.emails = []
        self.usage = {}
        self.emails_sha = None
        self.usage_sha = None
        self.lock = asyncio.Lock()
    
    async def sync_from_github(self):
        async with self.lock:
            try:
                content, sha = await self.github.get_file("emails.txt")
                if content:
                    self.emails = [e.strip() for e in content.split('\n') if e.strip()]
                    self.emails_sha = sha
                content, sha = await self.github.get_file("emails_usage.json")
                if content:
                    self.usage = json.loads(content)
                    self.usage_sha = sha
                logger.info(f"Sincronizado: {len(self.emails)} emails")
                return True
            except Exception as e:
                logger.error(f"Erro sync: {e}")
                return False
    
    async def sync_to_github(self, update_emails=True, update_usage=True):
        async with self.lock:
            try:
                if update_emails:
                    self.emails_sha = await self.github.update_file("emails.txt", '\n'.join(self.emails), self.emails_sha)
                if update_usage:
                    self.usage_sha = await self.github.update_file("emails_usage.json", json.dumps(self.usage, indent=2), self.usage_sha)
                return True
            except Exception as e:
                logger.error(f"Erro sync GitHub: {e}")
                return False
    
    async def add_email(self, email):
        async with self.lock:
            if email not in self.emails:
                self.emails.append(email)
                return True
            return False
    
    async def remove_email(self, email):
        async with self.lock:
            if email in self.emails:
                self.emails.remove(email)
                return True
            return False
    
    async def mark_executed(self, email, success=True, error_msg=None):
        async with self.lock:
            today = datetime.now().strftime('%Y-%m-%d')
            if email not in self.usage:
                self.usage[email] = {"executions": [], "total_success": 0, "total_failed": 0}
            self.usage[email]["executions"].append({
                "date": today, "time": datetime.now().strftime('%H:%M:%S'), "success": success, "error": error_msg
            })
            if success:
                self.usage[email]["total_success"] += 1
            else:
                self.usage[email]["total_failed"] += 1
            self.usage[email]["executions"] = self.usage[email]["executions"][-30:]
    
    def get_pending_today(self):
        today = datetime.now().strftime('%Y-%m-%d')
        return [e for e in self.emails if not any(
            ex.get("date") == today and ex.get("success") for ex in self.usage.get(e, {}).get("executions", [])
        )]


class ExecutionManager:
    def __init__(self, email_manager, bot):
        self.email_manager = email_manager
        self.bot = bot
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.running = False
        self.active_tasks = {}
        self.display_counter = 0
        self.display_lock = asyncio.Lock()
    
    async def get_next_display(self):
        async with self.display_lock:
            self.display_counter += 1
            return self.display_counter + 99
    
    async def execute_email(self, email, display_num):
        start_time = datetime.now()
        try:
            await self.send_notification(f"🔄 **Iniciando**\n📧 `{email}`\n🖥️ :{display_num}")
            cmd = f"export DISPLAY=:{display_num} && Xvfb :{display_num} -screen 0 1920x1080x24 & sleep 2 && cd {SCRIPT_DIR} && python3 {YT_SCRIPT} -email {email}"
            process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()
            duration = f"{int((datetime.now() - start_time).total_seconds() // 60)}m"
            
            if process.returncode == 0:
                await self.email_manager.mark_executed(email, True)
                await self.email_manager.sync_to_github(False, True)
                await self.send_notification(f"✅ **Sucesso**\n📧 `{email}`\n⏱️ {duration}")
                return True
            else:
                error = stderr.decode()[-300:] if stderr else "Erro"
                await self.email_manager.mark_executed(email, False, error)
                await self.email_manager.sync_to_github(False, True)
                await self.send_notification(f"❌ **Falhou**\n📧 `{email}`\n```{error[:200]}```")
                return False
        except Exception as e:
            await self.email_manager.mark_executed(email, False, str(e))
            await self.send_notification(f"❌ **Erro**\n📧 `{email}`\n```{str(e)[:200]}```")
            return False
    
    async def worker(self, email):
        async with self.semaphore:
            display = await self.get_next_display()
            try:
                await self.execute_email(email, display)
            finally:
                self.active_tasks.pop(email, None)
    
    async def run_daily_batch(self):
        await self.email_manager.sync_from_github()
        pending = self.email_manager.get_pending_today()
        if not pending:
            await self.send_notification("ℹ️ **Nenhum email pendente!**")
            return
        await self.send_notification(f"🚀 **Batch iniciado**\n📊 {len(pending)} emails")
        self.running = True
        self.display_counter = 0
        tasks = [asyncio.create_task(self.worker(e)) for e in pending]
        for e, t in zip(pending, tasks):
            self.active_tasks[e] = t
        await asyncio.gather(*tasks, return_exceptions=True)
        self.running = False
        success = sum(1 for e in pending if self.email_manager.usage.get(e, {}).get("executions", [{}])[-1].get("success"))
        await self.send_notification(f"🏁 **Concluído!**\n✅ {success}/{len(pending)}")
    
    async def send_notification(self, msg):
        try:
            ch = self.bot.get_channel(CHANNEL_ID)
            if ch:
                await ch.send(msg)
        except Exception as e:
            logger.error(f"Notif erro: {e}")


intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
github_manager = email_manager = execution_manager = None


@bot.event
async def on_ready():
    global github_manager, email_manager, execution_manager
    logger.info(f"Bot: {bot.user}")
    github_manager = GitHubManager(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_PATH)
    email_manager = EmailManager(github_manager)
    execution_manager = ExecutionManager(email_manager, bot)
    await email_manager.sync_from_github()
    ch = bot.get_channel(CHANNEL_ID)
    if ch:
        embed = discord.Embed(title="🧠 Bot Online!", color=0x00ff00, timestamp=datetime.now())
        embed.add_field(name="📧 Emails", value=str(len(email_manager.emails)))
        await ch.send(embed=embed)
    daily_task.start()


@bot.command(name='add')
async def cmd_add(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID or '@' not in email:
        return
    if await email_manager.add_email(email):
        await email_manager.sync_to_github(True, False)
        await ctx.send(f"✅ Adicionado: `{email}`")
    else:
        await ctx.send(f"⚠️ Já existe")


@bot.command(name='remove')
async def cmd_remove(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID:
        return
    if await email_manager.remove_email(email):
        await email_manager.sync_to_github(True, False)
        await ctx.send(f"✅ Removido: `{email}`")
    else:
        await ctx.send(f"⚠️ Não encontrado")


@bot.command(name='list')
async def cmd_list(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    if not email_manager.emails:
        await ctx.send("📭 Vazio")
        return
    embed = discord.Embed(title="📧 Emails", description="\n".join(f"• `{e}`" for e in email_manager.emails)[:4000], color=0x0099ff)
    await ctx.send(embed=embed)


@bot.command(name='status')
async def cmd_status(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    pending = email_manager.get_pending_today()
    embed = discord.Embed(title="📊 Status", color=0x9900ff)
    embed.add_field(name="Total", value=str(len(email_manager.emails)))
    embed.add_field(name="Pendentes", value=str(len(pending)))
    embed.add_field(name="Ativos", value=str(len(execution_manager.active_tasks)))
    await ctx.send(embed=embed)


@bot.command(name='pending')
async def cmd_pending(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    pending = email_manager.get_pending_today()
    if not pending:
        await ctx.send("✅ Todos executados!")
    else:
        embed = discord.Embed(title="⏳ Pendentes", description="\n".join(f"• `{e}`" for e in pending[:50])[:4000], color=0xff9900)
        await ctx.send(embed=embed)


@bot.command(name='run')
async def cmd_run(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    if execution_manager.running:
        await ctx.send("⚠️ Já executando!")
        return
    await ctx.send("🚀 Iniciando...")
    asyncio.create_task(execution_manager.run_daily_batch())


@bot.command(name='sync')
async def cmd_sync(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    await ctx.send("🔄 Sincronizando...")
    if await email_manager.sync_from_github():
        await ctx.send(f"✅ {len(email_manager.emails)} emails")
    else:
        await ctx.send("⚠️ Erro")


@bot.command(name='stop')
async def cmd_stop(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    for t in execution_manager.active_tasks.values():
        t.cancel()
    execution_manager.running = False
    await ctx.send("🛑 Parado")


@tasks.loop(hours=24)
async def daily_task():
    await asyncio.sleep(5)
    now = datetime.now()
    target = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now > target:
        target += timedelta(days=1)
    await asyncio.sleep((target - now).total_seconds())
    ch = bot.get_channel(CHANNEL_ID)
    if ch:
        await ch.send("⏰ **Execução diária...**")
    await execution_manager.run_daily_batch()


@daily_task.before_loop
async def before_daily():
    await bot.wait_until_ready()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    logger.info(f"Iniciando... Canal: {CHANNEL_ID}")
    bot.run(DISCORD_TOKEN)
