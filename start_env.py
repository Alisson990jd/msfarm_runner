#!/usr/bin/env python3
"""
Cérebro Central - Bot Discord para automação de execução de emails
Versão para GitHub Actions (usa variáveis de ambiente)
"""

import discord
from discord.ext import commands, tasks
import asyncio
import aiohttp
import json
import os
import subprocess
import base64
from datetime import datetime, timedelta
from collections import deque
import logging
import signal
import sys

# ==================== CONFIGURAÇÕES VIA VARIÁVEIS DE AMBIENTE ====================

DISCORD_TOKEN = os.environ.get("DISCORD_TOKEN")
CHANNEL_ID = int(os.environ.get("DISCORD_CHANNEL_ID", "0"))
GITHUB_TOKEN = os.environ.get("GH_API_TOKEN")
GITHUB_REPO = "Alisson990jd/apiss"
GITHUB_BRANCH = "main"
GITHUB_PATH = "msfarm"

# Validar variáveis obrigatórias
if not DISCORD_TOKEN:
    print("❌ ERRO: DISCORD_TOKEN não definido!")
    sys.exit(1)
if not CHANNEL_ID:
    print("❌ ERRO: DISCORD_CHANNEL_ID não definido!")
    sys.exit(1)
if not GITHUB_TOKEN:
    print("❌ ERRO: GH_API_TOKEN não definido!")
    sys.exit(1)

MAX_CONCURRENT = 4
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EMAILS_FILE = os.path.join(SCRIPT_DIR, "emails.txt")
USAGE_FILE = os.path.join(SCRIPT_DIR, "emails_usage.json")
YT_SCRIPT = os.path.join(SCRIPT_DIR, "yt.py")

# ==================== LOGGING ====================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('cerebro.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== CLASSES ====================

class GitHubManager:
    def __init__(self, token, repo, branch, path):
        self.token = token
        self.repo = repo
        self.branch = branch
        self.path = path
        self.base_url = f"https://api.github.com/repos/{repo}/contents/{path}"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
    
    async def get_file(self, filename):
        url = f"{self.base_url}/{filename}?ref={self.branch}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    content = base64.b64decode(data['content']).decode('utf-8')
                    return content, data['sha']
                elif response.status == 404:
                    return None, None
                else:
                    raise Exception(f"Erro ao baixar {filename}: {response.status}")
    
    async def update_file(self, filename, content, sha=None, message="Update via bot"):
        url = f"{self.base_url}/{filename}"
        if sha is None:
            _, sha = await self.get_file(filename)
        
        payload = {
            "message": message,
            "content": base64.b64encode(content.encode('utf-8')).decode('utf-8'),
            "branch": self.branch
        }
        if sha:
            payload["sha"] = sha
        
        async with aiohttp.ClientSession() as session:
            async with session.put(url, headers=self.headers, json=payload) as response:
                if response.status in [200, 201]:
                    data = await response.json()
                    return data['content']['sha']
                else:
                    error = await response.text()
                    raise Exception(f"Erro ao atualizar {filename}: {response.status} - {error}")


class EmailManager:
    def __init__(self, github_manager):
        self.github = github_manager
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
                    self.emails = [e.strip() for e in content.strip().split('\n') if e.strip()]
                    self.emails_sha = sha
                    with open(EMAILS_FILE, 'w') as f:
                        f.write('\n'.join(self.emails))
                else:
                    self.emails = []
                
                content, sha = await self.github.get_file("emails_usage.json")
                if content:
                    self.usage = json.loads(content)
                    self.usage_sha = sha
                    with open(USAGE_FILE, 'w') as f:
                        json.dump(self.usage, f, indent=2)
                else:
                    self.usage = {}
                
                logger.info(f"Sincronizado: {len(self.emails)} emails")
                return True
            except Exception as e:
                logger.error(f"Erro ao sincronizar: {e}")
                if os.path.exists(EMAILS_FILE):
                    with open(EMAILS_FILE, 'r') as f:
                        self.emails = [e.strip() for e in f.read().strip().split('\n') if e.strip()]
                if os.path.exists(USAGE_FILE):
                    with open(USAGE_FILE, 'r') as f:
                        self.usage = json.load(f)
                return False
    
    async def sync_to_github(self, update_emails=True, update_usage=True):
        async with self.lock:
            try:
                if update_emails:
                    content = '\n'.join(self.emails)
                    self.emails_sha = await self.github.update_file(
                        "emails.txt", content, self.emails_sha,
                        f"Update emails.txt - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    with open(EMAILS_FILE, 'w') as f:
                        f.write(content)
                
                if update_usage:
                    content = json.dumps(self.usage, indent=2)
                    self.usage_sha = await self.github.update_file(
                        "emails_usage.json", content, self.usage_sha,
                        f"Update usage - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    with open(USAGE_FILE, 'w') as f:
                        f.write(content)
                
                logger.info("Sincronizado para GitHub")
                return True
            except Exception as e:
                logger.error(f"Erro ao sincronizar para GitHub: {e}")
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
                "date": today,
                "time": datetime.now().strftime('%H:%M:%S'),
                "success": success,
                "error": error_msg
            })
            
            if success:
                self.usage[email]["total_success"] += 1
            else:
                self.usage[email]["total_failed"] += 1
            
            self.usage[email]["executions"] = self.usage[email]["executions"][-30:]
    
    def get_pending_today(self):
        today = datetime.now().strftime('%Y-%m-%d')
        pending = []
        for email in self.emails:
            if email not in self.usage:
                pending.append(email)
                continue
            executed_today = any(
                ex.get("date") == today and ex.get("success")
                for ex in self.usage[email].get("executions", [])
            )
            if not executed_today:
                pending.append(email)
        return pending


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
            
            cmd = f"""
            export DISPLAY=:{display_num}
            Xvfb :{display_num} -screen 0 1920x1080x24 &
            XVFB_PID=$!
            sleep 2
            cd {SCRIPT_DIR}
            python3 {YT_SCRIPT} -email {email}
            EXIT_CODE=$?
            kill $XVFB_PID 2>/dev/null
            exit $EXIT_CODE
            """
            
            process = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=SCRIPT_DIR
            )
            stdout, stderr = await process.communicate()
            
            duration = (datetime.now() - start_time).total_seconds()
            duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
            
            if process.returncode == 0:
                await self.email_manager.mark_executed(email, success=True)
                await self.email_manager.sync_to_github(update_emails=False, update_usage=True)
                await self.send_notification(f"✅ **Sucesso**\n📧 `{email}`\n⏱️ {duration_str}")
                return True
            else:
                error_msg = stderr.decode('utf-8')[-500:] if stderr else "Erro"
                await self.email_manager.mark_executed(email, success=False, error_msg=error_msg)
                await self.email_manager.sync_to_github(update_emails=False, update_usage=True)
                await self.send_notification(f"❌ **Falhou**\n📧 `{email}`\n⏱️ {duration_str}\n```{error_msg[:200]}```")
                return False
        except Exception as e:
            await self.email_manager.mark_executed(email, success=False, error_msg=str(e))
            await self.send_notification(f"❌ **Erro**\n📧 `{email}`\n```{str(e)[:200]}```")
            return False
    
    async def worker(self, email):
        async with self.semaphore:
            display_num = await self.get_next_display()
            try:
                await self.execute_email(email, display_num)
            finally:
                self.active_tasks.pop(email, None)
    
    async def run_daily_batch(self):
        await self.email_manager.sync_from_github()
        pending = self.email_manager.get_pending_today()
        
        if not pending:
            await self.send_notification("ℹ️ **Nenhum email pendente!**")
            return
        
        await self.send_notification(f"🚀 **Batch iniciado**\n📊 {len(pending)} emails\n⚡ {MAX_CONCURRENT} paralelos")
        
        self.running = True
        self.display_counter = 0
        
        tasks = []
        for email in pending:
            task = asyncio.create_task(self.worker(email))
            self.active_tasks[email] = task
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        self.running = False
        
        successful = sum(
            1 for e in pending
            if self.email_manager.usage.get(e, {}).get("executions", [{}])[-1].get("success", False)
        )
        
        await self.send_notification(f"🏁 **Concluído!**\n✅ {successful}\n❌ {len(pending) - successful}")
    
    async def send_notification(self, message):
        try:
            channel = self.bot.get_channel(CHANNEL_ID)
            if channel:
                await channel.send(message)
        except Exception as e:
            logger.error(f"Erro notificação: {e}")


# ==================== BOT ====================

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

github_manager = None
email_manager = None
execution_manager = None


@bot.event
async def on_ready():
    global github_manager, email_manager, execution_manager
    logger.info(f"Bot conectado: {bot.user}")
    
    github_manager = GitHubManager(GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_PATH)
    email_manager = EmailManager(github_manager)
    execution_manager = ExecutionManager(email_manager, bot)
    
    await email_manager.sync_from_github()
    
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        embed = discord.Embed(title="🧠 Bot Online!", color=discord.Color.green(), timestamp=datetime.now())
        embed.add_field(name="📧 Emails", value=str(len(email_manager.emails)), inline=True)
        embed.add_field(name="⚡ Paralelos", value=str(MAX_CONCURRENT), inline=True)
        await channel.send(embed=embed)
    
    daily_task.start()


@bot.command(name='add')
async def add_email(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID:
        return
    if '@' not in email:
        await ctx.send("❌ Email inválido!")
        return
    if await email_manager.add_email(email):
        await email_manager.sync_to_github(update_emails=True, update_usage=False)
        await ctx.send(f"✅ Adicionado: `{email}`")
    else:
        await ctx.send(f"⚠️ Já existe: `{email}`")


@bot.command(name='remove')
async def remove_email(ctx, email: str):
    if ctx.channel.id != CHANNEL_ID:
        return
    if await email_manager.remove_email(email):
        await email_manager.sync_to_github(update_emails=True, update_usage=False)
        await ctx.send(f"✅ Removido: `{email}`")
    else:
        await ctx.send(f"⚠️ Não encontrado: `{email}`")


@bot.command(name='list')
async def list_emails(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    if not email_manager.emails:
        await ctx.send("📭 Nenhum email.")
        return
    emails_text = "\n".join([f"• `{e}`" for e in email_manager.emails])
    embed = discord.Embed(title="📧 Emails", description=emails_text[:4000], color=discord.Color.blue())
    embed.set_footer(text=f"Total: {len(email_manager.emails)}")
    await ctx.send(embed=embed)


@bot.command(name='status')
async def status(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    pending = email_manager.get_pending_today()
    embed = discord.Embed(title="📊 Status", color=discord.Color.purple(), timestamp=datetime.now())
    embed.add_field(name="📧 Total", value=str(len(email_manager.emails)), inline=True)
    embed.add_field(name="⏳ Pendentes", value=str(len(pending)), inline=True)
    embed.add_field(name="🔄 Ativos", value=str(len(execution_manager.active_tasks)), inline=True)
    await ctx.send(embed=embed)


@bot.command(name='pending')
async def pending(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    pending = email_manager.get_pending_today()
    if not pending:
        await ctx.send("✅ Todos executados!")
        return
    emails_text = "\n".join([f"• `{e}`" for e in pending[:50]])
    embed = discord.Embed(title="⏳ Pendentes", description=emails_text[:4000], color=discord.Color.orange())
    await ctx.send(embed=embed)


@bot.command(name='run')
async def run_manual(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    if execution_manager.running:
        await ctx.send("⚠️ Já em execução!")
        return
    await ctx.send("🚀 Iniciando...")
    asyncio.create_task(execution_manager.run_daily_batch())


@bot.command(name='sync')
async def sync(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    await ctx.send("🔄 Sincronizando...")
    if await email_manager.sync_from_github():
        await ctx.send(f"✅ {len(email_manager.emails)} emails carregados.")
    else:
        await ctx.send("⚠️ Erro, usando dados locais.")


@bot.command(name='stop')
async def stop(ctx):
    if ctx.channel.id != CHANNEL_ID:
        return
    if not execution_manager.active_tasks:
        await ctx.send("ℹ️ Nenhuma execução ativa.")
        return
    for task in execution_manager.active_tasks.values():
        task.cancel()
    execution_manager.running = False
    await ctx.send(f"🛑 Canceladas.")


@tasks.loop(hours=24)
async def daily_task():
    await asyncio.sleep(5)
    now = datetime.now()
    target = now.replace(hour=7, minute=0, second=0, microsecond=0)
    if now > target:
        target += timedelta(days=1)
    wait_seconds = (target - now).total_seconds()
    logger.info(f"Próxima execução em {wait_seconds/3600:.2f}h")
    await asyncio.sleep(wait_seconds)
    
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        await channel.send("⏰ **Execução diária automática...**")
    await execution_manager.run_daily_batch()


@daily_task.before_loop
async def before_daily_task():
    await bot.wait_until_ready()


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        pass
    else:
        await ctx.send(f"❌ Erro: {str(error)[:200]}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    
    logger.info("Iniciando Cérebro Central...")
    logger.info(f"Canal: {CHANNEL_ID}")
    
    try:
        bot.run(DISCORD_TOKEN)
    except Exception as e:
        logger.error(f"Erro fatal: {e}")
        sys.exit(1)
