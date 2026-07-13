# Peter Assistant Gateway

Nová architektúra po vzore OpenClaw: jeden dlhodobo bežiaci Gateway proces, samostatné kanály a deterministické workflow.

## Čo už funguje
- Email inbound/outbound cez existujúci MCP server.
- Telegram outbound/inbound polling po zapnutí.
- WhatsApp outbound cez Meta Cloud API; inbound sa doplní webhookom.
- Jednotná Message schéma, dedup a SQLite sessions.
- Deterministická klasifikácia approval/acceptance udalostí.
- Základ sales workflow a learning queue.
- Camera watchdog: detekcia osoby + korelácia s autorizovanou prístupovou udalosťou.
- Windows Task Scheduler auto-start.

## Dôležité k watchdogu
Watchdog nerobí rozpoznávanie tváre. Stav „povolený“ vzniká iba vtedy, keď bola v definovanom časovom okne zaznamenaná autorizovaná udalosť z prístupového systému, napr. karta, čip alebo vrátnica. Bez tejto udalosti hlási „neoverený vstup“.

Príklad `data/access_events.jsonl`:
```json
{"timestamp":"2026-07-10T12:00:00+00:00","camera_id":"gate-1","authorized":true,"person":"Employee 123","source":"badge"}
```

## Inštalácia
```powershell
cd C:\AI\salesperson\peter_assistant
py -3.13 -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt
copy .env.example .env
```
Nastav `config/peter.yaml` a `.env`.

## Ručné spustenie
```powershell
.\.venv\Scripts\python app.py
```

## Automatický štart Windows
Spusti PowerShell ako administrátor:
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\install_windows_task.ps1 -PythonExe "C:\AI\salesperson\peter_assistant\.venv\Scripts\python.exe" -ProjectDir "C:\AI\salesperson\peter_assistant"
```
Task Scheduler spustí Gateway po štarte PC aj pri prihlásení, reštartuje ho pri páde a zabráni duplicitným inštanciám.

## Ďalšie fázy migrácie
1. Presun plného OfferRepository a approval flow z legacy agenta.
2. Service registry + JSON cenníky.
3. Samostatné generátory ponúk pre každú službu.
4. Contract workflow.
5. RAG a kontrolované samoučenie.
6. WhatsApp webhook a Teams channel.

## OpenClaw porovnanie
OpenClaw používa jeden Gateway pre kanály, sessions, tools a skills. Na Windows používa Scheduled Task a pri odmietnutí vytvorenia tasku môže použiť Startup fallback. Tento projekt používa rovnaký princíp: jedna dlhodobo bežiaca Gateway aplikácia, ktorú Windows spravuje ako plánovanú úlohu.
