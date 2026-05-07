# Local Scheduler

Локально через `macOS launchd` теперь работает только фоновая обработка команд Telegram-бота.

Ежедневная отправка выпуска в `08:00 Europe/London` теперь считается обязанностью Codex `cron automation`, а не локального `launchd` и не thread heartbeat.

Runtime-бандл теперь живёт вне `Documents`, в безопасном каталоге:

```bash
~/.mnewsdigest
```

## Что создано

- `scripts/process_bot_updates.sh`
- `scripts/sync_runtime_bundle.sh`
- `ops/launchd/com.mnewsdigest.bot-updates.plist`
- `scripts/install_launchd_job.sh`
- `scripts/uninstall_launchd_job.sh`

## Что делает локальный scheduler

Раз в минуту запускается:

```bash
~/.mnewsdigest/scripts/process_bot_updates.sh
```

Он обрабатывает команды бота:

- `/start`
- `/latest`
- `/subscribe`
- `/unsubscribe`
- `/help`

## Что делает Codex cron automation

Codex automation запускается как отдельная `cron`-задача:

1. собирает свежий выпуск
2. обновляет `data/outgoing/current_digest.html`
3. синхронизирует runtime-бандл через `scripts/sync_runtime_bundle.sh`
4. отправляет выпуск в Telegram

Время запуска: `08:00 Europe/London`.

## Установка

```bash
bash /Users/aaverin/Documents/News\ project/scripts/install_launchd_job.sh
```

## Удаление

```bash
bash /Users/aaverin/Documents/News\ project/scripts/uninstall_launchd_job.sh
```

## Логи

- `~/.mnewsdigest/data/state/bot-updates.stdout.log`
- `~/.mnewsdigest/data/state/bot-updates.stderr.log`

## Важно

Чтобы `launchd` не упирался в macOS permissions на `Documents`, install-скрипт копирует нужный runtime-бандл в `~/.mnewsdigest`.

После каждого обновления выпуска в рабочем проекте нужно синхронизировать runtime-бандл:

```bash
bash /Users/aaverin/Documents/News\ project/scripts/sync_runtime_bundle.sh
```
