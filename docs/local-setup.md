# Local Setup

Локальный MVP сейчас делает 7 вещей:

- проверяет Telegram bot token
- показывает `getUpdates`, чтобы найти chat id
- обрабатывает входящие команды Telegram
- позволяет подписывать и отписывать чаты
- показывает последний готовый дайджест по команде `/latest`
- рендерит тестовый выпуск
- отправляет готовый выпуск в Telegram всем получателям

## 1. Положить секреты в `.env.local`

Создайте файл `.env.local` в корне проекта:

```env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_TARGET=your_chat_id_or_channel_username
```

Важно:

- `TELEGRAM_BOT_TOKEN` не хранить в коде
- `TELEGRAM_TARGET` для private chat обычно должен быть `numeric chat id`
- username бота вида `@MNewsDigestbot` не является target для отправки сообщений самому пользователю

## 2. Узнать chat id

1. Откройте Telegram и напишите боту `/start`
2. Запустите:

```bash
python3 scripts/run_local_digest.py get-updates
```

3. В ответе найдите объект `chat` и его `id`
4. Положите этот `id` в `TELEGRAM_TARGET`

## 3. Проверить токен

```bash
python3 scripts/run_local_digest.py bot-info
```

## 4. Обработать команды `/start`, `/help`, `/latest`, `/subscribe`, `/unsubscribe`

```bash
python3 scripts/run_local_digest.py process-updates
```

Что делает команда:

- забирает новые Telegram updates после последнего обработанного `update_id`
- отвечает пользователям на команды
- сохраняет список подписчиков в `data/state/bot_state.json`

Если кто-то уже нажал `/start`, а бот молчал, эта команда разберёт накопившиеся апдейты и ответит задним числом.

## 5. Держать бота в живом режиме локально

```bash
python3 scripts/run_local_digest.py poll-updates --interval-seconds 15
```

Пока эта команда работает, бот регулярно забирает новые сообщения из Telegram и отвечает на них автоматически.

## 6. Посмотреть тестовый выпуск

```bash
python3 scripts/run_local_digest.py render-demo
```

## 7. Отправить тестовый выпуск

```bash
python3 scripts/run_local_digest.py send-demo
```

## 8. Отправить готовый выпуск всем получателям

```bash
python3 scripts/run_local_digest.py send-file data/outgoing/current_digest.html --parse-mode HTML
```

Кто получит выпуск:

- `TELEGRAM_TARGET`, если он задан
- все чаты, которые нажали `/subscribe`

После отправки:

- markdown-архив выпуска появится в `data/archive/`
- состояние запусков появится в `data/state/run_state.json`
- состояние подписчиков и offset бота появится в `data/state/bot_state.json`
