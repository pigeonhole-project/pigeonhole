# tg3 — S3 поверх Telegram Bot API

S3-совместимый HTTP gateway на Rust. Объекты режутся на чанки по 19 MiB и хранятся как документы в приватном Telegram-чате/канале. Метаданные — в локальном SQLite.

## Ограничения Bot API

- upload ≤ 50 MiB на файл
- download через `getFile` ≤ 20 MiB → размер чанка **19 MiB**

## Подготовка

1. Создай бота у [@BotFather](https://t.me/BotFather), получи `BOT_TOKEN`.
2. Создай приватный канал (или группу), добавь бота админом с правом писать сообщения.
3. Узнай `CHAT_ID` (для каналов обычно `-100...`).
4. Скопируй env:

```bash
cp .env.example .env
# отредактируй BOT_TOKEN и CHAT_ID
```

## Запуск

```bash
cargo run --release
```

Сервер слушает `http://0.0.0.0:8333`.

## Smoke test (aws cli)

```bash
export AWS_ACCESS_KEY_ID=tg3
export AWS_SECRET_ACCESS_KEY=tg3secret
export AWS_DEFAULT_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./README.md s3://demo/readme.md
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/
aws --endpoint-url http://127.0.0.1:8333 s3 cp s3://demo/readme.md ./out.md
aws --endpoint-url http://127.0.0.1:8333 s3 rm s3://demo/readme.md
```

Для отладки без подписи: `TG3_INSECURE=1`.

## Snapshot индекса

Экспорт метаданных в Telegram (JSON-документ в тот же чат):

```bash
curl -X POST 'http://127.0.0.1:8333/?tg3-snapshot=export' \
  -H "Authorization: dummy" \
  # либо с TG3_INSECURE=1
```

Восстановление из `file_id` или сырого JSON:

```bash
curl -X POST 'http://127.0.0.1:8333/?tg3-snapshot=import' \
  -H 'Content-Type: application/json' \
  -d '{"file_id":"BQACAg..."}'
```

## MVP API

| Операция | Статус |
|---|---|
| CreateBucket / ListBuckets / DeleteBucket | есть |
| PutObject / GetObject / HeadObject / DeleteObject | есть |
| ListObjectsV2 | есть |
| Multipart / Copy / Range / Presign | позже |

## Заметки

- Это эксперимент, не production object storage.
- `file_id` может инвалидироваться; держи snapshot индекса.
- Telegram ToS: не строй на этом публичный SaaS.
