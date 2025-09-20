# CRM File Event Monitoring Service

Цей сервіс відстежує локальні або мережеві папки та зберігає події у таблиці `file_events` бази даних. Отримані записи можна використовувати в CRM, наприклад, у модулі «Історія змін».

## Можливості

- Моніторинг кількох каталогів (локальних або змонтованих мережевих ресурсів).
- Фіксація подій створення, змін і видалення файлів.
- Збереження у SQLite таблицю `file_events` із полями:
  - `event_time` — час фіксації події (UTC ISO 8601);
  - `event_type` — тип події (`created`, `modified`, `deleted`);
  - `path` — повний шлях до файлу;
  - `project`, `username` — дані контексту (проект або користувач);
  - `file_size` — розмір файлу у байтах;
  - `checksum` — контрольна сума (опційно);
  - `details` — додаткова інформація, наприклад, шлях кореня моніторингу.
- Підтримка бекендів періодичного опитування та подієвого моніторингу через `watchfiles`.
- Гнучка фільтрація подій за шляхами та розміром файлів.

## Встановлення залежностей

Сервіс розрахований на Python 3.10+. Для роботи FastAPI-API та подієвого моніторингу
потрібні пакети з [`requirements.txt`](requirements.txt) (зокрема `watchfiles`).

## Конфігурація

Приклад конфігурації знаходиться у файлі [`config.example.json`](config.example.json). Створіть власний файл, наприклад `config.json`, і відредагуйте:

```json
{
  "database": {
    "path": "./file_events.db"
  },
  "poll_interval": 5,
  "directories": [
    {
      "path": "//server/share/documents",
      "project": "CRM",
      "username": "team-a",
      "include": ["*.docx", "*.xlsx"],
      "exclude": ["~$*"],
      "backend": "watchfiles",
      "compute_checksum": true,
      "min_file_size": 1024,
      "max_file_size": 52428800,
      "emit_on_start": false
    }
  ]
}
```

### Ключові параметри

- `database.path` — шлях до SQLite файлу з таблицею `file_events`.
- `poll_interval` — глобальний інтервал опитування (у секундах). Можна перевизначити для окремого каталогу через `directories[].poll_interval`.
- `directories[].include`/`exclude` — патерни (fnmatch) для фільтрації файлів.
- `directories[].backend` — `polling` (повне опитування) або `watchfiles` для подієвого моніторингу.
- `directories[].compute_checksum` — чи обчислювати контрольну суму файлів.
- `directories[].emit_on_start` — якщо `true`, то події для наявних файлів будуть зафіксовані одразу після запуску.
- `directories[].min_file_size`/`max_file_size` — обмеження на розмір файлів у байтах (події поза діапазоном ігноруються).

## Запуск

```bash
python -m crm_file_event_service --config config.json
```

Для одноразового циклу (корисно для діагностики) використайте прапорець `--once`:

```bash
python -m crm_file_event_service --config config.json --once
```

## FastAPI API та live-оновлення

Для швидкого прототипування REST/WebSocket-шару додано застосунок на FastAPI
(`crm_file_event_service/api.py`). Він запускає `FileEventService` у фоновому
потоці, надає REST-ендпоїнт `GET /events` та WebSocket `ws://.../ws/events` для
оновлень у режимі реального часу. Це дозволяє напряму зчитувати події із
SQLite та транслювати їх клієнту без додаткових бібліотек.

### Встановлення залежностей

```bash
python -m pip install -r requirements.txt
```

### Запуск у Windows (PowerShell)

```powershell
cd C:\path\to\CRM-dashboard-manager
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item config.example.json config.windows.json
# Відредагуйте config.windows.json, використовуючи Windows-шляхи, наприклад
# \\server\share або C:\\Data\\Documents
$env:CRM_SERVICE_CONFIG = "C:\\path\\to\\CRM-dashboard-manager\\config.windows.json"
uvicorn crm_file_event_service.api:app --host 0.0.0.0 --port 8000
```

Після старту API-ендпоїнт `http://127.0.0.1:8000/events` повертає останні
події, а WebSocket за адресою `ws://127.0.0.1:8000/ws/events` надсилатиме нові
записи. Фронтенд із `file-manager-dashboard.html` може під'єднуватись до цих
ендпоїнтів для live-оновлень.

## Інтеграція з CRM

- Таблиця `file_events` може бути підключена до CRM (наприклад, через ORM або API) для побудови модулю «Історія змін».
- Рекомендується налаштувати регулярне очищення або архівацію даних при великій кількості подій.

## Розгортання як сервісу

Скрипт можна запускати як фоновий процес за допомогою `systemd`, `supervisord` чи іншого менеджера процесів. Приклад юніта systemd:

```ini
[Unit]
Description=CRM File Event Monitor
After=network.target

[Service]
WorkingDirectory=/opt/crm-monitor
ExecStart=/usr/bin/python -m crm_file_event_service --config /opt/crm-monitor/config.json
Restart=always

[Install]
WantedBy=multi-user.target
```

Після інтеграції сервіс автоматично накопичуватиме події, а CRM зможе відображати їх у зручному вигляді для користувачів.
