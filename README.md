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
- Налаштування частоти опитування та фільтрації файлів.

## Встановлення залежностей

Додаткові залежності відсутні — використовується стандартна бібліотека Python 3.10+.

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
      "compute_checksum": true,
      "emit_on_start": false
    }
  ]
}
```

### Ключові параметри

- `database.path` — шлях до SQLite файлу з таблицею `file_events`.
- `poll_interval` — глобальний інтервал опитування (у секундах). Можна перевизначити для окремого каталогу через `directories[].poll_interval`.
- `directories[].include`/`exclude` — патерни (fnmatch) для фільтрації файлів.
- `directories[].compute_checksum` — чи обчислювати контрольну суму файлів.
- `directories[].emit_on_start` — якщо `true`, то події для наявних файлів будуть зафіксовані одразу після запуску.

## Запуск

```bash
python -m crm_file_event_service --config config.json
```

Для одноразового циклу (корисно для діагностики) використайте прапорець `--once`:

```bash
python -m crm_file_event_service --config config.json --once
```

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
