import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _clean(v) -> str:
    return str(v).strip() if v is not None else ""


def _req(name: str) -> str:
    v = _clean(os.getenv(name))
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def _parse_thresholds(s: str) -> list[int]:
    s = _clean(s)
    if not s:
        return [15, 30, 60]
    out: list[int] = []
    for x in s.split(","):
        try:
            out.append(int(x.strip()))
        except Exception:
            pass
    out = sorted(set([x for x in out if x > 0]))
    return out or [15, 30, 60]


@dataclass(frozen=True)
class Config:
    sipuni_user: str = _req("SIPUNI_USER")
    sipuni_secret: str = _req("SIPUNI_SECRET")

    tz: str = _clean(os.getenv("TZ", "Asia/Almaty"))
    sipuni_csv_tz: str = _clean(os.getenv("SIPUNI_CSV_TZ", ""))  # если пусто -> = tz

    check_every_seconds: int = int(_clean(os.getenv("CHECK_EVERY_SECONDS", "60")))

    thresholds_minutes: list[int] = field(
        default_factory=lambda: _parse_thresholds(os.getenv("THRESHOLDS_MINUTES", "15,30,60"))
    )

    # ПН–ПТ (по умолчанию 10:00–19:00)
    work_start: str = _clean(os.getenv("WORK_START", "10:00"))
    work_end: str = _clean(os.getenv("WORK_END", "19:00"))

    # ✅ СБ (по твоему требованию тоже 10:00–19:00)
    sat_work_start: str = _clean(os.getenv("SAT_WORK_START", "10:00"))
    sat_work_end: str = _clean(os.getenv("SAT_WORK_END", "19:00"))

    # График: 0=ПН ... 5=СБ
    work_schedule: dict[int, tuple[str, str]] = field(
        default_factory=lambda: {
            0: (_clean(os.getenv("WORK_START", "10:00")), _clean(os.getenv("WORK_END", "19:00"))),
            1: (_clean(os.getenv("WORK_START", "10:00")), _clean(os.getenv("WORK_END", "19:00"))),
            2: (_clean(os.getenv("WORK_START", "10:00")), _clean(os.getenv("WORK_END", "19:00"))),
            3: (_clean(os.getenv("WORK_START", "10:00")), _clean(os.getenv("WORK_END", "19:00"))),
            4: (_clean(os.getenv("WORK_START", "10:00")), _clean(os.getenv("WORK_END", "19:00"))),
            5: (_clean(os.getenv("SAT_WORK_START", "10:00")), _clean(os.getenv("SAT_WORK_END", "19:00"))),
        }
    )

    lunch_start: str = _clean(os.getenv("LUNCH_START", "13:00"))
    lunch_end: str = _clean(os.getenv("LUNCH_END", "14:00"))

    tg_token: str = _req("TELEGRAM_BOT_TOKEN")

    # группа (supergroup) id -100...
    tg_chat_id: str = _req("TELEGRAM_CHAT_ID")

    # ✅ дефолтный топик "Мониторинг" (message_thread_id), чтобы НЕ писать в General
    tg_thread_id: int = int(_clean(os.getenv("TELEGRAM_THREAD_ID", "0")) or 0)

    # алерты
    tg_alert_chat_id: str = _clean(os.getenv("TELEGRAM_ALERT_CHAT_ID", "")) or tg_chat_id
    tg_alert_thread_id: int = int(_clean(os.getenv("TELEGRAM_ALERT_THREAD_ID", "0")) or 0)

    sheet_id: str = _req("GOOGLE_SHEET_ID")
    google_creds_b64: str = _req("GOOGLE_CREDS_B64")