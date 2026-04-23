from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from typing import Dict, List, Optional, Tuple
import pytz
import re

from sipuni_api import fetch_calls_csv_export_all
from utils_csv import parse_csv
from state_store import StateStore


@dataclass
class OperatorStatus:
    name: str
    op_id: str
    category: str  # ACTIVE / INACTIVE / ABSENT
    last_call_time: Optional[datetime]
    current_inactive_seconds: int
    current_inactive_str: str
    calls_today: int
    total_inactive_seconds: int
    total_inactive_str: str
    first_call_str: str
    last_call_str: str
    from_number: Optional[str]
    to_number: Optional[str]


def fmt_hms(seconds: int) -> str:
    s = max(0, int(seconds or 0))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h}ч {m}м {sec}с"


def _parse_hm(s: str) -> dtime:
    h, m = map(int, s.split(":"))
    return dtime(h, m)


def parse_call_dt(value: str, csv_tz, target_tz):
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M"):
        try:
            naive = datetime.strptime(v, fmt)
            dt_csv = csv_tz.localize(naive)
            return dt_csv.astimezone(target_tz)
        except Exception:
            pass
    return None


def _norm(s: str) -> str:
    return str(s or "").strip().lower()


def _row_values_text(row: dict) -> str:
    try:
        return " ".join(_norm(v) for v in row.values())
    except Exception:
        return ""


def _pick_first_existing(row: dict, keys: List[str]) -> str:
    for k in keys:
        if k in row and row.get(k) not in (None, ""):
            return str(row.get(k))
    return ""


def _match_operator_row(row: dict, op_id: str, name: str, extra_keys: List[str]) -> bool:
    op_id = str(op_id).strip()
    name_n = _norm(name)

    id_val = _pick_first_existing(row, [
        "ID оператора", "ID Оператора", "Operator ID", "OperatorID",
        "operator_id", "operatorid", "ID"
    ])
    if id_val:
        id_val_n = re.sub(r"\D", "", str(id_val))
        op_id_n = re.sub(r"\D", "", op_id)
        if id_val_n and op_id_n and id_val_n == op_id_n:
            return True

    op_name_val = _pick_first_existing(row, ["Оператор", "Operator", "operator", "Оператор(ФИО)"])
    if op_name_val:
        on = _norm(op_name_val)
        if on == name_n:
            return True
        if name_n and name_n in on:
            return True

    text = _row_values_text(row)

    if name_n and name_n in text:
        return True

    op_digits = re.sub(r"\D", "", op_id)
    if op_digits:
        if re.search(rf"(?<!\d){re.escape(op_digits)}(?!\d)", text):
            return True

    for k in extra_keys or []:
        kn = _norm(k)
        if not kn:
            continue
        kd = re.sub(r"\D", "", kn)
        if kd and kd.isdigit():
            if re.search(rf"(?<!\d){re.escape(kd)}(?!\d)", text):
                return True
        else:
            if kn in text:
                return True

    return False


class MonitorService:
    def __init__(self, cfg, operators: Dict[str, Dict], state: StateStore):
        self.cfg = cfg
        self.operators = operators
        self.state = state

        self.tz = pytz.timezone(cfg.tz)
        self.csv_tz = pytz.timezone(cfg.sipuni_csv_tz or cfg.tz)

    def _mention_or_name(self, name: str) -> str:
        meta = self.operators.get(name) or {}
        tg = str(meta.get("tg") or "").strip()

        if not tg:
            return name

        if not tg.startswith("@"):
            if re.fullmatch(r"[A-Za-z0-9_]{5,32}", tg):
                tg = "@" + tg
            else:
                return name

        if re.fullmatch(r"@[A-Za-z0-9_]{5,32}", tg):
            return tg

        return name

    def _display_name(self, name: str) -> str:
        meta = self.operators.get(name) or {}
        project = str(meta.get("project") or "").strip()
        if project:
            return f"{name} | {project}"
        return name

    def _rop_by_project(self, project: str) -> str:
        try:
            from operators_config import PROJECT_ROPS
            return str(PROJECT_ROPS.get(project) or "").strip()
        except Exception:
            return ""

    def _shift_bounds(self, day: date) -> Optional[Tuple[datetime, datetime]]:
        weekday = day.weekday()
        if weekday not in self.cfg.work_schedule:
            return None
        ws, we = self.cfg.work_schedule[weekday]
        start = self.tz.localize(datetime.combine(day, _parse_hm(ws)))
        end = self.tz.localize(datetime.combine(day, _parse_hm(we)))
        return start, end

    def _break_bounds(self, day: date) -> Optional[Tuple[datetime, datetime]]:
        if not (self.cfg.lunch_start and self.cfg.lunch_end):
            return None
        ls = self.tz.localize(datetime.combine(day, _parse_hm(self.cfg.lunch_start)))
        le = self.tz.localize(datetime.combine(day, _parse_hm(self.cfg.lunch_end)))
        return ls, le

    def _work_segments_excluding_break(self, day: date) -> List[Tuple[datetime, datetime]]:
        sb = self._shift_bounds(day)
        if not sb:
            return []
        start, end = sb

        bb = self._break_bounds(day)
        if not bb:
            return [(start, end)]

        ls, le = bb
        segs: List[Tuple[datetime, datetime]] = []
        if start < ls:
            segs.append((start, min(ls, end)))
        if le < end:
            segs.append((max(le, start), end))
        return [(a, b) for a, b in segs if b > a]

    def is_in_shift(self, dt: datetime) -> bool:
        sb = self._shift_bounds(dt.date())
        if not sb:
            return False
        start, end = sb
        return start <= dt <= end

    def is_in_break(self, dt: datetime) -> bool:
        bb = self._break_bounds(dt.date())
        if not bb:
            return False
        ls, le = bb
        return ls <= dt < le

    def _clip_to_shift(self, day: date, dt: datetime) -> datetime:
        sb = self._shift_bounds(day)
        if not sb:
            return dt
        start, end = sb
        if dt < start:
            return start
        if dt > end:
            return end
        return dt

    def _seconds_between(self, segments, a: datetime, b: datetime) -> int:
        if b <= a:
            return 0
        total = 0
        for x, y in segments:
            s = max(x, a)
            e = min(y, b)
            if e > s:
                total += int((e - s).total_seconds())
        return total

    def build_snapshot(self):
        now = datetime.now(self.tz)
        today = now.date()

        sb = self._shift_bounds(today)
        if not sb:
            return [], now, False, False, "no shift today"

        shift_start, shift_end = sb
        in_shift_now = self.is_in_shift(now)
        break_now = self.is_in_break(now)

        now_clipped = self._clip_to_shift(today, now)
        segments = self._work_segments_excluding_break(today)

        csv_data, err = fetch_calls_csv_export_all(
            self.cfg.sipuni_user,
            self.cfg.sipuni_secret,
            limit=5000,
            order="desc",
            page=1,
        )
        if not csv_data:
            return [], now, in_shift_now, break_now, err

        _, rows = parse_csv(csv_data)
        snapshot: List[OperatorStatus] = []
        min_thr = min(self.cfg.thresholds_minutes) if self.cfg.thresholds_minutes else 15

        for name, meta in self.operators.items():
            op_id = str(meta["id"])
            extra_keys = meta.get("match") or []

            calls: List[datetime] = []
            last_row = None
            last_dt = None

            for r in rows:
                if not _match_operator_row(r, op_id, name, extra_keys):
                    continue

                dt_raw = _pick_first_existing(r, ["Время", "Дата", "Date", "Datetime", "Дата/время"])
                dt = parse_call_dt(dt_raw, self.csv_tz, self.tz)
                if not dt or dt.date() != today:
                    continue
                if not (shift_start <= dt <= shift_end):
                    continue

                calls.append(dt)

                if last_dt is None or dt > last_dt:
                    last_dt = dt
                    last_row = r

            calls.sort()
            first = calls[0] if calls else None
            last = calls[-1] if calls else None

            first_str = first.strftime("%H:%M") if first else "—"
            last_str = last.strftime("%H:%M") if last else "—"

            anchor = last if last else shift_start
            current = self._seconds_between(segments, anchor, now_clipped)

            points = [shift_start] + calls + [now_clipped]
            total = 0
            for i in range(len(points) - 1):
                total += self._seconds_between(segments, points[i], points[i + 1])

            if self.state.is_absent_today(op_id, now):
                category = "ABSENT"
                current = 0
                total = 0
            else:
                category = "ACTIVE" if (current // 60) < min_thr else "INACTIVE"

            snapshot.append(
                OperatorStatus(
                    name=name,
                    op_id=op_id,
                    category=category,
                    last_call_time=last,
                    current_inactive_seconds=current,
                    current_inactive_str=fmt_hms(current),
                    calls_today=len(calls),
                    total_inactive_seconds=total,
                    total_inactive_str=fmt_hms(total),
                    first_call_str=first_str,
                    last_call_str=last_str,
                    from_number=(last_row or {}).get("Откуда"),
                    to_number=(last_row or {}).get("Куда"),
                )
            )

        snapshot.sort(key=lambda x: x.name.lower())
        return snapshot, now, in_shift_now, break_now, None

    def find_by_id(self, snapshot: List[OperatorStatus], op_id: str) -> Optional[OperatorStatus]:
        for s in snapshot:
            if str(s.op_id) == str(op_id):
                return s
        return None

    def format_inactive_alert(self, s: OperatorStatus, threshold_min: int) -> str:
        thr = int(threshold_min)
        head = f"🚫 ОПЕРАТОР ОТСУТСТВУЕТ {thr} МИН" if thr >= 60 else f"⛔ ОПЕРАТОР НЕАКТИВЕН {thr} МИН"

        who = self._display_name(s.name)

        meta = self.operators.get(s.name) or {}
        project = str(meta.get("project") or "").strip()
        rop = self._rop_by_project(project)
        rop_line = f"👨‍💼 РОП: {rop}\n" if rop else ""

        return (
            f"{head}\n\n"
            f"👤 {who}\n"
            f"{rop_line}"
            f"🆔 ID: {s.op_id}\n"
            f"⏱ Не звонит: {s.current_inactive_str}\n"
            f"📞 Активные звонки: {s.calls_today}\n"
            f"🕒 Первый звонок: {s.first_call_str}\n"
            f"🕒 Последняя попытка: {s.last_call_str}\n"
            f"📍 Откуда: {s.from_number or '—'}\n"
            f"📍 Куда: {s.to_number or '—'}"
        )

    def format_status_text(self, snapshot, updated_at, working: bool) -> str:
        title = "🟢 РАБОЧАЯ СМЕНА" if working else "⚪️ ВНЕ СМЕНЫ/ОБЕД"
        lines = [
            f"{title}\n",
            f"📅 Дата: {updated_at.strftime('%d.%m.%Y')}",
            f"⏰ Текущее время: {updated_at.strftime('%H:%M:%S')}",
            "------------------------------",
        ]

        total_calls = 0
        total_inactive_all = 0

        for s in snapshot:
            total_calls += int(s.calls_today or 0)
            total_inactive_all += int(s.total_inactive_seconds or 0)

            lines.append(
                f"👤 {self._display_name(s.name)}\n"
                f"📞 Активные звонки: {s.calls_today}\n"
                f"⛔ Текущая неактивность: {s.current_inactive_str}\n"
                f"🕒 Первый звонок: {s.first_call_str}\n"
                f"🕒 Последняя попытка: {s.last_call_str}\n"
                f"🔘 Статус: {s.category}\n"
                f"------------------------------"
            )

        lines.append(
            f"📊 ИТОГО ПО СМЕНЕ\n"
            f"📞 Всего активных звонков: {total_calls}\n"
            f"⛔ Общая неактивность: {fmt_hms(total_inactive_all)}"
        )
        return "\n".join(lines)

    def format_who(self, snapshot, updated_at) -> str:
        items = [s for s in snapshot if s.category == "INACTIVE"]
        lines = [
            "🔴 НЕАКТИВНЫЕ ОПЕРАТОРЫ",
            f"⏰ Проверка: {updated_at.strftime('%H:%M:%S')}",
            "------------------------------",
        ]
        if not items:
            lines.append("✅ Нет неактивных операторов")
            return "\n".join(lines)

        for s in items:
            who = self._display_name(s.name)
            lines.append(
                f"👤 {who}\n"
                f"⛔ Не звонит: {s.current_inactive_str}\n"
                f"📞 Активные звонки: {s.calls_today}\n"
                f"🕒 Последняя попытка: {s.last_call_str}\n"
                f"------------------------------"
            )
        return "\n".join(lines)

    def format_operator_list(self) -> str:
        return "👥 Выберите оператора:"

    def format_operator_card(self, s: OperatorStatus) -> str:
        return (
            f"👤 ОПЕРАТОР: {self._display_name(s.name)}\n"
            f"🆔 ID: {s.op_id}\n\n"
            f"📞 Активные звонки: {s.calls_today}\n"
            f"⛔ Текущая неактивность: {s.current_inactive_str}\n"
            f"🕒 Первый звонок: {s.first_call_str}\n"
            f"🕒 Последняя попытка: {s.last_call_str}\n\n"
            f"🔘 Статус: {s.category}"
        )

    def format_absent_confirm(self, s: OperatorStatus) -> str:
        return (
            "⛔ ПОДТВЕРЖДЕНИЕ\n\n"
            "Отметить оператора как ОТСУТСТВУЮЩЕГО?\n\n"
            f"👤 {self._display_name(s.name)}\n"
            f"🆔 ID: {s.op_id}\n\n"
            "⚠️ Оператор будет исключён из мониторинга\n"
            "⚠️ Алерты будут отключены"
        )

    def format_daily_report(self, snapshot, updated_at) -> str:
        lines = [
            "📅 ДНЕВНОЙ ОТЧЁТ",
            f"Дата: {updated_at.strftime('%d.%m.%Y')}",
            "------------------------------",
        ]
        total_calls = 0

        for s in snapshot:
            total_calls += int(s.calls_today or 0)
            lines.append(
                f"👤 {self._display_name(s.name)}\n"
                f"📞 Звонков за сегодня: {s.calls_today}\n"
                f"🕒 Первый звонок: {s.first_call_str}\n"
                f"🕒 Последняя попытка: {s.last_call_str}\n"
                f"🔘 Статус на конец дня: {s.category}\n"
                f"------------------------------"
            )

        lines.append(
            f"📊 ИТОГО\n"
            f"📞 Всего активных звонков: {total_calls}\n"
        )
        return "\n".join(lines)