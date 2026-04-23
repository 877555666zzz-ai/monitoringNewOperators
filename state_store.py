from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class OperatorState:
    last_message_ids: List[int] = None

    absent_today: bool = False
    absent_by: str = ""
    absent_at: str = ""

    wa_count: int = 0

    status: str = "ACTIVE"
    last_call_time: Optional[datetime] = None

    sent_thresholds: Set[int] = None
    alert_counts: Dict[int, int] = None

    last_day: Optional[date] = None

    def __post_init__(self):
        self.last_message_ids = self.last_message_ids or []
        self.sent_thresholds = self.sent_thresholds or set()
        self.alert_counts = self.alert_counts or {15: 0, 30: 0, 60: 0}


class StateStore:
    def __init__(self):
        self._ops: Dict[str, OperatorState] = {}
        self._daily_report_sent_day: Optional[date] = None

        # msg_id -> (op_id, threshold)
        self._alert_msg_map: Dict[int, Tuple[str, int]] = {}

    def _get(self, op_id: str) -> OperatorState:
        op_id = str(op_id)
        if op_id not in self._ops:
            self._ops[op_id] = OperatorState()
        return self._ops[op_id]

    def _ensure_day(self, op_id: str, day: date):
        st = self._get(op_id)
        if st.last_day != day:
            st.last_day = day

            st.absent_today = False
            st.absent_by = ""
            st.absent_at = ""

            st.wa_count = 0

            st.status = "ACTIVE"
            st.last_call_time = None

            st.sent_thresholds.clear()
            st.last_message_ids.clear()
            st.alert_counts = {15: 0, 30: 0, 60: 0}

    # =========================
    # WA = "отменить алерт" + продолжить мониторинг
    # =========================
    def mark_wa_cancel_alert(self, op_id: str, now: datetime, message_id: int) -> bool:
        """
        Нажали WA на конкретном алерте.
        Делаем:
          - откатываем счётчик алерта (15/30/60) для этого message_id
          - wa_count += 1
          - monitoring continues (ничего не замораживаем)
        Возвращает True если реально отменили алерт.
        """
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())

        try:
            msg_id = int(message_id)
        except Exception:
            return False

        rec = self._alert_msg_map.get(msg_id)
        if not rec:
            return False

        mapped_op_id, thr = rec
        if str(mapped_op_id) != str(op_id):
            return False

        # ✅ это точно алерт данного оператора
        st.wa_count += 1

        t = int(thr)
        if t in (15, 30, 60):
            cur = int(st.alert_counts.get(t, 0))
            st.alert_counts[t] = max(0, cur - 1)

        if msg_id in st.last_message_ids:
            try:
                st.last_message_ids.remove(msg_id)
            except Exception:
                pass

        # удалить из карты, чтобы второй раз не откатывать
        try:
            del self._alert_msg_map[msg_id]
        except Exception:
            pass

        # ВАЖНО: sent_thresholds НЕ трогаем.
        # => 15м не будет слаться повторно в этом же цикле неактивности.
        return True

    def get_wa_count(self, op_id: str, now: datetime) -> int:
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        return st.wa_count

    # =========================
    # ABSENT
    # =========================
    def mark_absent_today(self, op_id: str, now: datetime, by: str = ""):
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())

        st.absent_today = True
        st.absent_by = by or ""
        st.absent_at = now.strftime("%H:%M")

        st.status = "ACTIVE"
        st.last_call_time = None
        st.sent_thresholds.clear()

    def is_absent_today(self, op_id: str, now: datetime) -> bool:
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        return st.absent_today

    # =========================
    # ACTIVITY
    # =========================
    def on_operator_active(self, op_id: str, now: datetime, last_call_time: Optional[datetime]):
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        if st.absent_today:
            return

        st.status = "ACTIVE"
        st.last_call_time = last_call_time or st.last_call_time

        # ✅ новая активность = новый цикл => снова можно слать 15/30/60
        st.sent_thresholds.clear()

    def on_operator_inactive(self, op_id: str, now: datetime, last_call_time: Optional[datetime]):
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        if st.absent_today:
            return

        st.status = "INACTIVE"
        st.last_call_time = last_call_time or st.last_call_time

    # =========================
    # THRESHOLDS
    # =========================
    def get_due_thresholds(
        self,
        op_id: str,
        now: datetime,
        current_inactive_seconds: int,
        thresholds_minutes: List[int]
    ) -> List[int]:
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())

        if st.absent_today or st.status != "INACTIVE":
            return []

        mins = int((current_inactive_seconds or 0) // 60)
        return [t for t in sorted(thresholds_minutes) if mins >= t and t not in st.sent_thresholds]

    def register_alert_sent(self, op_id: str, now: datetime, threshold_min: int, msg_id: Optional[int]):
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        if st.absent_today:
            return

        t = int(threshold_min)
        st.sent_thresholds.add(t)

        # ✅ каждый реальный алерт увеличивает значение
        if t in (15, 30, 60):
            st.alert_counts[t] = int(st.alert_counts.get(t, 0)) + 1

        if msg_id:
            mid = int(msg_id)
            st.last_message_ids.append(mid)
            self._alert_msg_map[mid] = (str(op_id), int(t))

    def get_alert_count(self, op_id: str, threshold_min: int, now: datetime) -> int:
        st = self._get(op_id)
        self._ensure_day(op_id, now.date())
        return int(st.alert_counts.get(int(threshold_min), 0))

    # =========================
    # DAILY
    # =========================
    def can_send_daily_report(self, now: datetime) -> bool:
        return self._daily_report_sent_day != now.date()

    def mark_daily_report_sent(self, now: datetime):
        self._daily_report_sent_day = now.date()