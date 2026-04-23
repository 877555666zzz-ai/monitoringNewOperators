from __future__ import annotations

import base64
import json
import time
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

import gspread
from google.oauth2.service_account import Credentials

SEPARATOR = "-" * 40
BLOCK_END = "=" * 40

HEADERS = [
    "Оператор",
    "ID",
    "Всего звонков",
    "Тег 15 мин",
    "Тег 30 мин",
    "Тег 60 мин",
    "WA",
    "Отсутствует",
    "Обновлено",
]


def _week_title(d: date) -> str:
    monday = d - timedelta(days=d.weekday())
    saturday = monday + timedelta(days=5)
    return f"{monday.strftime('%d.%m')}-{saturday.strftime('%d.%m')}"


def _col_letter(n: int) -> str:
    n = int(n)
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s or "A"


class SheetsLogger:
    def __init__(self, creds_b64: str, sheet_id: str):
        if not creds_b64:
            raise RuntimeError("GOOGLE_CREDS_B64 is empty")
        if not sheet_id:
            raise RuntimeError("GOOGLE_SHEET_ID is empty")

        try:
            creds_info = json.loads(base64.b64decode(creds_b64).decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Invalid GOOGLE_CREDS_B64: {e}")

        creds = Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(sheet_id)

        self._ws = None
        self._ws_title: Optional[str] = None

        self._cache_date: Optional[str] = None
        self._cache_index: Dict[str, int] = {}
        self._cache_header_row: Optional[int] = None
        self._cache_block_end: Optional[int] = None

    def _retry(self, fn, *args, **kwargs):
        for attempt in range(1, 5):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                msg = str(e).lower()
                transient = any(x in msg for x in [
                    "connection reset", "connection aborted", "timed out",
                    "502", "503", "504", "rate", "temporarily",
                    "transporterror", "remote disconnected", "protocolerror"
                ])
                if not transient or attempt == 4:
                    raise
                time.sleep(0.7 * attempt)

    def _get_or_create_week_ws(self, d: date):
        title = _week_title(d)
        if self._ws and self._ws_title == title:
            return self._ws

        try:
            ws = self._retry(self.sh.worksheet, title)
        except gspread.WorksheetNotFound:
            ws = self._retry(self.sh.add_worksheet, title=title, rows=6000, cols=20)

        self._ws = ws
        self._ws_title = title

        self._cache_date = None
        self._cache_index = {}
        self._cache_header_row = None
        self._cache_block_end = None

        return ws

    def _find_date_block(self, ws, date_str: str) -> Tuple[Optional[int], Optional[int]]:
        rows = self._retry(ws.get_all_values)
        if not rows:
            return None, None

        prefix = f"Дата: {date_str}"
        header_row = None

        for i, r in enumerate(rows, start=1):
            cell0 = (r[0] if r else "").strip()
            if cell0.startswith(prefix):
                header_row = i
                break

        if not header_row:
            return None, None

        end = header_row + 1
        while end <= len(rows):
            cell0 = (rows[end - 1][0] if rows[end - 1] else "").strip()
            if cell0 == BLOCK_END:
                return header_row, end
            end += 1

        return header_row, len(rows)

    def _create_date_block(self, ws, date_str: str, run_started_hm: str) -> Tuple[int, int]:
        self._retry(ws.append_row, [BLOCK_END])
        self._retry(ws.append_row, [f"Дата: {date_str} | Старт бота: {run_started_hm}"])
        self._retry(ws.append_row, [SEPARATOR])
        self._retry(ws.append_row, HEADERS)
        self._retry(ws.append_row, [BLOCK_END])

        rows = self._retry(ws.get_all_values)
        return len(rows) - 3, len(rows)

    def prepare_day(self, date_str: str, d: date, run_started_hm: str):
        ws = self._get_or_create_week_ws(d)

        if self._cache_date == date_str and self._cache_header_row and self._cache_block_end:
            return

        header_row, end_row = self._find_date_block(ws, date_str)
        if not header_row:
            header_row, end_row = self._create_date_block(ws, date_str, run_started_hm)

        self._cache_date = date_str
        self._cache_header_row = header_row
        self._cache_block_end = end_row
        self._cache_index = {}

        rows = self._retry(ws.get_all_values)
        data_start = header_row + 3
        data_end = end_row - 1

        for r_idx in range(data_start, data_end + 1):
            row = rows[r_idx - 1] if r_idx - 1 < len(rows) else []
            if len(row) >= 2:
                op_id = (row[1] or "").strip()
                if op_id:
                    self._cache_index[op_id] = r_idx

        last_col = _col_letter(len(HEADERS))  # I

        # форматирование
        try:
            # шапка блока даты
            ws.format(f"A{header_row}:{last_col}{header_row}", {
                "backgroundColor": {"red": 0.12, "green": 0.14, "blue": 0.18},
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            })
            # заголовки таблицы
            ws.format(f"A{header_row+2}:{last_col}{header_row+2}", {"textFormat": {"bold": True}})

            # ТЕГИ (D,E,F) — жёлтый фон
            ws.format(f"D{header_row+3}:F{end_row-1}", {
                "backgroundColor": {"red": 1.0, "green": 0.97, "blue": 0.75}
            })

            # WA (G) — зеленый
            ws.format(f"G{header_row+3}:G{end_row-1}", {
                "backgroundColor": {"red": 0.88, "green": 0.96, "blue": 0.88}
            })

            # Отсутствует (H) — красный
            ws.format(f"H{header_row+3}:H{end_row-1}", {
                "backgroundColor": {"red": 0.98, "green": 0.87, "blue": 0.87}
            })
        except Exception:
            pass

    def upsert_operator_day(
        self,
        date_str: str,
        d: date,
        run_started_hm: str,
        operator_name: str,
        op_id: str,
        calls_total: int,
        cnt_15: int,
        cnt_30: int,
        cnt_60: int,
        wa_count: int,
        absent_flag: int,
        updated_at: str,
    ):
        ws = self._get_or_create_week_ws(d)
        self.prepare_day(date_str, d, run_started_hm)

        row_values = [
            operator_name,
            str(op_id),
            int(calls_total or 0),
            int(cnt_15 or 0),
            int(cnt_30 or 0),
            int(cnt_60 or 0),
            int(wa_count or 0),
            int(absent_flag or 0),
            updated_at or "",
        ]

        last_col = _col_letter(len(HEADERS))  # I
        row = self._cache_index.get(str(op_id))
        if row:
            self._retry(ws.update, f"A{row}:{last_col}{row}", [row_values])
            return

        end = int(self._cache_block_end or 1)
        self._retry(ws.insert_row, row_values, index=end)

        self._cache_index[str(op_id)] = end
        self._cache_block_end = end + 1