import time
from datetime import datetime
import pytz

from config import Config
from operators_config import OPERATORS
from state_store import StateStore
from monitor import MonitorService
from telegram_client import TelegramClient
from sheets_logger import SheetsLogger


def normalize_command(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    if not t.startswith("/"):
        return ""
    t = t.split()[0]
    if "@" in t:
        t = t.split("@")[0]
    return t.lower()


def pick_thread_id_from_message(m: dict, cfg: Config):
    """
    Если команда пришла из топика -> отвечаем туда же.
    Если команда пришла из General -> отвечаем в дефолтный топик cfg.tg_thread_id (если задан).
    """
    tid = m.get("message_thread_id")
    if tid:
        return int(tid)
    return int(cfg.tg_thread_id) if int(cfg.tg_thread_id or 0) > 0 else None


def main():
    cfg = Config()
    tz = pytz.timezone(cfg.tz)

    tg = TelegramClient(cfg.tg_token, cfg.tg_chat_id)
    state = StateStore()
    monitor = MonitorService(cfg, OPERATORS, state)

    sheets = None
    try:
        sheets = SheetsLogger(cfg.google_creds_b64, cfg.sheet_id)
    except Exception as e:
        print(f"⚠️ Sheets disabled: {e}")
        sheets = None

    tg.set_my_commands([
        {"command": "status", "description": "Статус операторов"},
        {"command": "who", "description": "Кто неактивен"},
        {"command": "operator", "description": "Инфо по оператору"},
        {"command": "daily", "description": "Дневной отчет"},
    ])

    run_started_at = datetime.now(tz)
    run_started_hm = run_started_at.strftime("%H:%M")

    # ✅ стартовое сообщение — в дефолтный топик "Мониторинг" (чтобы не в General)
    tg.send_message(
        f"🚀 SipuniMonitor запущен\n{run_started_at.strftime('%d.%m.%Y %H:%M')} ({cfg.tz})",
        chat_id=cfg.tg_chat_id,
        message_thread_id=(cfg.tg_thread_id or None),
    )

    offset = None
    last_check_ts = 0

    snapshot = None
    updated_at = None
    in_shift = False
    in_break = False
    snapshot_ts = 0

    last_in_shift = False

    def get_snapshot(force: bool = False):
        nonlocal snapshot, updated_at, in_shift, in_break, snapshot_ts
        now_ts = time.time()
        if force or (snapshot is None) or (now_ts - snapshot_ts > 10):
            snapshot, updated_at, in_shift, in_break, err = monitor.build_snapshot()
            snapshot_ts = now_ts
            if err and in_shift:
                print(f"[SNAPSHOT ERROR] {err}")
        return snapshot, updated_at, in_shift, in_break

    while True:
        now = datetime.now(tz)
        date_obj = now.date()
        date_str = now.strftime("%d.%m.%Y")

        # =================
        # Telegram updates
        # =================
        updates = tg.get_updates(offset=offset, timeout_sec=1)
        for u in updates:
            offset = u["update_id"] + 1

            # ---------
            # CALLBACKS
            # ---------
            if "callback_query" in u:
                cq = u["callback_query"]
                data = cq.get("data", "")
                chat_id = str(cq["message"]["chat"]["id"])
                message_id = int(cq["message"]["message_id"])

                user = cq.get("from", {})
                by = user.get("username") or user.get("first_name") or "unknown"

                tg.answer_callback(cq["id"], "OK")
                snapshot, updated_at, in_shift, in_break = get_snapshot(force=True)

                if data == "status:refresh":
                    tg.edit_message(
                        chat_id, message_id,
                        monitor.format_status_text(snapshot, updated_at, bool(in_shift and not in_break)),
                        reply_markup=tg.keyboard_main(),
                    )

                elif data == "status:inactive":
                    tg.edit_message(
                        chat_id, message_id,
                        monitor.format_who(snapshot, updated_at),
                        reply_markup=tg.keyboard_main(),
                    )

                elif data == "operator:list":
                    ops = [(str(meta["id"]), name) for name, meta in OPERATORS.items()]
                    tg.edit_message(
                        chat_id, message_id,
                        monitor.format_operator_list(),
                        reply_markup=tg.keyboard_operator_list(ops),
                    )

                elif data.startswith("op:"):
                    op_id = data.split(":", 1)[1]
                    s = monitor.find_by_id(snapshot, op_id)
                    if not s:
                        tg.edit_message(chat_id, message_id, "⚠️ Оператор не найден", reply_markup=tg.keyboard_main())
                    else:
                        tg.edit_message(
                            chat_id, message_id,
                            monitor.format_operator_card(s),
                            reply_markup=tg.keyboard_operator_detail(op_id),
                        )

                elif data.startswith("wa:"):
                    op_id = data.split(":", 1)[1]

                    # ✅ WA = откат конкретного алерта + WA++
                    ok = state.mark_wa_cancel_alert(op_id, updated_at, message_id)
                    if ok:
                        tg.edit_message(chat_id, message_id, "✅ WhatsApp принят. Алерт отменён, мониторинг продолжается.")
                    else:
                        tg.edit_message(chat_id, message_id, "⚠️ Это не алерт (или уже отменён).")

                elif data.startswith("abs:"):
                    op_id = data.split(":", 1)[1]
                    s = monitor.find_by_id(snapshot, op_id)
                    if not s:
                        tg.edit_message(chat_id, message_id, "⚠️ Оператор не найден", reply_markup=tg.keyboard_main())
                    else:
                        tg.edit_message(
                            chat_id, message_id,
                            monitor.format_absent_confirm(s),
                            reply_markup=tg.keyboard_absent_confirm(op_id),
                        )

                elif data.startswith("abs_yes:"):
                    op_id = data.split(":", 1)[1]
                    state.mark_absent_today(op_id, updated_at, by=str(by))
                    tg.edit_message(chat_id, message_id, "⛔ Оператор отсутствует (исключён из мониторинга)")

                elif data.startswith("abs_cancel:"):
                    op_id = data.split(":", 1)[1]
                    s = monitor.find_by_id(snapshot, op_id)
                    if not s:
                        tg.edit_message(chat_id, message_id, "⚠️ Оператор не найден", reply_markup=tg.keyboard_main())
                    else:
                        tg.edit_message(
                            chat_id, message_id,
                            monitor.format_operator_card(s),
                            reply_markup=tg.keyboard_operator_detail(op_id),
                        )
                continue

            # ---------
            # MESSAGES
            # ---------
            if "message" in u:
                m = u["message"]
                cmd = normalize_command(m.get("text", ""))
                chat_id = str(m["chat"]["id"])
                thread_id = pick_thread_id_from_message(m, cfg)

                snapshot, updated_at, in_shift, in_break = get_snapshot(force=True)

                if cmd == "/status":
                    tg.send_message(
                        monitor.format_status_text(snapshot, updated_at, bool(in_shift and not in_break)),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        reply_markup=tg.keyboard_main(),
                    )

                elif cmd == "/who":
                    tg.send_message(
                        monitor.format_who(snapshot, updated_at),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        reply_markup=tg.keyboard_main(),
                    )

                elif cmd == "/operator":
                    ops = [(str(meta["id"]), name) for name, meta in OPERATORS.items()]
                    tg.send_message(
                        monitor.format_operator_list(),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                        reply_markup=tg.keyboard_operator_list(ops),
                    )

                elif cmd == "/daily":
                    tg.send_message(
                        monitor.format_daily_report(snapshot, updated_at),
                        chat_id=chat_id,
                        message_thread_id=thread_id,
                    )

        # =================
        # Periodic check
        # =================
        if time.time() - last_check_ts >= cfg.check_every_seconds:
            last_check_ts = time.time()

            snapshot, updated_at, in_shift, in_break = get_snapshot(force=True)

            # daily по факту окончания смены
            if last_in_shift and (not in_shift):
                if state.can_send_daily_report(updated_at):
                    tg.send_message(
                        monitor.format_daily_report(snapshot, updated_at),
                        chat_id=cfg.tg_chat_id,
                        message_thread_id=(cfg.tg_thread_id or None),
                    )
                    state.mark_daily_report_sent(updated_at)
            last_in_shift = in_shift

            # вне смены — ничего
            if not in_shift:
                time.sleep(1)
                continue

            # обед — ничего
            if in_break:
                time.sleep(1)
                continue

            # логи (по желанию)
            for s in snapshot:
                print(f"👤 {s.name} | {s.category} | current={s.current_inactive_str}")
            print(f"⏱ ПРОВЕРКА {updated_at.strftime('%H:%M:%S')}")

            if sheets:
                try:
                    sheets.prepare_day(date_str, date_obj, run_started_hm)
                except Exception as e:
                    print(f"[SHEETS] prepare_day error: {e}")

            for s in snapshot:
                absent_flag = 1 if state.is_absent_today(s.op_id, updated_at) else 0

                # (A) Синхронизация статуса
                if not absent_flag:
                    if s.category == "INACTIVE":
                        state.on_operator_inactive(s.op_id, updated_at, s.last_call_time)
                    else:
                        state.on_operator_active(s.op_id, updated_at, s.last_call_time)

                # (B) Алерты: только если INACTIVE и не absent
                if (not absent_flag) and s.category == "INACTIVE":
                    due = state.get_due_thresholds(
                        s.op_id, updated_at,
                        s.current_inactive_seconds,
                        cfg.thresholds_minutes,
                    )
                    for thr in due:
                        text = monitor.format_inactive_alert(s, thr)
                        msg_id = tg.send_message(
                            text,
                            chat_id=cfg.tg_alert_chat_id,
                            message_thread_id=(cfg.tg_alert_thread_id or None),
                            reply_markup=tg.keyboard_inactive(s.op_id),
                        )
                        state.register_alert_sent(s.op_id, updated_at, thr, msg_id)

                # (C) Sheets: пишем счётчики тегов (1->2->3)
                if sheets:
                    try:
                        sheets.upsert_operator_day(
                            date_str, date_obj, run_started_hm,
                            s.name, s.op_id,
                            int(s.calls_today),
                            state.get_alert_count(s.op_id, 15, updated_at),
                            state.get_alert_count(s.op_id, 30, updated_at),
                            state.get_alert_count(s.op_id, 60, updated_at),
                            state.get_wa_count(s.op_id, updated_at),
                            absent_flag,
                            updated_at.strftime("%H:%M"),
                        )
                    except Exception as e:
                        print(f"[SHEETS] upsert error {s.name}({s.op_id}): {e}")

        time.sleep(1)


if __name__ == "__main__":
    main()