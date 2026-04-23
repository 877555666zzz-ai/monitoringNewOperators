import requests
from typing import Optional, Dict, Any, List, Tuple


class TelegramClient:
    def __init__(self, bot_token: str, default_chat_id: str, timeout: int = 20):
        self.bot_token = str(bot_token).strip()
        self.default_chat_id = str(default_chat_id).strip()
        self.timeout = int(timeout)

        if not self.bot_token:
            raise RuntimeError("Telegram bot token is empty")
        if not self.default_chat_id:
            raise RuntimeError("Telegram default chat_id is empty")

        self._base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def _post(self, method: str, payload: Dict[str, Any]):
        url = f"{self._base_url}/{method}"
        try:
            r = requests.post(url, json=payload, timeout=self.timeout)
        except requests.exceptions.Timeout:
            print(f"[TG TIMEOUT] {method}")
            return None
        except Exception as e:
            print(f"[TG EXCEPTION] {method}: {e}")
            return None

        if r.status_code != 200:
            body = r.text

            if method == "answerCallbackQuery" and r.status_code == 400:
                return None

            if method == "editMessageText" and "message is not modified" in body:
                return None

            print(f"[TG ERROR] {method} {r.status_code}: {body}")
            return None

        try:
            return r.json()
        except Exception:
            return None

    def _get(self, method: str, params: Dict[str, Any]):
        url = f"{self._base_url}/{method}"
        try:
            r = requests.get(url, params=params, timeout=self.timeout + 5)
        except requests.exceptions.Timeout:
            return None
        except Exception:
            return None

        if r.status_code != 200:
            return None

        try:
            return r.json()
        except Exception:
            return None

    def set_my_commands(self, commands: list):
        self._post("setMyCommands", {"commands": commands})

    def send_message(
        self,
        text: str,
        chat_id: Optional[str] = None,
        reply_markup: Optional[dict] = None,
        disable_preview: bool = True,
        message_thread_id: Optional[int] = None,
    ) -> Optional[int]:
        payload = {
            "chat_id": str(chat_id or self.default_chat_id),
            "text": text,
            "disable_web_page_preview": disable_preview,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if message_thread_id:
            payload["message_thread_id"] = int(message_thread_id)

        data = self._post("sendMessage", payload)
        if not data:
            return None
        return data.get("result", {}).get("message_id")

    def edit_message(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: Optional[dict] = None,
        disable_preview: bool = True,
    ) -> bool:
        payload = {
            "chat_id": str(chat_id),
            "message_id": int(message_id),
            "text": text,
            "disable_web_page_preview": disable_preview,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        return self._post("editMessageText", payload) is not None

    def answer_callback(self, callback_query_id: str, text: str = ""):
        self._post("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})

    def get_updates(self, offset: Optional[int] = None, timeout_sec: int = 10):
        params: Dict[str, Any] = {"timeout": int(timeout_sec)}
        if offset is not None:
            params["offset"] = int(offset)

        data = self._get("getUpdates", params)
        if not data:
            return []
        return data.get("result", [])

    @staticmethod
    def keyboard_main():
        return {
            "inline_keyboard": [
                [
                    {"text": "🔄 Обновить", "callback_data": "status:refresh"},
                    {"text": "🔴 Неактивные", "callback_data": "status:inactive"},
                ],
                [
                    {"text": "👥 Операторы", "callback_data": "operator:list"},
                ],
            ]
        }

    @staticmethod
    def keyboard_inactive(op_id: str):
        return {
            "inline_keyboard": [
                [
                    {"text": "✅ WhatsApp (отменить алерт)", "callback_data": f"wa:{op_id}"},
                    {"text": "⛔ Отсутствует", "callback_data": f"abs:{op_id}"},
                ]
            ]
        }

    @staticmethod
    def keyboard_absent_confirm(op_id: str):
        return {
            "inline_keyboard": [
                [
                    {"text": "✅ Да, отсутствует", "callback_data": f"abs_yes:{op_id}"},
                    {"text": "❌ Отмена", "callback_data": f"abs_cancel:{op_id}"},
                ]
            ]
        }

    @staticmethod
    def keyboard_operator_list(operators: List[Tuple[str, str]]):
        rows = []
        line = []
        for op_id, name in operators:
            line.append({"text": name, "callback_data": f"op:{op_id}"})
            if len(line) == 2:
                rows.append(line)
                line = []
        if line:
            rows.append(line)
        rows.append([{"text": "⬅️ Назад", "callback_data": "status:refresh"}])
        return {"inline_keyboard": rows}

    @staticmethod
    def keyboard_operator_detail(op_id: str):
        return {
            "inline_keyboard": [
                [
                    {"text": "✅ WhatsApp (отменить алерт)", "callback_data": f"wa:{op_id}"},
                    {"text": "⛔ Отсутствует", "callback_data": f"abs:{op_id}"},
                ],
                [
                    {"text": "⬅️ К операторам", "callback_data": "operator:list"},
                ],
            ]
        }