"""Interpret adapter delivery results without changing shutdown notice policy."""
from typing import Any, Optional


def _notice_target_key(platform_value: str, chat_id, thread_id) -> tuple:
    """Dedup key for one notice destination: thread/topic platforms share a chat but route apart."""
    return (platform_value, str(chat_id), str(thread_id) if thread_id else None)


def _delivery_target_key(platform_value: str, chat_id, thread_id, *, profile: Optional[str] = None) -> tuple:
    """Dedupe key for one DELIVERED chat: profile-independent, except Telegram private chats.

    Two served profiles can share one home chat (one Telegram group for the whole host) and owe it
    ONE notice per host restart. A positive Telegram chat id names the USER, though: the same id
    under two bot tokens is two conversations, so those stay keyed per served profile (#118233).
    """
    from gateway.delivery import looks_like_telegram_private_chat_id
    if (profile and profile != "default" and platform_value == "telegram"
            and looks_like_telegram_private_chat_id(chat_id)):
        platform_value = f"{profile}:{platform_value}"
    return _notice_target_key(platform_value, chat_id, thread_id)


def _send_failed(result: Any) -> bool:
    """True when an adapter ``send()`` result explicitly reports failure."""
    return result is not None and getattr(result, "success", True) is False


def _send_error(result: Any) -> str:
    """Error text of a failed ``send()`` result (adapters may omit it)."""
    return getattr(result, "error", "send returned success=False")
