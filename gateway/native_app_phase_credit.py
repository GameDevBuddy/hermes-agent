"""Reserve final control-book cells BEFORE a prepared native call can be issued."""
from gateway.native_source_runtime import refuse


class FollowupCredit:
    def __init__(self, call, command, key, book):
        self.call, self.command, self.key, self.book = call, command, key, book
        self.phase = None


def reserve_followups(call, reader, frame):
    from gateway.native_app_context_records import RECORD_CREDIT, reserve
    from gateway.native_app_phases import COMMANDS
    source, book = reader.source, reader.source._app_context_requests
    for command in COMMANDS[1:]:
        key = ("native-phase-credit", frame["requestId"], command)
        reserve(source, book, key, RECORD_CREDIT)
        credit = FollowupCredit(call, command, key, book)
        call.phase_credits[command] = credit
        book[key] = credit


def original_followup(call, reader, frame):
    book = reader.source._app_context_requests
    credit = call.phase_credits.get(frame["command"])
    if (type(credit) is not FollowupCredit or credit.call is not call or credit.book is not book
            or book.get(credit.key) is not credit or credit.phase is not None):
        refuse("native_original_phase_credit_missing_or_consumed")
    if frame["requestId"] in book:
        refuse("native_original_phase_request_replay")
    for record in book.values():
        original = getattr(record, "frame", None)
        if isinstance(original, dict) and original.get("requestId") == frame["requestId"]:
            refuse("native_original_phase_request_replay")
    return credit
