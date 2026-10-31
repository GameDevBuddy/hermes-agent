"""Fresh real canonical SQLite observer; synthetic tools only, no native/model/context issuer."""
import hashlib
from pathlib import Path
import sys

from tests.agent.canonical_commit_fixture import CommitFixture

RETAINED = []


def source_case(request):
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("Canonical source fixtures require fresh declared PM environment")
    directory = (Path(sys.prefix).resolve().parent / "sqlite-fixtures"
                 / hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:24])
    directory.mkdir(parents=True, exist_ok=False)
    result = CommitFixture(directory)
    RETAINED.append(result)
    return result


def original_self(error, initializer):
    traceback = error.__traceback__
    while traceback is not None:
        if traceback.tb_frame.f_code is initializer.__code__:
            return traceback.tb_frame.f_locals["self"]
        traceback = traceback.tb_next
    raise AssertionError("Actual original initializer never ran")
