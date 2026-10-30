"""Query-only association with the original parent's non-killing Windows Job.

Nothing here allocates a Job, assigns a process, signals a process, or discovers
processes. The native API is loaded only by the real attach consumer; tests replace
that private loader. This association is local custody, never native-call proof.
"""
from __future__ import annotations

import os


class JobAssociationUnknown(RuntimeError):
    pass


def _load_api():
    if os.name != "nt":
        raise JobAssociationUnknown("owned-job-requires-windows")
    import ctypes
    from ctypes import wintypes

    class ObjectBasicInformation(ctypes.Structure):
        _fields_ = [("Attributes", wintypes.ULONG), ("GrantedAccess", wintypes.ULONG),
                    ("HandleCount", wintypes.ULONG), ("PointerCount", wintypes.ULONG),
                    ("Reserved", wintypes.ULONG * 10)]

    class JobBasicLimits(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64), ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t), ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD), ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD), ("SchedulingClass", wintypes.DWORD)]

    class JobAccounting(ctypes.Structure):
        _fields_ = [(name, ctypes.c_int64) for name in (
            "TotalUserTime", "TotalKernelTime", "ThisPeriodTotalUserTime", "ThisPeriodTotalKernelTime")]
        _fields_ += [(name, wintypes.DWORD) for name in (
            "TotalPageFaultCount", "TotalProcesses", "ActiveProcesses", "TotalTerminatedProcesses")]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    nt = ctypes.WinDLL("ntdll")
    kernel.GetCurrentProcess.argtypes, kernel.GetCurrentProcess.restype = [], wintypes.HANDLE
    kernel.IsProcessInJob.argtypes = [wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL)]
    kernel.IsProcessInJob.restype = wintypes.BOOL
    kernel.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                                wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    kernel.QueryInformationJobObject.restype = wintypes.BOOL
    nt.NtQueryObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                                wintypes.ULONG, ctypes.POINTER(wintypes.ULONG)]
    nt.NtQueryObject.restype = ctypes.c_long

    class API:
        def access(self, handle):
            info, length = ObjectBasicInformation(), wintypes.ULONG()
            status = nt.NtQueryObject(handle, 0, ctypes.byref(info), ctypes.sizeof(info), ctypes.byref(length))
            if status < 0:
                raise JobAssociationUnknown("job-access-query-failed")
            return info.GrantedAccess

        def limit_flags(self, handle):
            return self.query(handle, 2, JobBasicLimits).LimitFlags

        def is_current_member(self, handle):
            member = wintypes.BOOL()
            if not kernel.IsProcessInJob(kernel.GetCurrentProcess(), handle, ctypes.byref(member)):
                raise JobAssociationUnknown("current-process-job-query-failed")
            return bool(member.value)

        def query(self, handle, information_class, structure):
            value, length = structure(), wintypes.DWORD()
            if not kernel.QueryInformationJobObject(handle, information_class, ctypes.byref(value),
                                                     ctypes.sizeof(value), ctypes.byref(length)):
                raise JobAssociationUnknown("original-job-query-failed")
            return value

        def active_processes(self, handle):
            return self.query(handle, 1, JobAccounting).ActiveProcesses

    return API()


class JobAssociation:
    def __init__(self, handle):
        self.handle = handle
        self._api = _load_api()
        self.require_current_member()

    def require_current_member(self):
        if self._api.access(self.handle) != 0x0004:  # JOB_OBJECT_QUERY only
            raise JobAssociationUnknown("job-handle-is-not-query-only")
        if self._api.limit_flags(self.handle) & (0x2000 | 0x0800 | 0x1000):
            raise JobAssociationUnknown("job-kill-on-close-or-breakaway-limit")
        if not self._api.is_current_member(self.handle):
            raise JobAssociationUnknown("current-process-is-not-in-original-job")

    def require_only_current_process(self):
        self.require_current_member()
        if self._api.active_processes(self.handle) != 1:
            raise JobAssociationUnknown("original-job-has-other-or-unknown-processes")
