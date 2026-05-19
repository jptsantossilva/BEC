import bec.jobs_runner as jobs_runner


def test_resolve_schedule_script_keeps_existing_root_wrapper():
    assert jobs_runner._resolve_schedule_script("main.py") == "main.py"


def test_resolve_schedule_script_supports_legacy_signals_path():
    assert jobs_runner._resolve_schedule_script("signals/super_rsi.py") == "bec/signals/super_rsi.py"


def test_resolve_schedule_script_keeps_unknown_path_for_subprocess_error():
    assert jobs_runner._resolve_schedule_script("missing/script.py") == "missing/script.py"
