import streamlit as st
from crontab import CronTab
import getpass

# --- SETTINGS ---
CRON_COMMAND = "cd ~/BEC && python3 delisting_checker.py"
CRON_SCHEDULE = "0 * * * *"  # every hour
FULL_LINE = f"{CRON_SCHEDULE} {CRON_COMMAND}"

def get_user_crontab():
    username = getpass.getuser()
    return CronTab(user=username)

def line_matches_command(line):
    return CRON_COMMAND in line

def enable_cron_line():
    cron = get_user_crontab()
    found = False
    for job in cron:
        if CRON_COMMAND in job.command:
            job.enable(True)
            found = True
    if not found:
        # Add new job
        job = cron.new(command=CRON_COMMAND)
        job.setall(CRON_SCHEDULE)
        job.enable(True)
    cron.write()
    return True

def disable_cron_line():
    cron = get_user_crontab()
    for job in cron:
        if CRON_COMMAND in job.command:
            job.enable(False)
    cron.write()
    return True

def is_cron_enabled():
    cron = get_user_crontab()
    for job in cron:
        if CRON_COMMAND in job.command:
            return job.is_enabled()
    return False

# --- STREAMLIT UI ---
st.title("⚙️ Delisting Checker Cron Control")

enabled = st.checkbox("Enable hourly delisting checker", value=is_cron_enabled())

if enabled:
    enable_cron_line()
    st.success("✅ Cron job enabled.")
else:
    disable_cron_line()
    st.warning("⛔ Cron job disabled (commented out).")

# Debug: show matching jobs
with st.expander("🛠️ View cron entries containing 'delisting_checker.py'"):
    cron = get_user_crontab()
    for job in cron:
        if "delisting_checker.py" in job.command:
            status = "✅ Enabled" if job.is_enabled() else "🛑 Disabled"
            st.text(f"{job}   →   {status}")
