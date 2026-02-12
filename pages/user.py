import time
import re

import streamlit as st
import utils.database as database


st.title("User Settings")
st.caption(f"You are logged in with role: {st.session_state.get('role')}.")

authenticator = st.session_state.get("authenticator")
username = st.session_state.get("username")

if not authenticator or not username:
    st.error("Authenticator is not initialized. Please reload the app.")
elif st.session_state.get("authentication_status") is True:
    try:

        # pull current values
        creds = st.session_state.get("credentials")
        if not creds:
            st.warning("Credentials are not available in session; cannot load your profile.")
        else:
            current_username = st.session_state.get("username")
            current_email = creds["usernames"].get(current_username, {}).get("email", "")
            

            with st.form("update_profile_form", border=True):
                st.subheader("User details")

                new_username = st.text_input("Username", value=current_username)
                new_email = st.text_input("Email", value=current_email)
                submitted = st.form_submit_button("Update")

            if submitted:
                # basic email validation (lightweight)
                def is_valid_email(s: str) -> bool:
                    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s))

                if not new_username:
                    st.error("Username cannot be empty.")
                    st.stop()

                if not new_email or not is_valid_email(new_email):
                    st.error("Please provide a valid email address.")
                    st.stop()

                username_changed = new_username != current_username
                email_changed = new_email != current_email

                if not username_changed and not email_changed:
                    st.info("No changes detected.")
                    st.stop()

                # Check username availability if it changed
                if username_changed and new_username in creds["usernames"]:
                    st.error("This username is already taken.")
                    st.stop()

                # --- Update in-memory credentials ---
                if username_changed:
                    # move the whole user record to the new key
                    user_data = creds["usernames"].pop(current_username)
                    user_data["email"] = new_email  # update email in the same move
                    creds["usernames"][new_username] = user_data
                else:
                    # only email changed
                    creds["usernames"][current_username]["email"] = new_email

                # --- Persist to DB (atomic) ---
                try:
                    if username_changed:
                        # one-shot profile update that writes both fields
                        affected = database.update_user_profile(
                            old_username=current_username,
                            new_username=new_username,
                            new_email=new_email,
                        )
                        if affected == 0:
                            st.error("Failed to update the profile in the database.")
                            st.stop()
                        # update session username
                        st.session_state["username"] = new_username
                    else:
                        # only email changed
                        affected = database.update_email(
                            username=current_username,
                            email=new_email,
                        )
                        if affected == 0:
                            st.error("Failed to update the email in the database.")
                            st.stop()
                except Exception as e:
                    st.error(f"Database error: {e}")
                    st.stop()

                # --- UX feedback & auth handling ---
                if username_changed:
                    st.success(f"Profile updated. Username changed to '{new_username}'.")
                    # show countdown BEFORE logout, so the UI renders
                    countdown_seconds = 5
                    progress = st.progress(0)
                    status_text = st.empty()
                    for i in range(countdown_seconds, 0, -1):
                        progress.progress((countdown_seconds - i + 1) / countdown_seconds)
                        status_text.info(f"Please log in again in {i} second{'s' if i > 1 else ''}...")
                        time.sleep(1)

                    # now clear cookie and force a clean login
                    try:
                        authenticator.logout(location="unrendered")
                    except Exception:
                        pass
                    st.session_state["authentication_status"] = None
                    st.rerun()
                else:
                    st.success("Profile updated.")

        st.write("<br>", unsafe_allow_html=True)
        
        # open reset passw form
        if authenticator.reset_password(username):
            # reset_password updates the credentials dictionary passed during creation
            creds = st.session_state.get("credentials")
            if not creds:
                st.warning("Credentials are not available in session; cannot persist the new password.")
            else:
                # the new password is already hashed inside the dictionary
                new_hash = creds["usernames"][username]["password"]
                database.update_user_password(username=username, password=new_hash)
                st.success("Password successfully changed.")

    except Exception as e:
        st.error(e)
else:
    st.info("You must be logged in to manage settings.")
