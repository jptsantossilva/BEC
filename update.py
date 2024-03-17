import os
import zipfile
from datetime import datetime
import requests
import shutil
import subprocess

def create_backup():
    # Get the current folder path and folder name
    current_folder = os.getcwd()
    folder_name = os.path.basename(current_folder)

    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now().strftime("%Y%m%d")

    # Create the backup folder path
    backup_folder = os.path.join(os.path.dirname(current_folder), "BEC_backups")

    # Create the backup folder if it doesn't exist
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    # Create a zip file for the backup
    backup_file_name = os.path.join(backup_folder, f"{current_date}_{folder_name}.zip")
    with zipfile.ZipFile(backup_file_name, "w") as backup_zip:
        # Add all files from the current folder to the zip file
        for root, _, files in os.walk(current_folder):
            for file in files:
                file_path = os.path.join(root, file)
                # Exclude the backup file itself
                if file_path != backup_file_name:
                    # Add the file to the zip file
                    backup_zip.write(file_path, os.path.relpath(file_path, current_folder))

    msg = "Backup created successfully."
    print(msg)
    return msg

def delete_files():
    current_folder = os.getcwd()

    # Folders to exclude from deletion and their subfolders
    folders_to_keep = ["static"]

    # Files to keep
    files_to_keep = ["config.yaml", "data.db", "update.py"]

    for root, dirs, files in os.walk(current_folder, topdown=True):
        # Exclude specific folders and their contents from deletion
        if os.path.basename(root) in folders_to_keep:
            dirs[:] = [d for d in dirs if d in folders_to_keep]  # Only keep specified subfolders
            continue

        # Delete files not in files_to_keep
        for file in files:
            file_path = os.path.join(root, file)
            if file not in files_to_keep:
                os.remove(file_path)
                print(f"Deleted file: {file_path}")

        # Delete directories not in folders_to_keep
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if dir_name not in folders_to_keep:
                shutil.rmtree(dir_path)
                print(f"Deleted folder: {dir_path}")

    msg = "Deleted unnecessary files and folders."
    print(msg)
    return msg

def rename_config_file():
    current_folder = os.getcwd()
    config_file_path = os.path.join(current_folder, "config.yaml")
    renamed_config_file_path = os.path.join(current_folder, "config_old.yaml")

    if os.path.exists(config_file_path):
        os.rename(config_file_path, renamed_config_file_path)
        msg = "Configurations backup created successfully."
    else:
        msg = "No 'config.yaml' file found. Skipping rename process."
    
    print(msg)
    return msg

def download_files_from_github():
    github_url = "https://github.com/jptsantossilva/BEC/archive/main.zip"
    response = requests.get(github_url)
    if response.status_code == 200:
        current_folder = os.getcwd()
        zip_file_path = os.path.join(current_folder, "bec_temp.zip")

        with open(zip_file_path, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(current_folder)

        # Find the extracted folder name (e.g., "BEC-main") and copy its contents to the current folder
        # extracted_folder = os.path.join(current_folder, os.listdir(current_folder)[1])
        extracted_folder = os.path.join(current_folder, "BEC-main")
        for item in os.listdir(extracted_folder):
            item_path = os.path.join(extracted_folder, item)
            destination_path = os.path.join(current_folder, item)
            if item != "static":  # Skip the "static" folder
                if os.path.isfile(item_path):
                    shutil.move(item_path, destination_path)
                elif os.path.isdir(item_path):
                    shutil.move(item_path, destination_path)

        # Clean up: remove the downloaded zip file and the extracted folder
        os.remove(zip_file_path)
        shutil.rmtree(extracted_folder)

        msg = "New app version downloaded successfully."
    else:
        msg = "Failed to download files from GitHub."

    print(msg)
    return msg

def copy_contents_to_config():
    current_folder = os.getcwd()
    config_file_path = os.path.join(current_folder, "config.yaml")
    renamed_config_file_path = os.path.join(current_folder, "config_old.yaml")

    if os.path.exists(renamed_config_file_path):
        with open(renamed_config_file_path, "r") as old_file:
            config_content = old_file.read()
        with open(config_file_path, "w") as new_file:
            new_file.write(config_content)
        msg = "Configurations restored successfully."
    else:
        msg = "No 'config_old.yaml' file found. Skipping copy process."

    print(msg)
    return msg

def install_packages_from_requirements():
    current_folder = os.getcwd()
    requirements_file = os.path.join(current_folder, "requirements.txt")

    if os.path.exists(requirements_file):
        try:
            subprocess.check_call(["pip", "install", "-r", requirements_file])
            msg = "Python packages installed."
        except subprocess.CalledProcessError:
            msg = "Failed to install Python packages from requirements.txt."
    else:
        msg = "'requirements.txt' file not found. Skipping installation."

    print(msg)
    return msg

def main():
    msg1 = create_backup()
    msg2 = delete_files()
    msg3 = rename_config_file()
    msg4 = download_files_from_github()
    msg5 = copy_contents_to_config()
    msg6 = install_packages_from_requirements()

    msg7 = "Update finished! Yeahhh 🎉"

    # List of messages to combine
    messages = [
        msg1, msg2, msg3, msg4, msg5, msg6, msg7
    ]

    # Combine all messages into a single message with newline separators
    combined_message = "\n".join(msg for msg in messages if msg is not None)

    # print(combined_message)
    return combined_message
    
if __name__ == "__main__":
    main()