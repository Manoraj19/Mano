
# from cryptography.fernet import Fernet
# import os

# key = Fernet.generate_key()
# cipher_suite = Fernet(key)


# def encrypt_credential(username, password):
#     encrypted_username = cipher_suite.encrypt(username.encode()).decode()
#     encrypted_password = cipher_suite.encrypt(password.encode()).decode()
#     return encrypted_username, encrypted_password

# num_accounts = int(input("Enter the number of LinkedIn accounts: "))
# base_dir    = os.path.dirname(os.path.realpath(__file__))
# env_file_path    = os.path.join(base_dir, '.env')

# key = Fernet.generate_key()
# cipher_suite = Fernet(key)

# if os.path.exists(env_file_path):
#     with open(env_file_path, 'r') as env_file:
#         current_env_content = env_file.read()
# else:
#     current_env_content = ""

# if "SECRET_KEY" in current_env_content:

#     current_env_content = current_env_content.replace(
#         f"SECRET_KEY={current_env_content.split('SECRET_KEY=')[1].splitlines()[0]}",
#         f"SECRET_KEY={key.decode()}"
#     )
# else:

#     current_env_content += f"SECRET_KEY={key.decode()}\n"

# with open(env_file_path, 'w') as env_file:
#     env_file.write(current_env_content) 

# with open(env_file_path, 'a') as env_file:
#     for i in range(1, num_accounts + 1):
#         username = input(f"Enter username for account {i}: ")
#         password = input(f"Enter password for account {i}: ")

#         encrypted_username, encrypted_password = encrypt_credential(username, password)
#         env_file.write(f"LINKEDIN_USERNAME_ACCOUNT{i}={encrypted_username}\n")
#         env_file.write(f"LINKEDIN_PASSWORD_ACCOUNT{i}={encrypted_password}\n")
# print("Writing .env to:", env_file)

# print(f"Encrypted credentials for {num_accounts} accounts saved to .env file.")


from cryptography.fernet import Fernet
import os

def encrypt_credential(cipher_suite, text):
    return cipher_suite.encrypt(text.encode()).decode()

def get_input(prompt, valid=None):
    while True:
        val = input(prompt).strip()
        if not valid or val.lower() in valid:
            return val.lower()
        print(f"Enter one of: {valid}")

def update_env_file(env_lines, key, value):
    # Overwrite or add the key
    found = False
    for i, line in enumerate(env_lines):
        if line.startswith(f"{key}="):
            env_lines[i] = f"{key}={value}\n"
            found = True
            break
    if not found:
        env_lines.append(f"{key}={value}\n")
    return env_lines

base_dir = os.path.dirname(os.path.realpath(__file__))
env_file_path = os.path.join(base_dir, '.env')

# Load or create env lines
if os.path.exists(env_file_path):
    with open(env_file_path, 'r') as f:
        env_lines = f.readlines()
else:
    env_lines = []

# Read current SECRET_KEY or generate new
existing_key = None
for line in env_lines:
    if line.startswith("SECRET_KEY="):
        existing_key = line.split("=", 1)[1].strip()
        break
if not existing_key:
    key = Fernet.generate_key()
    env_lines = update_env_file(env_lines, "SECRET_KEY", key.decode())
else:
    key = existing_key.encode()
cipher_suite = Fernet(key)

# ----- Clay.run credentials -----
clayrun_email_key = "CLAYRUN_EMAIL"
clayrun_pass_key = "CLAYRUN_PASSWORD"
clayrun_exists = any(l.startswith(clayrun_email_key + "=") for l in env_lines)
if clayrun_exists:
    overwrite = get_input("Clay.run credentials found in .env. Replace? (y/n): ", {"y", "n"})
    if overwrite == "y":
        # Remove old Clay.run creds
        env_lines = [l for l in env_lines if not (l.startswith(clayrun_email_key) or l.startswith(clayrun_pass_key))]
    else:
        print("Keeping existing Clay.run credentials.")
else:
    overwrite = "y"

if overwrite == "y":
    clayrun_email = input("Enter Clay.run email: ").strip()
    clayrun_pass = input("Enter Clay.run password: ").strip()
    enc_email = encrypt_credential(cipher_suite, clayrun_email)
    enc_pass = encrypt_credential(cipher_suite, clayrun_pass)
    env_lines = update_env_file(env_lines, clayrun_email_key, enc_email)
    env_lines = update_env_file(env_lines, clayrun_pass_key, enc_pass)

# ----- LinkedIn Accounts -----
# Remove all existing LinkedIn account entries if replacing
linkedin_keys = [l for l in env_lines if l.startswith("LINKEDIN_USERNAME_ACCOUNT") or l.startswith("LINKEDIN_PASSWORD_ACCOUNT")]
if linkedin_keys:
    overwrite = get_input(f"{len(linkedin_keys)//2} LinkedIn account(s) found in .env. Replace all? (y/n): ", {"y", "n"})
    if overwrite == "y":
        env_lines = [l for l in env_lines if not (l.startswith("LINKEDIN_USERNAME_ACCOUNT") or l.startswith("LINKEDIN_PASSWORD_ACCOUNT"))]
    else:
        print("Keeping existing LinkedIn credentials.")
        overwrite = "n"
else:
    overwrite = "y"

if overwrite == "y":
    account_count = 0
    while True:
        add_acc = get_input("Add a LinkedIn account? (y/exit): ", {"y", "exit"})
        if add_acc == "exit":
            break
        account_count += 1
        username = input(f"Enter username for LinkedIn account {account_count}: ").strip()
        password = input(f"Enter password for LinkedIn account {account_count}: ").strip()
        enc_username = encrypt_credential(cipher_suite, username)
        enc_password = encrypt_credential(cipher_suite, password)
        env_lines = update_env_file(env_lines, f"LINKEDIN_USERNAME_ACCOUNT{account_count}", enc_username)
        env_lines = update_env_file(env_lines, f"LINKEDIN_PASSWORD_ACCOUNT{account_count}", enc_password)
    print(f"Added {account_count} LinkedIn account(s).")

# ----- Write to .env file -----
with open(env_file_path, 'w') as f:
    f.writelines(env_lines)

print("\n.env file updated! (Clay.run & LinkedIn creds changed if chosen.)")
