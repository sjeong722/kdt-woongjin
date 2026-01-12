import os
import sys
import re

dags_root = r'c:\Users\Comet\Documents\GitHub\kdt-woongjin\dags'
failed = False

if not os.path.exists(dags_root):
    print("dags directory not found")
    sys.exit(0)

for nickname in os.listdir(dags_root):
    nickname_path = os.path.join(dags_root, nickname)
    if not os.path.isdir(nickname_path) or nickname.startswith('.') or nickname == '__pycache__':
        continue

    print(f"Checking DAGs for nickname: {nickname}")

    for root, dirs, files in os.walk(nickname_path):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    matches = re.finditer(r'dag_id\s*=\s*([\'\"])(.*?)\1', content)

                    for match in matches:
                        dag_id_val = match.group(2)
                        if nickname not in dag_id_val:
                            print(f"ERROR: file={file_path} :: Invalid dag_id '{dag_id_val}'. It must contain the nickname '{nickname}'.")
                            failed = True
                except Exception as e:
                    print(f"WARNING: Could not check {file_path}: {e}")

if failed:
    print("Validation FAILED")
else:
    print("Validation PASSED")
