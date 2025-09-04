import os
import subprocess
from pathlib import Path


def fuzzy_find_dat_file(start_dir=None):
    start_dir = start_dir or str(Path.home())

    # Build the 'find' command to list all .dat files
    find_cmd = ["find", start_dir, "-type", "f", "-name", "*.dat"]

    try:
        # Pipe the output of find to fzf
        find_proc = subprocess.Popen(find_cmd, stdout=subprocess.PIPE)
        fzf_proc = subprocess.run(
            ["fzf", "--prompt", "Select a .dat file: "],
            stdin=find_proc.stdout,
            text=True,
            stdout=subprocess.PIPE
        )
        find_proc.stdout.close()  # Allow find to receive SIGPIPE

        selected = fzf_proc.stdout.strip()
        if selected:
            return selected
        else:
            print("❌ No file selected.")
            return None

    except FileNotFoundError as e:
        print("❌ Required binary not found:", e)
        return None


# 🧪 Example usage
if __name__ == "__main__":
    selected_file = fuzzy_find_dat_file()
    if selected_file:
        print(selected_file)
