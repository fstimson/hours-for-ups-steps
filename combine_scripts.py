combine_scripts.py
import subprocess

# Define the paths to each script
scripts = [
    "calculations_A.py",
    "calculations_B.py",
    "calculations_C.py",
    "calculations_D.py",
    "calculations_a_d_total.py"
]

# Run each script in order
for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run(["python", script], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr)

print("All scripts executed successfully.")
