import subprocess

def run_training_script():
    try:
        subprocess.run(['python', 'Automation_Training_data.py'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running training script: {e}")

def run_testing_script():
    try:
        subprocess.run(['python', 'Automation_Testing_data.py'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running testing script: {e}")

if __name__ == "__main__":
    print("Running Automation Training script...")
    run_training_script()
    
    print("\nRunning Automation Testing script...")
    run_testing_script()
