import os
import subprocess
import time
import argparse

def should_update_local(local_commit, remote_commit):
    return local_commit != remote_commit

def run_auto_updater():
    print("Starting auto-updater...")
    print("First i'll run the docker containers...")
    print("Checking for .vali.env file...")
    while not os.path.exists('.vali.env'):
        print(".vali.env file not found. Waiting 10 seconds before checking again...")
        time.sleep(10)
    print(".vali.env file found. Proceeding with docker setup...")
    launch_command = "./utils/launch_validator.sh"
    os.system(launch_command)
    time.sleep(60)

    while True:
        print("Checking for updates...")
        current_branch = subprocess.getoutput("git rev-parse --abbrev-ref HEAD")
        local_commit = subprocess.getoutput("git rev-parse HEAD")
        os.system("git fetch")
        remote_commit = subprocess.getoutput(f"git rev-parse origin/{current_branch}")

        if should_update_local(local_commit, remote_commit):
            print("Local repo is not up-to-date. Updating...")
            reset_cmd = f"git reset --hard {remote_commit}"
            process = subprocess.Popen(reset_cmd.split(), stdout=subprocess.PIPE)
            output, error = process.communicate()

            if error:
                print("Error in updating:", error)
            else:
                print("Updated local repo to latest version:", remote_commit)

                print("Running the autoupdate steps...")
                os.system("./utils/autoupdate_validator_steps.sh")
                time.sleep(20)

                print("Finished running the autoupdate steps! Ready to go 😎")

        else:
            print("Repo is up-to-date.")

        time.sleep(60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run auto updates for a validator")

    args = parser.parse_args()

    run_auto_updater()