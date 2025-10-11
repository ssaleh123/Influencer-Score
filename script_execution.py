import subprocess
import os
import time
from supabase import create_client, Client

# Supabase config


supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def log_error(script_name, error_message):
    supabase.table("API_LOG").insert({
        "script_name": script_name,
        "error": error_message
    }).execute()

def run_script(script_name):
    print(f"\nRunning: {script_name}\n{'=' * 40}")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"

    try:
        process = subprocess.Popen(
            ["python", "-u", script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
            encoding='utf-8',
            errors='replace',
            env=env
        )

        for line in process.stdout:
            print(line, end="")

        process.wait()

        stderr_output = process.stderr.read()
        if stderr_output:
            print(f"\nError output from {script_name}:\n{stderr_output}")
            log_error(script_name, stderr_output)

    except Exception as e:
        print(f"\nException running {script_name}: {e}")
        log_error(script_name, str(e))

    print(f"{'=' * 40}\nFinished: {script_name}\n")

post_user_scripts = [
    "Instagram2v7.py",  
    "facebook5v13.py",
    "facebookreelsv2.py",
    "tiktokscraperv21.py",
    "youtubev4.py", 
    "Twitterv10.py"
]

follower_scripts = [
    "FollowerScoreInstagram.py",
    "FollowerScoreFacebook.py",
    "FollowerScoreTiktok.py",
    "FollowerScoreYoutube.py",
    "FollowerScoreTwitter.py"
]

engagement_scripts = [
    "Engagement_Score_Instagramv3.py",
    "Engagement_Score_Facebookv3.py",
    "Engagement_Score_Tiktokv5.py",
    "Engagement_Score_Youtubev3.py",
    "Engagement_Score_Twitterv2.py"
]

google_info_scripts = [
    "googlev6.py",
    "googlev7.py",
    "googlealgorithm.py"
]

score_script = "ScoreTotalv6.py"
niche_script = "Niche_Score.py"

# Loop forever every 24 hours
while True:
    print("\n=== STARTING SCRIPT CYCLE ===\n")
    
    for script in post_user_scripts:
        run_script(script)

    for script in follower_scripts:
        run_script(script)

    for script in engagement_scripts:
        run_script(script)

    for script in google_info_scripts:
        run_script(script)

    run_script(score_script)
    run_script(niche_script)

    print("\n=== SCRIPT CYCLE COMPLETE. Sleeping for 24 hours... ===\n")
    time.sleep(86400)  # Sleep for 24 hours
