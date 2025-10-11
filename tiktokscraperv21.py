#last 20 days
import requests
import time
from datetime import datetime, timedelta, UTC
from supabase import create_client, Client

# API Credentials
API_URL_VIDEOS = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/getUserVideos"
API_URL_USER = "https://tiktok-api15.p.rapidapi.com/index/Tiktok/getUserInfo"


# Supabase Credentials

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
'''
# Function to fetch all influencers from the INFLUENCER table
def get_influencers():
    response = supabase.table("INFLUENCER").select("Influencer_id, Tiktok_Username").order("Influencer_id").execute()
    return response.data if response.data else []
'''

def get_influencers():
    response = supabase.table("INFLUENCER")\
        .select("Influencer_id, Tiktok_Username")\
        .gte("Influencer_id", 32)\
        .order("Influencer_id")\
        .execute()
    return response.data

# Function to fetch follower count
def get_follower_count(username):
    """Fetches the follower count of a given TikTok user."""
    try:
        response = requests.get(API_URL_USER, headers=HEADERS, params={"unique_id": username})
        data = response.json()
        return data.get("data", {}).get("stats", {}).get("followerCount", None)  
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Error fetching follower count for {username}: {e}")
        return 0

# Function to fetch TikTok posts within the last 5 days
def get_latest_videos(username):
    videos = []
    cursor = 0  
    timeframe = datetime.now(UTC) - timedelta(days=20)

    while True:
        try:
            params = {"unique_id": username, "count": 35, "cursor": cursor}
            response = requests.get(API_URL_VIDEOS, headers=HEADERS, params=params)
            data = response.json()

            new_videos = data.get("data", {}).get("videos", [])
            if not new_videos:
                break  

            for video in new_videos:
                create_time = datetime.fromtimestamp(video.get("create_time", None), UTC)
                
                # Only keep videos within the last 5 days
                if create_time >= timeframe:
                    videos.append(video)
                else:
                    break  # Stop if we encounter an old video

            cursor = data["data"].get("cursor", None)
            if cursor is None:
                break  

            time.sleep(1)  

        except (requests.exceptions.RequestException, KeyError) as e:
            print(f"Error fetching videos for {username}: {e}")
            break  

    return videos 

'''# Function to delete posts older than 15 days
def delete_old_posts():
    fifteen_days_ago = (datetime.now(UTC) - timedelta(days=15)).date().isoformat()
    try:
        response = supabase.table("TIKTOK_POST_METRICS").delete().lt("create_time", fifteen_days_ago).execute()
        print(f"Deleted old posts older than {fifteen_days_ago}.")
    except Exception as e:
        print(f"Error deleting old posts: {e}")
'''
# Fetch all influencers
influencers = get_influencers()

if not influencers:
    print("No influencers found.")
else:
    for influencer in influencers:
        influencer_id = influencer["Influencer_id"]
        username = influencer["Tiktok_Username"]

        print(f"Fetching follower count for {username} (Influencer ID: {influencer_id})")
        follower_count = get_follower_count(username)

        # Upsert follower count into TIKTOK_USER_METRICS
        user_metrics_data = {
            "Influencer_id": influencer_id,
            "follower_count": follower_count,
        }
        try:
            supabase.table("TIKTOK_USER_METRICS").upsert(user_metrics_data, on_conflict=["Influencer_id"]).execute()
            print(f"Updated follower count for {username}: {follower_count}")
        except Exception as e:
            print(f"Failed to update follower count for {username}: {e}")

        print(f"Fetching latest post metrics for {username}...")
        videos = get_latest_videos(username)

        if not videos:
            print(f"No recent posts found for {username}.")
            continue

        # Prepare data for insertion
        upsert_data = []
        for video in videos:
            video_id = video.get("video_id", "N/A")
            digg_count = video.get("digg_count", 0)
            play_count = video.get("play_count", 0)
            comment_count = video.get("comment_count", 0)
            share_count = video.get("share_count", 0)
            collect_count = video.get("collect_count", 0)
            create_time = video.get("create_time", None)

            create_time = datetime.fromtimestamp(create_time, UTC).date().isoformat()

            video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"

            upsert_data.append({
                "Influencer_id": influencer_id,
                "video_id": video_id,
                "like_count": digg_count,
                "play_count": play_count,
                "comment_count": comment_count,
                "share_count": share_count,
                "collect_count": collect_count,
                "create_time": create_time,
                "url": video_url
            })

        if upsert_data:
            try:
                supabase.table("TIKTOK_POST_METRICS").upsert(upsert_data, on_conflict=["video_id"]).execute()
                print(f"Upserted {len(upsert_data)} posts for {username}.")
            except Exception as e:
                print(f"Failed to upsert posts for {username}: {e}")

# Delete old posts after inserting new ones
# delete_old_posts()
