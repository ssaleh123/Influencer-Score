import requests
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# Supabase credentials

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# YouTube API setup


# Date setup
today = datetime.now(timezone.utc)
timeframe = today - timedelta(days=20)
published_after = timeframe.isoformat()
'''
def get_influencers():
    response = supabase.table("INFLUENCER").select("Influencer_id, Youtube_Username").neq("Youtube_Username", None).limit(3).execute()
    return response.data if response.data else []
'''
def get_influencers():
    response = supabase.table("INFLUENCER")\
        .select("Influencer_id, Youtube_Username")\
        .gte("Influencer_id", 1)\
        .order("Influencer_id")\
        .execute()
    return response.data

def fetch_youtube_videos(channel_id):
    if not channel_id:
        return []

    url = f"{BASE_URL}search"
    params = {
        "part": "snippet",
        "channelId": channel_id,
        "maxResults": 10,
        "order": "date",
        "publishedAfter": published_after,
        "type": "video",
        "key": API_KEY
    }
    response = requests.get(url, params=params)
    return response.json().get("items", []) if response.status_code == 200 else []

def get_video_stats(video_id):
    url = f"{BASE_URL}videos"
    params = {"part": "statistics", "id": video_id, "key": API_KEY}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        stats = response.json().get("items", [{}])[0].get("statistics", {})
        return {
            "Like_Count": int(stats.get("likeCount", 0) or 0),  
            "Comment_Count": int(stats.get("commentCount", 0) or 0),
            "View_Count": int(stats.get("viewCount", 0) or 0)
        }
    return {"Like_Count": 0, "Comment_Count": 0, "View_Count": 0}

def get_subscriber_count(channel_id):
    """Fetch real-time subscriber count using RapidAPI based on resolved @username."""
    # Step 1: Resolve custom username (handle) from YouTube API
    url = f"{BASE_URL}channels"
    params = {
        "part": "snippet",
        "id": channel_id,
        "key": API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"❌ Failed to fetch username for {channel_id}: {response.status_code} - {response.text}")
        return 0

    items = response.json().get("items", [])
    if not items:
        print(f"❌ No items found for channel ID {channel_id}")
        return 0

    custom_url = items[0]["snippet"].get("customUrl")
    if not custom_url:
        print(f"❌ No custom URL found for channel ID {channel_id}")
        return 0

    # Step 2: Use resolved username to get subscriber count from RapidAPI
    rapidapi_url = "https://youtube-real-time-api.p.rapidapi.com/subscriberCount"
    querystring = {"link": f"https://www.youtube.com/{custom_url}"}
    headers = {
     
    }

    try:
        sub_response = requests.get(rapidapi_url, headers=headers, params=querystring)
        sub_response.raise_for_status()
        data = sub_response.json()
        if data.get("status") and "subscriberCount" in data["data"]:
            return int(data["data"]["subscriberCount"])
        else:
            print(f"⚠️ Unexpected response from subscriber API for {custom_url}: {data}")
            return 0
    except Exception as e:
        print(f"❌ Error fetching subscriber count from RapidAPI for {custom_url}: {e}")
        return 0

def upsert_subscriber_count(influencer_id, subscriber_count):
    """Upsert subscriber count into YOUTUBE_USER_METRICS table."""
    data = {
        "Influencer_id": influencer_id,
        "follower_count": subscriber_count
    }
    try:
        response = supabase.table("YOUTUBE_USER_METRICS").upsert(data, on_conflict=["Influencer_id"]).execute()
        print(f"✅ Upserted subscriber count for Influencer {influencer_id}: {subscriber_count}")
    except Exception as e:
        print(f"❌ Error upserting subscriber count for Influencer {influencer_id}: {e}")

def insert_videos(videos, influencer_id):
    entries = []
    for video in videos:
        video_id = str(video["id"]["videoId"])
        snippet = video["snippet"]
        stats = get_video_stats(video_id)

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        entry = {
            "Influencer_id": int(influencer_id),
            "Video_id": video_id,
            "Video_url": video_url,
            "Publish_Date": str(snippet["publishedAt"][:10]),
            "Like_Count": stats["Like_Count"],
            "Comment_Count": stats["Comment_Count"],
            "View_Count": stats["View_Count"]
        }

        entries.append(entry)

    print("📹 Inserting video entries:", entries)

    if entries:
        try:
            response = supabase.table("YOUTUBE_POST_METRICS").upsert(entries, on_conflict=["Video_id"]).execute()
            print("🗂️ Video insert response:", response)
        except Exception as e:
            print("❌ Error inserting video data:", e)

def main():
    influencers = get_influencers()
    for influencer in influencers:
        influencer_id = influencer["Influencer_id"]
        channel_id = influencer["Youtube_Username"]

        # 🔄 Fetch and upsert subscriber count
        subscriber_count = get_subscriber_count(channel_id)
        upsert_subscriber_count(influencer_id, subscriber_count)

        # 🎥 Fetch and insert recent videos
        videos = fetch_youtube_videos(channel_id)
        if videos:
            insert_videos(videos, influencer_id)

if __name__ == "__main__":
    main()
