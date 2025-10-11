import requests
import time
from datetime import datetime, timedelta, UTC
from supabase import create_client, Client

# Supabase configuration


supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# API headers and endpoint

USER_ID_API = "https://instagram-scrapper-posts-reels-stories-downloader.p.rapidapi.com/user_id_by_username"
POSTS_API = "https://instagram-scrapper-posts-reels-stories-downloader.p.rapidapi.com/posts_by_user_id"

def get_user_id(username):
    params = {"username": username}
    response = requests.get(USER_ID_API, headers=HEADERS, params=params)
    time.sleep(2)
    if response.status_code == 200:
        return response.json().get("UserID")
    else:
        print(f"⚠️ Failed to fetch user ID for {username}: {response.text}")
        return None
time.sleep(3)
    
def get_follower_count(user_id):
    url = "https://instagram-scrapper-posts-reels-stories-downloader.p.rapidapi.com/profile_by_user_id"
    params = {"user_id": user_id}
    
    for attempt in range(4):  # 4 attempts max
        response = requests.get(url, headers=HEADERS, params=params)
        time.sleep(2)
        if response.status_code == 200:
            follower_count = response.json().get("follower_count")
            if follower_count is not None:
                return follower_count
            else:
                print(f"⚠️ Follower count not found in response for user_id {user_id}.")
        else:
            print(f"⚠️ Failed to fetch follower count for user_id {user_id} (Attempt {attempt + 1}/4): {response.text}")
        time.sleep(2)  # delay before retry
    
    print(f"⚠️ All attempts failed. Returning 0 for follower count for user_id {user_id}.")
    return 0


time.sleep(3)

def fetch_all_posts(user_id, influencer_id):
    all_posts = []
    next_max_id = ""
    page = 1
    cutoff_date = datetime.now(UTC) - timedelta(days=20)

    # ✅ Fetch and upsert follower count before entering loop
    follower_count = get_follower_count(user_id)
    if follower_count is not None:
        print(f"👥 Follower Count: {follower_count}")
        supabase.table("INSTAGRAM_USER_METRICS").upsert({
            "Influencer_id": influencer_id,
            "follower_count": follower_count
        }, on_conflict=["Influencer_id"]).execute()

    while True:
        params = {"user_id": user_id}
        if next_max_id:
            params["next_max_id"] = next_max_id

        print(f"\n📤 Fetching page {page} for user_id {user_id}...")
        response = requests.get(POSTS_API, headers=HEADERS, params=params)
        time.sleep(2)
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code} - {response.text}")
            break

        json_data = response.json()
        posts = json_data.get("items", [])
        if not posts:
            print("⛔ No posts found.")
            break

        i = 0
        while i < len(posts):
            post = posts[i]
            taken_at = post.get("taken_at")
            if not taken_at:
                i += 1
                continue

            post_datetime = datetime.fromtimestamp(taken_at, UTC)

            if post_datetime < cutoff_date:
                print(f"🛑 Post at index {i} is older than 20 days.")

                # Look ahead to next 4 posts
                lookahead_window = posts[i+1:i+5]
                has_recent = False

                for future_post in lookahead_window:
                    future_taken_at = future_post.get("taken_at")
                    if not future_taken_at:
                        continue
                    future_post_datetime = datetime.fromtimestamp(future_taken_at, UTC)
                    if future_post_datetime >= cutoff_date:
                        has_recent = True
                        break

                if not has_recent:
                    print("🛑 Next 4 posts are also older. Stopping for this influencer.")
                    return all_posts
                else:
                    print("⏭️ Some upcoming posts are recent. Continuing...")
                    i += 1
                    continue

            # Extract post info
            like_count = post.get("like_count")
            play_count = post.get("play_count", None)
            full_post_id = post.get("id", "")
            post_id = full_post_id.split("_")[0] if "_" in full_post_id else full_post_id
            post_date = post_datetime.strftime("%Y-%m-%d")
            comment_count = post.get("comment_count")
            reshare_count = post.get("reshare_count")
            code = post.get("code", "")
            post_url = f"https://instagram.com/p/{code}" if code else "N/A"

            print(f"▶️ like_count: {like_count}, play_count: {play_count}, post_id: {post_id}, post_date: {post_date}, comment_count: {comment_count}, reshare_count: {reshare_count}, url: {post_url}")

            # Upsert into INSTAGRAM_POST_METRICS
            supabase.table("INSTAGRAM_POST_METRICS").upsert({
                "Influencer_id": influencer_id,
                "post_id": post_id,
                "like_count": like_count,
                "play_count": play_count,
                "post_date": post_date,
                "comment_count": comment_count,
                "reshare_count": reshare_count,
                "post_url": post_url
            }, on_conflict=["post_id"]).execute()

            all_posts.append(post)
            i += 1

        next_max_id = json_data.get("next_max_id")
        if not next_max_id:
            print("⛔ No more next_max_id found. Done fetching.")
            break

        page += 1
        time.sleep(2)

    return all_posts



# Get first 3 influencers
#response = supabase.table("INFLUENCER").select("Influencer_id, Instagram_Username").order("Influencer_id").execute()
response = supabase.table("INFLUENCER")\
    .select("Influencer_id, Instagram_Username")\
    .gte("Influencer_id", 1)\
    .order("Influencer_id")\
    .execute()

influencers = response.data

for influencer in influencers:
    influencer_id = influencer["Influencer_id"]
    username = influencer["Instagram_Username"]

    print(f"\n🔍 Fetching user_id for @{username} (Influencer ID: {influencer_id})...")
    user_id = get_user_id(username)

    time.sleep(5)

    if user_id:
        print(f"✅ Found user_id: {user_id}. Fetching posts...")
        posts = fetch_all_posts(user_id, influencer_id)
        print(f"🎉 Total posts fetched and upserted for @{username}: {len(posts)}")
    else:
        print(f"❌ Skipping @{username} due to missing user_id.")

    time.sleep(3)


