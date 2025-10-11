import requests
import time
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# Supabase info


# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Define the API headers


BASE_URL = "https://facebook-scraper3.p.rapidapi.com/page/posts"

def get_influencers():
    response = supabase.table("INFLUENCER")\
        .select("Influencer_id, Facebook_Username")\
        .gte("Influencer_id", 3)\
        .order("Influencer_id")\
        .execute()
    return response.data

def upsert_follower_count(influencer_id, follower_count):
    response = supabase.table("FACEBOOK_USER_METRICS").upsert(
        {"Influencer_id": influencer_id, "follower_count": follower_count},
        on_conflict=["Influencer_id"]
    ).execute()
    return response

def upsert_post_metrics(post_data):
    response = supabase.table("FACEBOOK_POST_METRICS").upsert(
        post_data,
        on_conflict=["post_id"]
    ).execute()
    return response

def get_page_id(facebook_url):
    page_url = f'https://facebook-scraper3.p.rapidapi.com/page/page_id?url={facebook_url}'
    try:
        response = requests.get(page_url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            page_id = data.get("page_id")
            if not page_id:
                print(f"API returned 200 but no page_id for {facebook_url}. Full response: {data}")
            return page_id
        else:
            print(f"Failed to get page ID for {facebook_url}. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Exception occurred while fetching page ID for {facebook_url}: {str(e)}")
    return None

def get_play_count(post_id):
    url = f"https://facebook-scraper3.p.rapidapi.com/post?post_id={post_id}"
    response = requests.get(url, headers=HEADERS)
    return response.json().get("results", {}).get("play_count", None) if response.status_code == 200 else None

def get_follower_count(facebook_username, retries=3, delay=3):
    url = f"https://facebook-scraper3.p.rapidapi.com/page/details?url=https%3A%2F%2Fwww.facebook.com%2F{facebook_username}"
    for attempt in range(retries + 1):
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            followers = data.get("results", {}).get("followers")
            if isinstance(followers, int):
                return followers
        print(f"Attempt {attempt + 1} failed to get valid follower count for {facebook_username}")
        time.sleep(delay)
    return 0  # Default to 0 after all retries fail

def fetch_all_facebook_posts(page_id, start_date, end_date, influencer_id, delay=1):
    all_posts = []
    cursor = None

    while True:
        params = {
            "page_id": page_id,
            "start_date": start_date,
            "end_date": end_date
        }
        if cursor:
            params["cursor"] = cursor

        try:
            response = requests.get(BASE_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get("results")
            if not results:
                print("[INFO] No posts found. Ending fetch.")
                break

            # Process posts
            for post in results:
                post_id = post.get("post_id")
                post_url = post.get("url", "No URL available")
                comments_count = post.get("comments_count", 0)
                like_count = post.get("reactions", {}).get("like", 0)
                reshare_count = post.get("reshare_count", 0)
                play_count = get_play_count(post_id)
                publish_timestamp = post.get("timestamp")
                publish_time = (
                    datetime.fromtimestamp(publish_timestamp, timezone.utc).strftime("%Y-%m-%d")
                    if publish_timestamp else None
                )

                print(f"\nPost ID: {post_id}")
                print(f"URL: {post_url}")
                print(f"Publish Time: {publish_time}")
                print(f"Like Count: {like_count}")
                print(f"Comments Count: {comments_count}")
                print(f"Reshare Count: {reshare_count}")
                print(f"Play Count: {play_count}")

                post_data = {
                    "post_id": post_id,
                    "publish_time": publish_time,
                    "like_count": like_count,
                    "play_count": play_count,
                    "comments_count": comments_count,
                    "reshare_count": reshare_count,
                    "post_url": post_url,
                    "Influencer_id": influencer_id
                }

                upsert_post_metrics(post_data)

            all_posts.extend(results)

            cursor = data.get("cursor")
            if not cursor:
                print("[INFO] No next cursor found. Completed fetching posts.")
                break

            time.sleep(delay)

        except requests.RequestException as req_err:
            print(f"[HTTP ERROR] {req_err}")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            break

    return all_posts

def fetch_and_store_post_data(influencer):
    influencer_id = influencer["Influencer_id"]
    facebook_username = influencer["Facebook_Username"]

    page_id = get_page_id(f"https://facebook.com/{facebook_username}")
    if not page_id:
        print(f"Could not fetch Page ID for {facebook_username}")
        return
    time.sleep(5)
    follower_count = get_follower_count(facebook_username)
    print(f"Follower Count for {facebook_username}: {follower_count}")

    upsert_follower_count(influencer_id, follower_count)

    # Calculate last 30 days window
    end_date_dt = datetime.now(timezone.utc)

    start_date_dt = end_date_dt - timedelta(days=20)

    start_date = start_date_dt.strftime('%Y-%m-%d')
    end_date = end_date_dt.strftime('%Y-%m-%d')

    print(f"Fetching posts from {start_date} to {end_date} for {facebook_username} (Page ID: {page_id})")

    fetch_all_facebook_posts(page_id, start_date, end_date, influencer_id)

# Fetch influencers and process their data
influencers = get_influencers()
for influencer in influencers:
    if not influencer.get("Facebook_Username"):
        print(f"Skipping Influencer ID {influencer.get('Influencer_id')} due to missing Facebook Username")
        continue
    fetch_and_store_post_data(influencer)
