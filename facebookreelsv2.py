import requests
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone

# Supabase configuration

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# RapidAPI configuration


HEADERS = {
    "x-rapidapi-host": RAPIDAPI_HOST,
    "x-rapidapi-key": RAPIDAPI_KEY
}

def get_existing_urls():
    """Fetch all existing post URLs from FACEBOOK_POST_METRICS."""
    response = supabase.table("FACEBOOK_POST_METRICS").select("post_url").execute()
    if response.data:
        return {row["post_url"] for row in response.data}
    return set()

def get_page_id(facebook_url):
    page_url = f'https://facebook-scraper3.p.rapidapi.com/page/page_id?url={facebook_url}'
    try:
        response = requests.get(page_url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            return data.get("page_id")
        else:
            print(f"Failed to get page ID for {facebook_url}. Status: {response.status_code}")
    except Exception as e:
        print(f"Exception occurred while fetching page ID for {facebook_url}: {str(e)}")
    return None

def get_post_id_from_url(post_url):
    """Fetch the post_id for a given Facebook post URL using the separate /post endpoint."""
    url = f"https://facebook-scraper3.p.rapidapi.com/post?post_url={post_url}"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", {})
            return results.get("post_id")
        else:
            print(f"Failed to get post ID for {post_url}. Status: {response.status_code}")
    except Exception as e:
        print(f"Exception occurred while fetching post ID for {post_url}: {str(e)}")
    return None


def fetch_all_reels(facebook_username):
    profile_link = f"https://www.facebook.com/{facebook_username}"
    page_id = get_page_id(profile_link)
    if not page_id:
        print(f"Could not resolve page ID for {facebook_username}. Skipping...")
        return

    # Set timeframe (in days)
    timeframe = 20
    cutoff_ts = int((datetime.now(timezone.utc) - timedelta(days=timeframe)).timestamp())

    cursor = ""
    total_posts = 0

    while True:
        url = f"https://facebook-scraper3.p.rapidapi.com/page/reels?page_id={page_id}"
        if cursor:
            url += f"&cursor={cursor}"

        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        json_data = response.json()
        reels = json_data.get("results", [])
        cursor = json_data.get("cursor")

        if not reels:
            print("No more reels found.")
            break

        for reel in reels:
            ts = reel.get("timestamp")
            if not ts or ts < cutoff_ts:
                print("Reached post older than timeframe or missing timestamp. Stopping.")
                return

            post_url = reel.get("url")
            if not post_url:
                continue

            post_id = get_post_id_from_url(post_url)
            if not post_id:
                print(f"Skipping reel due to missing post ID: {post_url}")
                continue

            # Check if post_id already exists
            exists_response = supabase.table("FACEBOOK_POST_METRICS").select("post_id").eq("post_id", post_id).limit(1).execute()
            if not exists_response.data:
                print(f"Post ID {post_id} not found in DB. Skipping update.")
                continue

            post_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            play_count = reel.get("play_count", 0)
            comments_count = reel.get("comments_count", 0)
            reactions_count = reel.get("reactions_count", 0)

            print(f"Updating Post ID: {post_id}, URL: {post_url}, Play Count: {play_count}, Comments: {comments_count}, Reactions: {reactions_count}, Date: {post_date}")

            update_data = {
                "play_count": play_count,
                "comments_count": comments_count,
                "like_count": reactions_count,
                "publish_time": post_date,
                "post_url": post_url
            }

            supabase.table("FACEBOOK_POST_METRICS").update(update_data).eq("post_id", post_id).execute()
            total_posts += 1

        if not cursor:
            break

    print(f"\n✅ Finished. Total Reels updated: {total_posts}")


'''
def get_influencers(Influencer_id=4):
    response = supabase.table("INFLUENCER").select("*").eq("Influencer_id", Influencer_id).order("Influencer_id").execute()
    if response.data:
        return response.data
    return []
'''

def get_influencers():
    response = supabase.table("INFLUENCER")\
        .select("Influencer_id, Facebook_Username")\
        .gte("Influencer_id", 3)\
        .order("Influencer_id")\
        .execute()
    return response.data

def main():
    influencers = get_influencers()
    if not influencers:
        print("No influencer found with the given ID.")
        return

    for influencer in influencers:
        username = influencer.get("Facebook_Username")
        if not username:
            print(f"No Facebook username found for influencer ID {influencer.get('Influencer_id')}")
            continue

        print(f"\nFetching reels for Facebook username: {username}")
        fetch_all_reels(username)

if __name__ == "__main__":
    main()
