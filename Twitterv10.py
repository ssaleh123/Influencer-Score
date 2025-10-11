#this works. 
import requests
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import email.utils as eut
# Supabase credentials

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# RapidAPI credentials
RAPIDAPI_HOST = "twitter154.p.rapidapi.com"
R

# Only include tweets from the past 10 days
timeframe = datetime.now(timezone.utc) - timedelta(days=20)

def get_follower_count(username):
    url = f"https://{RAPIDAPI_HOST}/user/details"
    params = {"username": username}
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error fetching user details for {username}: {response.text}")
        return None

    data = response.json()
    return data.get("follower_count", None)

def fetch_tweets(username):
    print(f"\nFetching tweets for {username}...")

    # Step 1: Get follower count
    follower_count = get_follower_count(username)
    print(f"Follower Count: {follower_count}")

    # Step 2: Get Influencer_id from INFLUENCER table
    influencer_response = supabase.table("INFLUENCER").select("Influencer_id").eq("Twitter_Username", username).single().execute()
    if not influencer_response.data:
        print(f"No Influencer_id found for {username}")
        return
    influencer_id = influencer_response.data["Influencer_id"]

    # Step 3: Insert or upsert into TWITTER_USER_METRICS
    supabase.table("TWITTER_USER_METRICS").upsert({
        "Influencer_id": influencer_id,
        "follower_count": follower_count
    }, on_conflict=["Influencer_id"]).execute()

    # Step 4: Get tweets
    url = f"https://{RAPIDAPI_HOST}/user/tweets"
    querystring = {
        "username": username,
        "limit": "50",
        "include_replies": "false",
        "include_pinned": "false"
    }
    headers = {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": RAPIDAPI_KEY
    }

    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code != 200:
        print(f"Error fetching tweets for {username}: {response.text}")
        return

    data = response.json()

    for tweet in data.get("results", []):
        raw_date = tweet.get("creation_date")
        if not raw_date:
            continue

        parsed_date = eut.parsedate_to_datetime(raw_date)
        if parsed_date < timeframe:
            continue

        tweet_id = tweet.get("tweet_id")
        tweet_url = f"https://x.com/{username}/status/{tweet_id}"
        creation_date = parsed_date.strftime("%Y-%m-%d")
        favorite_count = tweet.get("favorite_count")
        retweet_count = tweet.get("retweet_count")
        reply_count = tweet.get("reply_count")
        views = tweet.get("views")
        bookmark_count = tweet.get("bookmark_count", 0)

        # Step 5: Upsert into TWITTER_POST_METRICS
        post_data = {
            "post_id": tweet_id,
            "post_url": tweet_url,
            "creation_date": creation_date,
            "like_count": favorite_count,
            "view_count": views,
            "retweet_count": retweet_count,
            "reply_count": reply_count,
            "bookmark_count": bookmark_count,
            "Influencer_id": influencer_id
        }

        supabase.table("TWITTER_POST_METRICS").upsert(post_data, on_conflict=["post_id"]).execute()

        print(f"Inserted/Updated tweet: {tweet_id}")

def main():
    try:
        response = (
            supabase.table("INFLUENCER")
            .select("Twitter_Username")
            .gte("Influencer_id", 37)  
            .order("Influencer_id")
            .execute()
        )
        users = response.data

        if not users:
            print("No influencers found.")
            return

        for user in users:
            username = user.get("Twitter_Username")
            if username:
                fetch_tweets(username)
            else:
                print("Missing Twitter_Username in row.")
    except Exception as e:
        print("Error fetching data from Supabase:", e)

if __name__ == "__main__":
    print("Script started")
    main()
