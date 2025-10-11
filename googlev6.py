#this inserts article info
import requests
from supabase import create_client, Client
from urllib.parse import urlparse, quote
import time
from datetime import datetime, timezone
from dateutil.parser import parse as parse_date

# Supabase credentials

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# RapidAPI credentials


# OpenAI credentials

'''
def get_influencers():
    response = supabase.table("INFLUENCER").select("Influencer_id, Influencer_Name").order("Influencer_id").execute()
    return response.data if response.data else []
'''

def get_influencers():
    response = (
        supabase.table("INFLUENCER")
        .select("Influencer_id, Influencer_Name")
        .gte("Influencer_id", 11)  # Only include influencers with ID >= 11
        .order("Influencer_id")
        .execute()
    )
    return response.data if response.data else []

def get_domain_from_url(url):
    return urlparse(url).netloc

def get_domain_rating(article_url):
    domain = get_domain_from_url(article_url)
    url = f"https://ahrefs2.p.rapidapi.com/authority?url={domain}&mode=subdomains"
    headers = {
  
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("domainRating", None)
    except Exception as e:
        print(f"[DR ERROR] {domain}: {e}")
        return None

def get_traffic_monthly_avg(article_url):
    def make_api_request(url_to_query):
        encoded_url = quote(url_to_query, safe='')
        url = f"https://ahrefs2.p.rapidapi.com/traffic?url={encoded_url}&mode=exact"
        headers = {

        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[API ERROR] {url_to_query}: {e}")
            return None

    # Make first API call
    print(f"[INFO] Making first API call for {article_url}")
    data = make_api_request(article_url)
    if not data:
        print("[ERROR] No response from API.")
        return 0

    traffic_monthly_avg = data.get("trafficMonthlyAvg")
    print(f"[DEBUG] trafficMonthlyAvg: {traffic_monthly_avg}")

    # If trafficMonthlyAvg is valid and greater than 0, use it
    if traffic_monthly_avg and traffic_monthly_avg > 0:
        print("[INFO] Using trafficMonthlyAvg.")
        return traffic_monthly_avg

    print("[INFO] trafficMonthlyAvg is 0 — attempting to fetch fallback organic from traffic_history...")

    # Fallback to organic from traffic_history (same or second call)
    traffic_history = data.get("traffic_history") or data.get("trafficHistory")
    if traffic_history and isinstance(traffic_history, list):
        try:
            # Sort by date to find the latest entry
            sorted_history = sorted(
                traffic_history,
                key=lambda x: parse_date(x["date"])
            )
            latest_entry = sorted_history[-1]  # most recent
            organic = latest_entry.get("organic")

            print(f"[DEBUG] Found organic in traffic_history: {organic}")
            return organic if organic is not None else 0
        except Exception as e:
            print(f"[ERROR] Failed parsing traffic_history: {e}")
    else:
        print("[WARN] traffic_history missing or not a valid list")

    return 0



def get_article_date(article_url):
    encoded_url = quote(article_url, safe='')
    url = f"https://article-extractor2.p.rapidapi.com/article/proxy/parse?url={encoded_url}"
    headers = {
       
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        published = response.json().get("data", {}).get("published", None)
        if published:
            if '+' in published:
                published = published.split('+')[0]
            elif '-' in published[10:]:
                published = published[:19]
            return datetime.fromisoformat(published).date().isoformat()
       
    except Exception as e:
        print(f"[DATE ERROR] {article_url}: {e}")
    return None

def get_semantic_score(url, influencer_name):
    prompt = f"Read this article {url} and give a semantic score out of 100 for {influencer_name} only give the number and nothing else. Base the score on this: At the top end of the scale, philanthropy or charity efforts should score between 95 and 100. Innovation and leadership actions, such as launching new ventures or demonstrating business acumen, should score between 90 and 94. Cultural relevance, including participation in viral trends or staying current in pop culture, should score between 85 and 89. Collaborations and brand deals, which indicate commercial success and mainstream appeal, should score between 80 and 84. Mentions in passing or lifestyle content, such as entertainment or day-in-the-life features, should score between 70 and 79. On the negative side, backlash or controversy, including online criticism or minor drama, should score between 50 and 69. Scandals or accusations involving legal or ethical concerns should score between 30 and 49. Public apologies or crises that confirm fault and reinforce negative public memory should score between 20 and 29. Finally, the most damaging category, cancellation or brand termination, which often signals a collapse in influence or commercial viability, should score between 0 and 19. Provide only a single number rounded to the nearest whole number based on the article’s content and its impact on {influencer_name}’s reputation."

    
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "gpt-4",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1000,
        "temperature": 1.0
    }

    try:
        # Send the request to GPT-4
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data)
        response.raise_for_status()

        content = response.json()["choices"][0]["message"]["content"]
        
        return round(float(content.strip()))

    
    except Exception as e:
        print(f"[SEMANTIC ERROR] {url}: {e}")
        return None

def upsert_article(article_data):
    try:
        # Check if an article with this URL already exists
        existing_article = supabase.table("ARTICLES").select("Semantic_Score").eq("URL", article_data['URL']).limit(1).execute()

        if existing_article.data:
            existing_score = existing_article.data[0].get("Semantic_Score")
            if existing_score not in (None, 0):
                # Preserve the existing non-zero score
                article_data["Semantic_Score"] = existing_score

        # Upsert the article
        supabase.table("ARTICLES").upsert(article_data, on_conflict=["URL"]).execute()
        print(f"[UPSERTED] {article_data['Title']}")
    except Exception as e:
        print(f"[UPSERT ERROR] {article_data['Title']}: {e}")


def fetch_and_store_articles(influencer_id, influencer_name):
    url = f"https://google-news13.p.rapidapi.com/search"
    params = {"keyword": influencer_name, "lr": "en-US"}
    headers = {
        "x-rapidapi-host": "google-news13.p.rapidapi.com",
        "x-rapidapi-key": RAPIDAPI_KEY
    }

    def convert_timestamp(ms_timestamp):
        try:
            ms_timestamp = int(ms_timestamp)
            seconds = ms_timestamp / 1000
            date = datetime.fromtimestamp(seconds, tz=timezone.utc)  # Use timezone.utc
            return date.date().isoformat()
        except Exception as e:
            print(f"[TIMESTAMP ERROR] {ms_timestamp}: {e}")
            return None

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        articles = response.json().get("items", [])[:5]

        for article in articles:
            title = article.get("title")
            link = article.get("newsUrl")
            timestamp = article.get("timestamp")
            date = convert_timestamp(timestamp)

            if not title or not link:
                continue

            domain = get_domain_from_url(link)
            dr = get_domain_rating(link)
            traffic = get_traffic_monthly_avg(link)
            score = get_semantic_score(link, influencer_name)  # Unique score for each article

            article_data = {
                "Title": title,
                "URL": link,
                "Domain": domain,
                "Date": date,
                "Domain_Authority": dr,
                "URL_Monthly_Visits": traffic,
                "Influencer_id": influencer_id,
                "Semantic_Score": score
            }

            upsert_article(article_data)
            time.sleep(1)

    except Exception as e:
        print(f"[NEWS ERROR] {influencer_name}: {e}")



def main():
    influencers = get_influencers()
    if not influencers:
        print("No influencers found.")
        return

    for influencer in influencers:
        influencer_id = influencer["Influencer_id"]
        influencer_name = influencer["Influencer_Name"]
        print(f"\nFetching articles for {influencer_name}...")
        fetch_and_store_articles(influencer_id, influencer_name)
        time.sleep(1)

if __name__ == "__main__":
    main()