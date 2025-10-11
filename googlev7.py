# this inserts volume, trend and knowledge panel into supabase. with unique identifier for influencer id. upserts
import requests
from supabase import create_client, Client
import time

# Supabase credentials


# API credentials




# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
'''
def get_influencers():
    response = supabase.table("INFLUENCER").select("Influencer_id, Influencer_Name").order("Influencer_id").execute()
    return response.data if response.data else []
'''

def get_influencers():
    response = (
        supabase.table("INFLUENCER")
        .select("Influencer_id, Influencer_Name")   
        .gte("Influencer_id", 23)  
        .order("Influencer_id")
        .execute()
    )
    return response.data if response.data else []

def get_keyword_insight(name):
    url = f"https://{GOOGLE_KEYWORD_INSIGHT_HOST}/keysuggest/"
    headers = {
        "x-rapidapi-host": GOOGLE_KEYWORD_INSIGHT_HOST,
        "x-rapidapi-key": GOOGLE_KEYWORD_INSIGHT_KEY
    }
    params = {"keyword": name, "location": "US", "lang": "en"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if isinstance(data, list) and len(data) > 0:
            keyword_data = data[0]
            volume = keyword_data.get("volume", None)
            trend = keyword_data.get("trend", None)
            return volume, trend
        else:
            return None, None
    except Exception as e:
        print(f"Error getting keyword insight for {name}: {e}")
        return None, None

def has_knowledge_panel(name):
    time.sleep(30)
    url = f"https://{GOOGLE_SEARCH_HOST}/"
    headers = {
        "x-rapidapi-host": GOOGLE_SEARCH_HOST,
        "x-rapidapi-key": GOOGLE_SEARCH_KEY
    }
    params = {"query": name, "limit": "10", "related_keywords": "true"}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        kp = data.get("knowledge_panel", None)
        return "Yes" if kp else "No"
    except Exception as e:
        print(f"Error getting knowledge panel for {name}: {e}")
        return "no"

def upsert_into_google_info(influencer_id, volume, trend, knowledge_panel):
    try:
        data = {
            "Influencer_id": influencer_id,
            "Volume": volume,
            "Trend": trend,
            "Knowledge_Panel": knowledge_panel
        }
        supabase.table("GOOGLE_INFO").upsert(data, on_conflict=["Influencer_id"]).execute()
        print("Upserted into GOOGLE_INFO successfully.")
    except Exception as e:
        print(f"Error upserting into GOOGLE_INFO: {e}")


def main():
    influencers = get_influencers()

    if not influencers:
        print("No influencers found.")
        return

    for influencer in influencers:
        name = influencer["Influencer_Name"]
        influencer_id = influencer["Influencer_id"]

        print(f"\n{name}")

        volume, trend = get_keyword_insight(name)
        print(f"Volume: {volume}")
        print(f"Trend: {trend}")

        knowledge_panel = has_knowledge_panel(name)
        print(f"Knowledge Panel: {knowledge_panel}")

        upsert_into_google_info(influencer_id, volume, trend, knowledge_panel)
        time.sleep(1)  

if __name__ == "__main__":
    main()
