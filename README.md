Influencer Scoring System – Calculates overall influencer scores from social media and Google data.

Description

This project collects post and user data from Instagram, Facebook, TikTok, YouTube, and Twitter, as well as Google search metrics, to compute engagement, follower, and search scores. 
These scores are weighted, normalized, and combined to produce a final influencer score out of 100, with niche-based adjustments. Everything is stored in a supabase database. 
Anything related to algorithmic equations for the scores have been omitted. 

Features

Fetches post data and follower counts from multiple social media platforms via APIs.

Calculates platform-specific engagement scores, separating video and picture posts.

Computes follower scores relative to the highest follower count.

Gathers Google search metrics including article info, traffic, trends, and knowledge panel presence.

Combines all data into a final influencer score, weighted by platform and search metrics.

APIs Used

Instagram, Facebook, TikTok, YouTube, Twitter (via RapidAPI)

Google News, Ahrefs, Article Extractor, Search Volume & Trend, Knowledge Panel APIs

Installation

Clone the repository:

git clone https://github.com/ssaleh123/Influencer-Score.git

Run the main execution script to fetch data and calculate scores:

python script_execution.py
