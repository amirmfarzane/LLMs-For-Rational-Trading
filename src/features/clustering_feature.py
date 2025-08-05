import os
import json
import time
import pandas as pd
import re
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from openai import OpenAI
from tqdm import tqdm


load_dotenv()
API_KEY = os.getenv("AVVALAI_API_KEY")
os.environ["AVVALAI_API_KEY"] = API_KEY


system_prompt = """You are a news classification assistant. Your job is to assign a short news article to the most relevant topic cluster.

You will receive a list of existing topic clusters and a news article. You must:
1. Assign the article to one of the existing clusters if it's relevant.
2. If the article doesn't fit any current cluster, create a new one and assign the article to that.

Be concise and only return the result in the specified JSON format.
"""

def build_user_prompt(news_text, clusters):
    return f"""Here is the current list of topic clusters:
        {json.dumps(clusters, indent=2)}

        Here is the news article:
        \"\"\"
        {news_text}
        \"\"\"

        Now, decide which cluster it belongs to. If none match create a new one and assign the article to it.
        I want the clusters to be detailed specially in the financial news. But not that detailed. Because there are 200000 news and i want to have 1000 clusters at most. So dont choose clusters based on single events but on group of events. For example you can seperate 4,5 wars and a cluster for famous people talking and for sanctions, oil cost change, and things like that but not like clustering as for example some specific event happened at some specific country which is not what I want. I want your title to be 4 to 5 words that describe cluster of the news.
        Return your response in this JSON format:
        {{
        "assigned_cluster": "<assigned cluster name>",
        }} dont type anything else"""



def update_clusters(initial_clusters_path, clusters, assigned_cluster):
    if assigned_cluster not in clusters:
        clusters.append(assigned_cluster)
        with open(initial_clusters_path, "w") as f:
            json.dump(clusters, f, indent=2)    
    return clusters

def append_to_csv(csv_path, date, link, cluster):
    new_row = pd.DataFrame([{
        "date": date,
        "link": link,
        "assigned_cluster": cluster
    }])

    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path)
        updated_df = pd.concat([existing_df, new_row], ignore_index=True)
    else:
        updated_df = new_row

    updated_df.to_csv(csv_path, index=False)


def clean_json_response(raw_text):
    cleaned = re.sub(r"^```(?:json)?|```$", "", raw_text.strip(), flags=re.IGNORECASE)
    cleaned = cleaned.replace("\\n", "").replace("\\r", "").strip()
    return cleaned


if __name__ == "__main__":
    client = OpenAI(
        base_url="https://api.avalai.ir/v1",        
        api_key=API_KEY,
    )

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    csv_path = "2023.csv"
    start_date = "2025-01-01"
    end_date = "2025-08-01"
    sample_per_day = 5
    initial_clusters_path = "clusters.json"
    clustered_news_path = "clustered.csv"
    with open(initial_clusters_path, "r") as f:
        clusters = json.load(f)

    df = pd.read_csv(csv_path, parse_dates=["date"])
    df["date"] = pd.to_datetime(df["date"])

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    date_range = pd.date_range(start=start, end=end)

    for single_date in tqdm(date_range, desc="Processing dates"):
        day_rows = df[df["date"] == single_date].head(30) 
        if day_rows.empty:
            continue

        for idx, row in day_rows.iterrows():
            link = row.get("link")
            if not link:
                continue

            try:
                driver.get(link)
                time.sleep(2)
                try:
                    title_elem = driver.find_element(By.XPATH, '//h1[@class="article-title" and @id="articleTitle"]')
                    title = title_elem.text.strip()
                except:
                    title = "No title found"

                paragraphs = driver.find_elements(By.TAG_NAME, "p")
                paragraph_texts = [p.text.strip() for p in paragraphs[:3]]
                news_text = f"{title}\n" + "\n".join(paragraph_texts)

                user_prompt = build_user_prompt(news_text, clusters)

                completion = client.chat.completions.create(
                    model="gpt-4o-mini-2024-07-18",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=1,
                    max_tokens=30,
                    top_p=1,
                    stream=False
                )

                response_content = completion.choices[0].message.content
                result = json.loads(clean_json_response(response_content))
                assigned_cluster = result["assigned_cluster"]
                clusters = update_clusters(initial_clusters_path, clusters, assigned_cluster)
                append_to_csv(clustered_news_path, single_date.date(), link, assigned_cluster)

            except Exception as e:
                print(f"Error processing {link}: {e}")

driver.quit()
