from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langgraph.prebuilt import ToolNode
import pandas as pd
from src.news_query.full_news_extractor import get_news_content

# @tool
def get_date_important_news(date, config):
    """Provides a description for a specific column within a given table.

        Args:
            date: The date to get the important news from in format of YYYY-MM-DD
        Returns:
            A string containing the important news of the day.
    """
    df  = pd.read_csv(config["news_csv"])
    matching_rows = df[df['date'] == date]

    if matching_rows.empty:
        return f"No news found for {date}"

    result = []
    for _, row in matching_rows.iterrows():      
        url = row['url']
  
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        try:
            content = get_news_content(url)
            result.append(f"URL: {url}\nContent: {content}\n{'-'*50}")
        except Exception as e:
            result.append(f"URL: {url}\nError fetching content: {str(e)}\n{'-'*50}")
    
    return "\n".join(result)

    

print(get_date_important_news("2025-06-02", {"news_csv":"filtered_news.csv"}))



