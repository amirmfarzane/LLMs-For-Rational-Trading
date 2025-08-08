from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langgraph.prebuilt import ToolNode
import pandas as pd
from langchain_community.tools import DuckDuckGoSearchResults

@tool
def get_date_important_news_topics(date:str, config:RunnableConfig):
    """Provides The most important news topics for the given date
        Args:
            date: The date to get the important news from in format of YYYY-MM-DD
        Returns:
            A string containing the important news of the day.
    """
    csv_path = config["configurable"].get("news_path")
    df  = pd.read_csv(csv_path)
    matching_rows = df[df['date'] == date].head(5)

    if matching_rows.empty:
        return f"No news found for {date}"

    result = []
    for _, row in matching_rows.iterrows():      
        result.append(row["link"].split("/")[-1])
    return "\n".join(result)

@tool
def search_web__for_news_topic(news_topic:str, config:RunnableConfig):
    """Searches for recent news related to a news_topic and returns the top result.
        Args:
            news_topic: generated news topic  
        Returns:
            A string containing the content of the search news.
    
    """
    search = DuckDuckGoSearchResults(backend="news", output_format="list", max_results=1)
    results = search.run(news_topic)

    if not results:
        return "No search results found."

    top_result = results[0]
    return f"{top_result['title']}\n{top_result['link']}"


#TODO:(AmirMahdi) add the market state