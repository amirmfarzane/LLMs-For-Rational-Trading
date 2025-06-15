from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig
from langgraph.prebuilt import ToolNode
import sqlite3
from news_query.full_news_extractor import get_news_content

@tool
def get_date_important_news(date, config: RunnableConfig):
    """Provides a description for a specific column within a given table.

        Args:
            date: The date to get the important news from in format of YYYY-MM-DD
        Returns:
            A string containing the important news of the day.
    """
    conn = sqlite3.connect(config["db_name"])
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT url FROM {config['table_name']} WHERE date = ?", (date,))
    urls = cursor.fetchall()
    
    conn.close()
    
    if not urls:
        return f"No news found for {date}"
    
    result = []
    for url_tuple in urls:
        url = url_tuple[0]  
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        try:
            content = get_news_content(url)
            result.append(f"URL: {url}\nContent: {content}\n{'-'*50}")
        except Exception as e:
            result.append(f"URL: {url}\nError fetching content: {str(e)}\n{'-'*50}")
    
    return "\n".join(result)
    
    



