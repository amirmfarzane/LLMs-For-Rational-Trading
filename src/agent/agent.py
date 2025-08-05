import os 
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.graph import MessagesState, StateGraph, START
from langgraph.prebuilt import tools_condition
from langchain_core.messages import HumanMessage
from typing_extensions import TypedDict
from src.agent.tools import *
from src.agent.goldapi import get_open_close_in_range_from_csv
from pydantic import BaseModel
from typing import List, Literal

class TradeStrategy(BaseModel):
    date: str  # e.g., "2025-07-01"
    action: Literal["buy", "sell", "wait"]

class StrategyOutput(BaseModel):
    explanation: str
    strategy: List[TradeStrategy]

REACT_SYS_PROMPT = """
Your task is to trade in a gold stock market.
Your thinking should be thorough and so it's fine if it's very long. You can think step by step before and after each action you decide to take.
You MUST iterate and keep going until the problem is solved.
Only terminate your turn when you are sure that the problem is solved. Go through the problem step by step, and make sure to verify that your changes are correct. NEVER end your turn without having solved the problem, and when you say you are going to make a tool call, make sure you ACTUALLY make the tool call, instead of ending your turn.
THE PROBLEM CAN DEFINITELY BE SOLVED WITHOUT THE INTERNET.
If after using a tool you still need more information use more of the tools provided
On your Final answer give the final sell, buy, and wait state list for the asked range of days given the user prompt if you dont want to use a tool anymore
# Workflow

## High-Level Problem Solving Strategy

1. Understand the problem deeply. Carefully read the issue and think critically about what is required.
2. Use all the tools. Try to gather as much data as possible from the state of the market.
3. Use the given tools carefully. They can provide you really good information.
4. On your Final answer give the final sell, buy, and wait for the given range of days to trade in the gold stock market.

# Output Format

When you are ready to provide the final answer, you MUST output it in the following exact JSON format:

{
  "explanation": "A detailed explanation of the strategy and reasoning goes here.",
  "strategy": [
    {"date": "2025-07-01", "action": "buy"},
    {"date": "2025-07-02", "action": "wait"},
    {"date": "2025-07-03", "action": "sell"}
    // ...continue for the rest of the days
  ]
}
GET THE NEWS TOPICS ONLY FOR THE LAST 3 DAYS NOT ALL THE INTERVAL.
The strategy must be a list of actions (buy, sell, or wait) for each day in the given date range. Each action must be mapped to the correct date.
"""

class ConfigSchema(TypedDict):
    news_csv: str

class GoldTradingAgent:
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        os.environ["GOOGLE_API_KEY"] = self.api_key
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-preview-05-20",
            temperature=1,
            max_tokens=5000,
        )

        def agent_node(state: MessagesState) -> MessagesState:   
            msg_history = state["messages"]
            new_msg = self.llm.invoke([REACT_SYS_PROMPT] + msg_history)
            msg_history.append(new_msg)
            return {"messages": msg_history}

        tools_node = ToolNode(tools=[search_news, get_date_important_news_topics])
        self.llm = self.llm.bind_tools([search_news, get_date_important_news_topics])

        self.react_builder = StateGraph(MessagesState, config_schema=ConfigSchema)
        self.react_builder.add_node("agent", agent_node)
        self.react_builder.add_node("tools", tools_node)
        self.react_builder.add_edge(START, "agent")
        self.react_builder.add_conditional_edges("agent", tools_condition)
        self.react_builder.add_edge("tools", "agent")

        self.react_graph = self.react_builder.compile()

    def run(self, start_date: str, end_date: str, news_csv: str, gold_prices_csv: str) -> StrategyOutput:
        user_prompt = f"""Predict the trade strategy in a week for gold stock market given the information and data you have.
                Give a full explanation why you decide on this strategy.
                Your strategy should be list of buy,sell or wait for each day.\n
                REMBEMBER THE FORMAT:
                {{
                  "explanation": "A detailed explanation of the strategy and reasoning goes here.",
                  "strategy": [
                    {{"date": "2020-01-01", "action": "buy"}},
                    {{"date": "2020-01-02", "action": "wait"}},
                    {{"date": "2020-01-03", "action": "sell"}}
                    // ...continue for the rest of the days
                  ]
                }}\n
                Gold prices in the past two weeks:\n
                {get_open_close_in_range_from_csv(start_date, end_date, gold_prices_csv)}\n
                GIVE THE STRATEGY.
                """
        input_msg = HumanMessage(content=user_prompt)
        input_config = {"configurable": {"news_csv": news_csv}}

        response = self.react_graph.invoke(MessagesState(messages=[input_msg]), config=input_config)
        for msg in response["messages"]:
            msg.pretty_print()
        final_response = response["messages"][-1].content
        return StrategyOutput.model_validate_json(final_response)  
      
  
  
if __name__ == "__main__":
    agent = GoldTradingAgent()
    start_date = "2025-07-01"
    end_date = "2025-07-07"
    news_csv = "2023.csv"
    gold_prices_csv = "gold_prices_2025.csv"
    strategy_output = agent.run(start_date, end_date, news_csv, gold_prices_csv)
    print("hello")