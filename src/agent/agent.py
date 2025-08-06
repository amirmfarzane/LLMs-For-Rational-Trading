from datetime import datetime, timedelta
import os 
from langchain_openai import ChatOpenAI
from langgraph.graph import MessagesState, StateGraph, START
from langgraph.prebuilt import tools_condition
from langchain_core.messages import HumanMessage
from typing_extensions import TypedDict
from src.agent.tools import *
from src.agent.goldapi import get_technical_indicators_in_range_from_csv
from pydantic import BaseModel
from typing import List, Literal
import yaml
import random
import httpx

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
GET THE NEWS TOPICS ONLY FOR THE LAST 3 DAYS NOT ALL THE INTERVAL. USE ALL THE TOOLS PROVIDED TO YOU.
The strategy must be a list of actions (buy, sell, or wait) for each day in the given date range. Each action must be mapped to the correct date.
"""

class ConfigSchema(TypedDict):
    news_csv: str

class GoldTradingAgent:
    def __init__(self):
        self.api_key = os.getenv("AVVALAI_API_KEY")
        os.environ["OPENAI_API_KEY"] = self.api_key
        self.llm = ChatOpenAI(
            model="gpt-4o-mini-2024-07-18",
            base_url = "https://api.avalai.ir/v1",
            temperature=1,
            max_tokens=5000,
        )
        self.llm = self.llm.bind_tools([search_news, get_date_important_news_topics])
            
        def agent_node(state: MessagesState) -> MessagesState:   
            msg_history = state["messages"]
            new_msg = self.llm.invoke([REACT_SYS_PROMPT] + msg_history)
            msg_history.append(new_msg)
            return {"messages": msg_history}

        tools_node = ToolNode(tools=[search_news, get_date_important_news_topics])

        self.react_builder = StateGraph(MessagesState, config_schema=ConfigSchema)
        self.react_builder.add_node("agent", agent_node)
        self.react_builder.add_node("tools", tools_node)
        self.react_builder.add_edge(START, "agent")
        self.react_builder.add_conditional_edges("agent", tools_condition)
        self.react_builder.add_edge("tools", "agent")

        self.react_graph = self.react_builder.compile()

    def run(self, start_date: str, end_date: str, news_csv: str, gold_prices_csv: str) -> StrategyOutput:
        user_prompt = user_prompt = f"""
                You are a trading assistant. Based on the daily technical indicators and gold price open/close values, decide whether the strategy for each day is:
                - 2 → Buy
                - 1 → Sell
                - 0 → Neutral (Wait)

                Use these indicators to help you:
                - **SMA & EMA crossover**: Buy if short > long, Sell if short < long.
                - **MACD**: Buy if MACD > Signal, Sell if MACD < Signal.
                - **RSI**: Buy if RSI < 30, Sell if RSI > 70.
                - **Bollinger Bands**: Buy if close < lower band, Sell if close > upper band.
                - **Stochastic Oscillator**: Buy if %K < 20 and rising above %D, Sell if %K > 80 and falling below %D.

                Give your answer in this format:
                {{
                "explanation": "A detailed explanation of how indicators influenced your strategy.",
                "strategy": [
                    {{"date": "2020-01-01", "action": 2}},
                    {{"date": "2020-01-02", "action": 0}},
                    {{"date": "2020-01-03", "action": 1}}
                    // ... continue for all days
                ]
                }}

                Here is the input data:

                {get_technical_indicators_in_range_from_csv(start_date, end_date, gold_prices_csv)}
                """

        input_msg = HumanMessage(content=user_prompt)
        input_config = {"configurable": {"news_csv": news_csv}}

        response = self.react_graph.invoke(MessagesState(messages=[input_msg]), config=input_config)
        for msg in response["messages"]:
            msg.pretty_print()
        final_response = response["messages"][-1].content
        return StrategyOutput.model_validate_json(final_response)  
      
def load_config(path: str) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_action_from_prompt(prompt):
    ####TODO####
    pass

def choose_actions(agent, config, news_csv, price_df, lookback):
    """
    Runs agent for each day using a rolling lookback window,
    gets decisions, and evaluates them based on next day's price movement.
    """
    start_date = config['start_date']
    end_date = config['end_date']

    actions = []
    dates = []

    current_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") + timedelta(days=lookback)
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

    while current_date <= end_date_dt:
        lookback_start = current_date - timedelta(days=lookback)
        lb_start_str = lookback_start.strftime("%Y-%m-%d")
        current_str = current_date.strftime("%Y-%m-%d")

        try:
            action = get_action_from_prompt(agent.run(lb_start_str, current_str, news_csv, config['paths']['evaluation']))
        except:
            action = random.randint(0, 2)

        actions.append(action)
        dates.append(current_str)

        current_date += timedelta(days=1)

    # Create result DataFrame
    result_df = pd.DataFrame({
        'date': dates,
        'final_decision': actions
    })

    # Merge with price_df to get open/close prices
    return pd.merge(result_df, price_df[['date', 'open', 'close']], on='date', how='left')

def evaluate_actions(merged_df):
    # Evaluate performance using next day's open/close
    profits = []
    for i in range(len(merged_df) - 1):
        action = merged_df.loc[i, 'final_decision']
        next_open = merged_df.loc[i + 1, 'open']
        next_close = merged_df.loc[i + 1, 'close']

        if action == 2:  # Buy
            profit = next_close - next_open
        elif action == 1:  # Sell
            profit = next_open - next_close
        else:
            profit = 0.0

        profits.append(profit)

    profits.append(0.0)  # No next day for the last row
    merged_df['profit'] = profits
    merged_df['cumulative_profit'] = merged_df['profit'].cumsum()
    merged_df = merged_df.dropna()

    # Calculate statistics
    total_profit = merged_df['profit'].sum()
    buy_profit = merged_df.loc[merged_df['final_decision'] == 2, 'profit'].sum()
    sell_profit = merged_df.loc[merged_df['final_decision'] == 1, 'profit'].sum()
    num_nans = merged_df.isna().sum().sum()  # total number of NaN values in all columns

    # Calculate statistics
    total_profit = merged_df['profit'].sum()
    buy_profit = merged_df.loc[merged_df['final_decision'] == 2, 'profit'].sum()
    sell_profit = merged_df.loc[merged_df['final_decision'] == 1, 'profit'].sum()
    num_nans = merged_df.isna().sum().sum()  # total NaN cells
    num_valid_rows = merged_df.dropna().shape[0]  # fully valid rows

    # Print stats
    print(f"Total Profit: {total_profit:.2f}")
    print(f"Buy Profit: {buy_profit:.2f}")
    print(f"Sell Profit: {sell_profit:.2f}")
    print(f"Total NaNs in DataFrame: {num_nans}")
    print(f"Rows without any NaNs: {num_valid_rows}")


def run_pipline():
    agent = GoldTradingAgent()
    config = load_config("configs/numerical_feature_extractor.yaml")
    
    news_csv = "2023.csv"
    lookback = 7

    price_df = pd.read_csv(config['paths']['evaluation'])  # must include date, open, close

    df = choose_actions(agent, config, news_csv, price_df, lookback)
    evaluate_actions(df)
  
if __name__ == "__main__":
    
    agent = GoldTradingAgent()
    config = load_config("configs/numerical_feature_extractor.yaml")
    
    news_csv = "2023.csv"
    strategy_output = agent.run(config['start_date'], config['end_date'], news_csv, config['paths']['evaluation'])
    print("hello")