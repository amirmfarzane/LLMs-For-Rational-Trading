from datetime import datetime, timedelta
import os
import yaml
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
from src.agent.goldapi import get_technical_indicators_in_range_from_csv
from pydantic import BaseModel
from typing import List, Literal

class TradeStrategy(BaseModel):
    date: str
    action: Literal["buy", "sell", "wait"]

class StrategyOutput(BaseModel):
    explanation: str
    strategy: List[TradeStrategy]

class GoldTradingNumericalLLM:
    def __init__(self):
        from dotenv import load_dotenv
        load_dotenv()
        os.environ["OPENAI_API_KEY"] = os.getenv("AVVALAI_API_KEY")
        self.llm = ChatOpenAI(
            model="gpt-4o-mini-2024-07-18",
            base_url="https://api.avalai.ir/v1",
            temperature=1,
            max_tokens=5000,
        )

    def run(self, start_date: str, end_date: str, numerical_csv: str) -> StrategyOutput:
        data = get_technical_indicators_in_range_from_csv(start_date, end_date, numerical_csv)

        user_prompt = f"""
        You are a trading assistant. Based only on the daily technical indicators and gold price open/close values, decide whether the strategy for the future day is:
        - buy
        - sell
        - wait

        Use these indicators:
        - SMA & EMA crossover
        - MACD
        - RSI
        - Bollinger Bands
        - Stochastic Oscillator

        Provide a detailed explanation of how indicators influenced the strategy. Try to be more aggresive in trading by using more buys and sells.
        Return the output in the format:
        {{
          "explanation": "...",
          "strategy": [
            {{
              "date": "<next_date>",
              "action": "<buy/sell/wait>"
            }}
          ]
        }}

        Here is the input data:
        {data}
        """

        input_msg = HumanMessage(content=user_prompt)
        response = self.llm.invoke([input_msg])
        return response.content

if __name__ == "__main__":
    agent = GoldTradingNumericalLLM()

    with open("configs/run_pipline.yaml", 'r') as f:
        config = yaml.safe_load(f)

    strategy_output = agent.run(
        start_date=config["dates"]["start_date"],
        end_date=config["dates"]["end_date"],
        numerical_csv=config["paths"]["evaluation"]
    )

    print(strategy_output)
