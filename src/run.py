from datetime import datetime, timedelta
import re 
import pandas as pd
import yaml
from src.agent.agent import GoldTradingAgent
import pickle
import os
    

    
def get_action_from_prompt(prompt):
    match = re.search(r'"action"/s*:/s*(/d+)', prompt)
    if match:
        return int(match.group(1))
    else:
        print("Action not found.")
        return -1

def choose_actions(agent, config):
    """
    Runs agent for each day using a rolling lookback window,
    gets decisions, and evaluates them based on next day's price movement.
    """
    start_date = config["dates"]['start_date']
    end_date = config["dates"]['end_date']
    news_csv_path = config['paths']['news']
    numerical_csv_path = config['paths']['evaluation']
    lookback = config["hyps"]["lookback"]

    dates = []
    model_responses = []
    # with open("E:/UT/Term8/LLM/LLMs-For-Rational-Trading/trader_results/dates.pkl", "rb") as f:   # "rb" = read in binary mode
    #     dates = pickle.load(f)
    # with open("E:/UT/Term8/LLM/LLMs-For-Rational-Trading/trader_results/model_responses.pkl", "rb") as f:   # "rb" = read in binary mode
    #     model_responses = pickle.load(f)
    

    current_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") + timedelta(days=lookback)
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    os.makedirs("trader_results", exist_ok=True)

    while current_date <= end_date_dt:
        lookback_start = current_date - timedelta(days=lookback)
        lb_start_str = lookback_start.strftime("%Y-%m-%d")
        current_str = current_date.strftime("%Y-%m-%d")
        model_response = agent.run(lb_start_str, current_str, news_csv_path, numerical_csv_path, "COT")
        dates.append(current_str)
        model_responses.append(model_response)
        print(f"### Current data: {current_str} ###")

        with open('trader_results/model_responses.pkl', 'wb') as f:
            pickle.dump(model_responses, f)
        with open('trader_results/dates.pkl', 'wb') as f:
            pickle.dump(dates, f)

        current_date += timedelta(days=1)


  
if __name__ == "__main__":

    agent = GoldTradingAgent()
    
    with open("configs/run_pipline.yaml", 'r') as file:
        config = yaml.safe_load(file)
    
    choose_actions(agent, config)