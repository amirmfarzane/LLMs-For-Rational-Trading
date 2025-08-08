from datetime import datetime, timedelta
import re 
import pandas as pd
import yaml
from src.agent.agent import GoldTradingAgent

def get_action_from_prompt(prompt):
    match = re.search(r'"action"\s*:\s*(\d+)', prompt)
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
    price_df = pd.read_csv(config['paths']['evaluation'])

    actions = []
    dates = []
    model_responses = []

    current_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") + timedelta(days=lookback)
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    while current_date <= end_date_dt:
        lookback_start = current_date - timedelta(days=lookback)
        lb_start_str = lookback_start.strftime("%Y-%m-%d")
        current_str = current_date.strftime("%Y-%m-%d")
        model_response = agent.run(lb_start_str, current_str, news_csv_path, numerical_csv_path)
        action = get_action_from_prompt(model_response)

        actions.append(action)
        dates.append(current_str)
        model_responses.append(model_response)

        current_date += timedelta(days=1)

    import pickle

    with open('model_responses.pkl', 'wb') as f:
        pickle.dump(model_responses, f)

    
    # Create result DataFrame
    result_df = pd.DataFrame({
        'date': dates,
        'final_decision': actions
    })

    with open('result_df.pkl', 'wb') as f:
        pickle.dump(result_df, f)

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

  
if __name__ == "__main__":

    agent = GoldTradingAgent()
    
    with open("configs/run_pipline.yaml", 'r') as file:
        config = yaml.safe_load(file)
    
    

    df = choose_actions(agent, config)
    evaluate_actions(df)