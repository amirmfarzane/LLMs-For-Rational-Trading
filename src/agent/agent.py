import os 
from langchain_community.chat_models import ChatOpenAI
from langgraph.graph import MessagesState

REACT_SYS_PROMPT = """
Your task is to trade in a gold stock market.
Your thinking should be thorough and so it's fine if it's very long. You can think step by step before and after each action you decide to take.
You MUST iterate and keep going until the problem is solved.
Only terminate your turn when you are sure that the problem is solved. Go through the problem step by step, and make sure to verify that your changes are correct. NEVER end your turn without having solved the problem, and when you say you are going to make a tool call, make sure you ACTUALLY make the tool call, instead of ending your turn.
THE PROBLEM CAN DEFINITELY BE SOLVED WITHOUT THE INTERNET.
If after using a tool you still need more information use more of the tools provided
On your Final answer give the final sell, buy, and dontTrade state list for the asked range of days given the user prompt if you dont want to use a tool anymore
# Workflow

## High-Level Problem Solving Strategy

1. Understand the problem deeply. Carefully read the issue and think critically about what is required.
2. Use all the tools. Try to gather as much data as possible from the state of the market.
3. Use the given tools carefully. They can provide you really good information.
4. On your Final answer give the final sell, buy, and dontTrade for the given range of days to trade in the gold stock market.
"""


API_KEY = os.getenv("GAPGPT_API_KEY")
llm = ChatOpenAI(
    openai_api_key=API_KEY,
    openai_api_base="https://api.gapgpt.app/v1",
    model="gemini-2.0-flash",
    temperature=1,
    max_tokens=2048
)

def agent_node(state: MessagesState) -> MessagesState:   
    msg_history = state["messages"]
    new_msg = llm.invoke([REACT_SYS_PROMPT] + msg_history)
    msg_history.append(new_msg)

    return {"messages": msg_history}