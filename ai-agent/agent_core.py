import os
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder


db_url = "postgresql://ai_reader:secure_pass@localhost:5432/f1_analytics"
db = SQLDatabase.from_uri(
    db_url,
    include_tables=["race_and_tyre_performance"]
)

llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash-lite", temperature=0, timeout=20, max_retries=1)
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are a Formula 1 SQL expert interacting with a PostgreSQL database. "
        "First, check the available tables and their schema. "
        "Then write and execute a syntactically correct SELECT query to answer the user's question. "
        "Answer in the same language as the user's question."
    ),
    ("human", "{input}"),
    MessagesPlaceholder("agent_scratchpad"),
])

agent_executor = create_sql_agent(
    llm=llm,
    db=db,
    prompt=prompt,
    agent_type="tool-calling",
    use_query_checker=False,
    verbose=True
)
if __name__ == "__main__":
    print("AI agent works! Type your question('exit' to exit): ")
    while True:
        user_input = input(">>")
        if user_input.lower() in ['exit', 'quit']:
            break
        try:
            response = agent_executor.invoke({"input" : user_input})
            print(f"Response: {response['output']}")
            print("-" * 50)
        except Exception as e:
            print(f"Error was ouccured: {e}")
#який пілот виграв гонку  в сільверстоуні в 2026 році?