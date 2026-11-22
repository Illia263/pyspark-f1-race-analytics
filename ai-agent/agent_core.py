import os
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import create_sql_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from dotenv import load_dotenv
load_dotenv()
db_url = os.getenv("DATABASE_URL")
if db_url:
    if db_url.startswith("postgres://"):
        db_uri = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_uri = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        db_uri = db_url
else:    
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB")
    db_uri = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"



db = SQLDatabase.from_uri(
    db_uri,
    include_tables=["race_and_tyre_performance"],
    sample_rows_in_table_info=2
)

llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash-lite", temperature=0, timeout=120, max_retries=3)
prompt = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are a Formula 1 SQL expert interacting with a PostgreSQL database. "
        "First, check the available tables and their schema. "
        "Then write and execute a syntactically correct SELECT query to answer the user's question. "
        "CRITICAL RULE: You MUST reply in the EXACT SAME LANGUAGE as the user's question (English -> English, Ukrainian -> Ukrainian)."
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
def ask_f1_agent(question):
    response = agent_executor.invoke({"input" : question})
    output = response['output']
    if isinstance(output, list):
        output = "\n".join(
            block.get("text", "") for block in output if isinstance(block, dict)
        )
    return output

if __name__ == "__main__":
    print("AI agent works! Type your question('exit'/'quit' to exit): ")
    while True:
        user_input = input(">>")
        if user_input.lower() in ['exit', 'quit']:
            break
        try:    
            answer = ask_f1_agent()
            print(f"\n {answer}\n")
            print("-" * 50)
            
        except Exception as e:
            print(f"Error was ouccured: {e}")
#який пілот виграв гонку  в сільверстоуні в 2026 році?