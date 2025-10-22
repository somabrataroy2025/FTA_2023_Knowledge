import os
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI,AzureOpenAIEmbeddings

load_dotenv()

llm = AzureChatOpenAI(  
    azure_deployment="gpt-4.1",  # or your deployment
    api_version="2024-12-01-preview",  # or your api version
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

embedding = AzureOpenAIEmbeddings(
    azure_deployment="text-embedding-3-small",
    api_version="2024-12-01-preview"
)
