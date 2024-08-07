from langchain.chains import NebulaGraphQAChain
from langchain_community.graphs import NebulaGraph
from langchain_openai import ChatOpenAI


graph = NebulaGraph(
    space="test",
    username="root",
    password="nebula",
    address="127.0.0.1",
    port=9669,
    session_pool_size=30,
)

print(graph.get_schema)


chain = NebulaGraphQAChain.from_llm(
    ChatOpenAI(
        base_url="http://10.8.21.32:3000/v1",
        model="gpt-3.5-turbo",
        api_key="sk-2fpMc0GBGTGG96w62cF7B9621bA34aDa8b2112D26404Ae4e",
        temperature=0
        ), graph=graph, verbose=True
)

chain.run("李白出生在哪?")