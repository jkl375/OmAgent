import json
from langchain_community.document_loaders import PyPDFLoader, TextLoader, Docx2txtLoader
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter

front = json.load(open("res.json", "r"))
back = json.load(open("re.json", "r"))

def combine(front, back):
    nodes = []
    for node in back["nodes"]:
        if node not in front["nodes"]:
            nodes.append(node)

    relationships = []
    for relationship in back["relationships"]:
        if relationship not in front["relationships"]:
            relationships.append(relationship)

    back["nodes"].extend(nodes)
    back["relationships"].extend(relationships)
    return back



docx_file = "2021年国调直调系统继电保护运行规定.docx"
loader = Docx2txtLoader(docx_file)


pages = loader.load_and_split(RecursiveCharacterTextSplitter(chunk_size=500))
for page in pages:
    print(page.page_content)
    break

