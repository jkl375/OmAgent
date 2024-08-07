import hashlib
import pickle
from pathlib import Path
from typing import List
import copy
from omagent_core.core.llm.base import BaseLLMBackend
from omagent_core.core.node.base import BaseProcessor
from omagent_core.core.prompt.parser import DictParser
from omagent_core.core.prompt.prompt import PromptTemplate
from omagent_core.handlers.data_handler.ltm import LTM
from omagent_core.handlers.log_handler.logger import logging
from omagent_core.schemas.base import BaseInterface
from omagent_core.utils.registry import registry
from pydantic import Field, field_validator
from langchain_community.document_loaders import PyPDFLoader, TextLoader, Docx2txtLoader
from langchain.text_splitter import CharacterTextSplitter, RecursiveCharacterTextSplitter


CURRENT_PATH = root_path = Path(__file__).parents[0]
PARSER = DictParser()


def combine(front, back):
    nodes = []
    for node_back in back["nodes"]:
        flag = False
        for node_front in front["nodes"]:
            if node_back["id"] == node_front["id"] and node_back["label"] == node_front["label"]:
                node_front["chunk"].extend(node_back["chunk"])
                flag = True
        if not flag:
            nodes.append(node_back)
            # else:
            #     nodes.append(node_back)

    relationships = []
    for relationship in back["relationships"]:
        if relationship not in front["relationships"]:
            relationships.append(relationship)

    front["nodes"].extend(nodes)
    front["relationships"].extend(relationships)
    return front

def add_page(page, res):
    for each in res["nodes"]:
        each["chunk"] = [page]
    return res

@registry.register_node()
class KGPreprocessor(BaseLLMBackend, BaseProcessor):
    prompts: List[PromptTemplate] = Field(
        default=[
            PromptTemplate.from_file(
                CURRENT_PATH.joinpath("sys_prompt.prompt"), role="system"
            ),
            PromptTemplate.from_file(
                CURRENT_PATH.joinpath("user_prompt.prompt"), role="user"
            ),
        ]
    )

    scene_detect_threshold: int = 27
    min_scene_len: int = 1
    frame_extraction_interval: int = 5
    show_progress: bool = True

    use_cache: bool = False
    cache_dir: str = "./running_logs/video_cache"


    def calculate_md5(self, file_path):
        md5_hash = hashlib.md5()
        with open(file_path, "rb") as file:
            # 读取文件内容并更新哈希对象
            for byte_block in iter(lambda: file.read(4096), b""):
                md5_hash.update(byte_block)
        # 返回MD5哈希值
        return md5_hash.hexdigest()

    def _run(self, args: BaseInterface, ltm: LTM) -> BaseInterface:
        # docx_file = "2021年国调直调系统继电保护运行规定.docx"
        # loader = Docx2txtLoader(docx_file)


        # pages = loader.load_and_split(RecursiveCharacterTextSplitter(chunk_size=500))
        import json
        data = json.load(open("crud_rag_recalls.json"))
        front = ""
        for i, page in enumerate(data["recalls"]):
            print(page)

            chat_complete_res = self.infer(
                            input_list=[
                                {
                                    "node_labels": "",
                                    "relationship_types": "",
                                    "input": page
                                }
                            ]
                        )
            
            try:
                res = PARSER.parse(
                                chat_complete_res[0]["choices"][0]["message"]["content"]
                            )
                res = add_page(i, res)
                if front:
                    res = combine(front, res)
                front = copy.deepcopy(res)
                
                print(111111111111, res)
            except Exception as e:
                print("error", e)


        import json
        with open(f"res.json", "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)

        return args

    