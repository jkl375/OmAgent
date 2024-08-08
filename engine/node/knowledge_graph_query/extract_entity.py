import hashlib
import pickle
from pathlib import Path
from typing import List
import copy
import requests
import json
from tqdm import tqdm
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



@registry.register_node()
class KGExtractEntityProcess(BaseLLMBackend, BaseProcessor):
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
    
    def bge_rerank(self, query, input_list):
        text = []
        for input in input_list:
            text.append([query, input])
        url = "http://172.16.36.38:3602/clip/v2/serving/ranker_encode"
        body = {
            "model_id": "rerank",
            "text": text
        }
        resp = requests.post(url, json=body).json()
        features = resp["features"]
        # features是一个一维列表，返回值最大前10个的索引
        return sorted(range(len(features)), key=lambda k: features[k], reverse=True)[:10]

    def _run(self, args: BaseInterface, ltm: LTM) -> BaseInterface:
        qas = json.load(open(args.task.task))
        querys = [qa["questions"] for qa in qas]
        recalls = json.load(open("data/crud_rag_recalls.json"))["recalls"]
        results = []
        for query in tqdm(querys):
            chat_complete_res = self.infer(
                            input_list=[
                                {
                                    "input": query
                                }
                            ]
                        )
            resp = PARSER.parse(
                            chat_complete_res[0]["choices"][0]["message"]["content"]
                        )
            Pages = []
            for node in resp["nodes"]:
                resp = ltm.NebulaHandler.query_data(f'GET SUBGRAPH WITH PROP 1 STEPS FROM "{node}" YIELD VERTICES AS nodes, EDGES AS relationships;')
                for related_nodes in resp["nodes"]:
                    if "sources" in related_nodes["props"]:
                        pages = [int(page) for page in related_nodes["props"]["sources"].split(",")]
                        Pages.extend(pages)
            Pages = list(set(Pages))
            select_recalls = [recalls[i] for i in Pages]
            rerank_indexs = self.bge_rerank(query, select_recalls)
            rerank_recalls = [select_recalls[i] for i in rerank_indexs]

            results.append({"questions": query, "recalls": rerank_recalls})
        with open("data/crud_rag_kg_results.json", "w") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        return args

    