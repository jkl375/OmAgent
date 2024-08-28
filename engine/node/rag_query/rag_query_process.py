import hashlib
import pickle
from pathlib import Path
from typing import List
import copy
import json
import requests
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
from tqdm import tqdm


CURRENT_PATH = root_path = Path(__file__).parents[0]
PARSER = DictParser()




@registry.register_node()
class RagQueryPreprocessor(BaseLLMBackend, BaseProcessor):
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
    
    def bge_encode(self, text):
        url = "http://172.16.36.38:3603/clip/v2/serving/text_encode"
        body = {
            "model_id": "bge",
            "text": [
                text
            ]
        }
        resp = requests.post(url=url, json=body).json()
        return resp["features"][0]

    def _run(self, args: BaseInterface, ltm: LTM) -> BaseInterface:

        data = json.load(open(args.task.task))
        
        for each in tqdm(data):
            recalls = []
            text_vector = self.bge_encode(each["questions"])
            search_query = {"match": {"text_vector": {"value": text_vector, "threshold": 0.6}}, "size": 10, "include": ["text"]}
            r2_resp = ltm.R2BaseHandler.query(
                search_query=search_query,
                index_id="rag_ir_test"
                )
            if len(r2_resp) > 0:
                for _source in r2_resp:
                    recalls.append(_source["_source"]["text"])
            each["recalls"] = recalls
        with open("data/crud_rag_rag_results.json", "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        return args

    