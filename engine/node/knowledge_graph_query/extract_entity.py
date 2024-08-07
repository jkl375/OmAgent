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

    def _run(self, args: BaseInterface, ltm: LTM) -> BaseInterface:
        

        chat_complete_res = self.infer(
                        input_list=[
                            {
                                "node_labels": "",
                                "relationship_types": "",
                                "input": page
                            }
                        ]
                    )
        

        res = PARSER.parse(
                        chat_complete_res[0]["choices"][0]["message"]["content"]
                    )





        return args

    