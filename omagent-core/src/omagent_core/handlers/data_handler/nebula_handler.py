from typing import Any, Dict, List, Union

from pydantic import BaseModel

from ...schemas.base import BaseTable
from ..error_handler.error import VQLError
from ..log_handler.logger import logging
from ...utils.registry import registry
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
import pandas as pd
from typing import Dict
from nebula3.data.DataObject import Value, ValueWrapper
from nebula3.data.ResultSet import ResultSet
import json

def result_to_df(result: ResultSet) -> pd.DataFrame:
    """
    build list for each column, and transform to dataframe
    """
    assert result.is_succeeded()
    columns = result.keys()
    d: Dict[str, list] = {}
    for col_num in range(result.col_size()):
        col_name = columns[col_num]
        col_list = result.column_values(col_name)
        d[col_name] = [x.cast() for x in col_list]
    return pd.DataFrame(d)

@registry.register_handler()
class NebulaHandler(BaseModel):
    """
    数据库操作基础类
    """

    # 获取数据库相关环境变量
    db: str
    user: str = "root"
    passwd: str = "nebula"
    host: str = "127.0.0.1"
    port: int = 9669
    client: Any = None

    DELETED: int = 1
    NO_DELETED: int = 0
    

    def __init__(self, **data: Any) -> None:
        """
        从环境变量，初始化数据库，创建数据库，创建表。
        """
        super().__init__(**data)

        config = Config()
        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        assert connection_pool.init([(self.host, self.port)], config)

        # get session from the pool
        self.client = connection_pool.get_session(self.user, self.passwd)
        assert self.client is not None

        self.client.execute(
            f"CREATE SPACE IF NOT EXISTS {self.db}(vid_type=FIXED_STRING(256)); USE {self.db};"
        )

    def create_tag(self):
        resp = self.client.execute(
                f"CREATE TAG IF NOT EXISTS node(id string, label string, sources string);"
            )
        assert resp.is_succeeded(), resp.error_msg()

    def create_edge(self):
        resp = self.client.execute(
                f"CREATE EDGE IF NOT EXISTS relationship(id string);"
            )
        assert resp.is_succeeded(), resp.error_msg()

    def insert_data(self, nodes: List[Dict[str, Union[str, List[str]]]], relationships: List[Dict[str, str]]):
        for node in nodes:
            self.client.execute(
                f"INSERT VERTEX node(id, label, sources) VALUES {node};"
            )
        for relationship in relationships:
            self.client.execute(
                f"INSERT EDGE relationship(id) VALUES {relationship};"
            )
    
    def query_data(self, query: str):
        resp = self.client.execute(query)
        # json_data = json.loads(resp.decode('utf-8'))
        assert resp.is_succeeded(), resp.error_msg()
        res = resp.dict_for_vis()
        # for each in json_data["results"][0]["data"]:

        return res