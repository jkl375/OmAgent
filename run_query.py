from pathlib import Path

from omagent_core.core.node.dnc.interface import DnCInterface
from omagent_core.core.node.dnc.schemas import AgentTask
from omagent_core.handlers.log_handler.logger import logging
from omagent_core.utils.build import Builder
from omagent_core.utils.registry import registry
from omagent_core.handlers.data_handler.nebula_handler import NebulaHandler
from omagent_core.handlers.data_handler.ltm import LTM


def run_agent(task):
    logging.init_logger("omagent", "omagent", level="INFO")
    registry.import_module(project_root=Path(__file__).parent, custom=["./engine"])
    bot_builder = Builder.from_file("workflows/knowledge_graph_query")

    # ltm = LTM()
    # memory_handler = NebulaHandler(host='127.0.0.1', port=9669, db="rag_test", user="root", passwd="nebula")
    # ltm.handler_register('knowledge', memory_handler) 

    input = DnCInterface(bot_id="1", task=AgentTask(id=0, task=task))

    bot_builder.run_bot(input)
    return input.last_output


if __name__ == "__main__":
    run_agent("data/crud_rag_qa.json")
