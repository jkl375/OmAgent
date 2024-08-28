import os

import requests

from ..error_handler.error import VQLError
from ..log_handler.logger import logging
from ...utils.registry import registry
from pydantic import BaseModel

@registry.register_handler()
class R2BaseHandler(BaseModel):
    class Config:
        """Configuration for this pydantic object."""

        extra = "allow"
        arbitrary_types_allowed = True

    host_url: str

    # def __init__(self, endpoint):
    #     self.host_url = endpoint

    def delete_index(self, index_id):
        res = requests.delete(
            url=os.path.join(self.host_url, "r2base/v1/index/{}".format(index_id))
        )
        if res.status_code > 299:
            logging.error("del error --- {}".format(res.json()))
            raise VQLError(550, msg="R2base delete_index error[{}]".format(res.json()))
        return res.status_code

    def make_index(self, index_id, mapping):
        if not self.is_indx_in(index_id):
            res = requests.post(
                url=os.path.join(self.host_url, "r2base/v1/index/{}".format(index_id)),
                json={"mappings": mapping, "settings": {"number_of_shards": 9}},
            )
            if res.status_code > 299:
                logging.error("make error --- {}".format(res.json()))
                print("R2base make_index error[{}]".format(res.status_code))
                raise VQLError(
                    550, msg="R2base make_index error[{}]".format(res.json())
                )
            return res.status_code

    def do_add(self, index_id, docs):
        res = requests.post(
            url=os.path.join(self.host_url, "r2base/v1/index/{}/docs".format(index_id)),
            json={"docs": docs, "batch_size": 100},
        )
        if res.status_code > 299:
            logging.error("add error --- {}".format(res.json()))
            raise VQLError(
                550, detail="R2base add_docs error[{}]".format(res.status_code)
            )
        return res.json()["doc_ids"]

    def add_docs(self, index_id, docs, batch_size=100):
        buffer = []
        ids = []
        for doc in docs:
            buffer.append(doc)
            if len(buffer) >= batch_size:
                ids += self.do_add(index_id, buffer)
                buffer = []
        if len(buffer) != 0:
            ids += self.do_add(index_id, buffer)

        return ids

    def is_indx_in(self, index_id):
        res = requests.get(
            url=os.path.join(
                self.host_url, "r2base/v1/index/{}/mappings".format(index_id)
            )
        )
        if res.json()["mappings"] == {}:
            return False
        else:
            return True

    def is_id_in(self, index_id, doc_id):
        res = requests.get(
            url=os.path.join(
                self.host_url, "r2base/v1/index/{}/docs/{}".format(index_id, doc_id)
            )
        )

        if res.json()["docs"] == []:
            return False
        else:
            return True

    def get_doc_page(self, index_id, last_key=None, page_size=100):
        query = {"query": {"size": page_size}}
        if last_key:
            query["query"].update({"search_after": [last_key]})

        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/scroll_query".format(index_id)
            ),
            json=query,
            timeout=600000,
        )
        page = res.json()
        if "error" in page:
            logging.error("get page error --- {}".format(res.json()))
            print("R2base scroll data error[{}]".format(res.status_code))

        return page["docs"], page["last_id"]

    def get_doc_num(self, index_id):
        res = requests.get(
            url=os.path.join(self.host_url, "r2base/v1/index/{}".format(index_id))
        )
        return res.json()["size"]

    def del_doc(self, index_id, doc_id):
        res = requests.delete(
            url=os.path.join(
                self.host_url, "r2base/v1/index/{}/docs/{}".format(index_id, doc_id)
            )
        )
        print(res.json())
        return res.status_code

    def update_doc(self, index_id, content):
        body = {"docs": content}
        res = requests.put(
            url=os.path.join(self.host_url, "r2base/v1/index/{}/docs".format(index_id)),
            json=body,
        )
        if res.status_code > 299:
            print("add", res.json())
            print("R2base add_docs error[{}]".format(res.status_code))
        return res.json()["doc_ids"]

    def get_mapping(self, index_id):
        res = requests.get(
            url=os.path.join(
                self.host_url, "r2base/v1/index/{}/mappings".format(index_id)
            )
        )
        return res.json()["mappings"]

    def get_docs_by_id(self, index_id, ids):
        docs = []
        buffer = []
        batch_size = 50
        cnt = 0

        for id in ids:
            buffer.append(id)
            cnt += 1

            if len(buffer) == batch_size:
                buffer = "%2C".join(buffer)
                res = requests.get(
                    url=os.path.join(
                        self.host_url,
                        "r2base/v1/index/{}/docs/{}".format(index_id, buffer),
                    )
                )
                docs += res.json()["docs"]
                msg = res.status_code
                buffer = []
                if msg > 299:
                    print("Can not get data from R2Base [{}]".format(msg))
                    raise VQLError(
                        550, msg="R2base get_docs_by_id error[{}]".format(res.json())
                    )
                # print("Processed {}".format(cnt))

        if len(buffer) > 0:
            buffer = "%2C".join(buffer)
            # print("Processed {}".format(cnt))
            res = requests.get(
                url=os.path.join(
                    self.host_url, "r2base/v1/index/{}/docs/{}".format(index_id, buffer)
                )
            )
            docs += res.json()["docs"]
            msg = res.status_code
            buffer = []
            if msg > 299:
                print("Can not get data from R2Base [{}]".format(msg))
                raise VQLError(
                    550, msg="R2base get_docs_by_id error[{}]".format(res.json())
                )

        return docs

    def del_docs_by_id(self, index_id, ids):
        if ids == []:
            return 200
        buffer = []
        batch_size = 50
        cnt = 0

        for id in ids:
            buffer.append(id)
            cnt += 1

            if len(buffer) == batch_size:
                buffer = "%2C".join(buffer)
                res = requests.delete(
                    url=os.path.join(
                        self.host_url,
                        "r2base/v1/index/{}/docs/{}".format(index_id, buffer),
                    )
                )
                msg = res.status_code
                buffer = []
                if msg > 299:
                    print("Can not get data from R2Base [{}]".format(msg))
                    raise VQLError(
                        550, msg="R2base del_docs_by_id error[{}]".format(res.json())
                    )
                # print("Processed {}".format(cnt))

        if len(buffer) > 0:
            buffer = "%2C".join(buffer)
            # print("Processed {}".format(cnt))
            res = requests.delete(
                url=os.path.join(
                    self.host_url, "r2base/v1/index/{}/docs/{}".format(index_id, buffer)
                )
            )
            msg = res.status_code
            buffer = []
            if msg > 299:
                print("Can not get data from R2Base [{}]".format(msg))
                raise VQLError(
                    550, msg="R2base del_docs_by_id error[{}]".format(res.json())
                )

        return msg

    def del_docs_by_searching(self, index_id, query):
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/delete_query".format(index_id)
            ),
            json=query,
        )
        if res.status_code > 299:
            logging.error("Search del error --- {}".format(res.json()))
            print("R2base del_docs_by_searching error[{}]".format(res.status_code))
            raise VQLError(
                550, msg="R2base del_docs_by_searching error[{}]".format(res.json())
            )
        return res.json()["msg"]["deleted"]

    def get_docs_by_searching(
            self, index_id, query, last_key=[], page_size=100, include=None
    ):
        if last_key != []:
            query["query"]["search_after"] = last_key
        query["query"]["size"] = page_size
        query["query"]["include"] = include
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/scroll_query".format(index_id)
            ),
            json=query,
        )
        if res.status_code > 299:
            print(res.json())
            raise VQLError(
                550, detail="Can not search from R2Base [{}]".format(res.status_code)
            )
        res = res.json()
        docs = res["docs"]
        # output = []
        # for doc in ranks:
        #     output.append(doc['_source'])
        return docs, res["last_id"]

    def batch_query(self, search_query, index_id):
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/batch_query".format(index_id)
            ),
            json={"query": search_query},
        )
        try:
            res = res.json()["ranks"]
        except Exception as error:
            raise VQLError(550, msg="R2base batch_query error[{}]".format(res.json()))
        return [[item for item in r] for r in res]

    def query(self, search_query, index_id):
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/query".format(index_id)
            ),
            json={"query": search_query},
        )
        try:
            res = res.json()["ranks"]
        except Exception as error:
            raise VQLError(550, msg="R2base batch_query error[{}]".format(res.json()))
        return [r for r in res]

    def batch_match(
            self,
            vectors,
            index_id,
            candidates=5000,
            batch_size=100,
            res_size=100,
            include=None,
            exclude=None,
            filter=None,
            threshold=0.80,
    ):
        search_query = {"size": res_size, "sort": [{"_score": "desc"}]}
        if include != None:
            search_query["include"] = include
        if exclude != None:
            search_query["exclude"] = exclude

        if filter != None:
            search_query["filter"] = filter

        subj_q = []
        res = []
        for vector in vectors:
            subj_q.append(
                {
                    "vector": {
                        "value": vector,
                        "threshold": threshold,
                        "candidates": candidates,
                        "reverse": False,
                    }
                }
            )
            if len(subj_q) >= batch_size:
                search_query["match"] = subj_q
                res += self.batch_query(search_query, index_id)
                subj_q = []
        if len(subj_q) > 0:
            search_query["match"] = subj_q
            res += self.batch_query(search_query, index_id)
        return res

    def get_index_range(self, index_id):
        body = {"query": {"include": ["_uid"], "sort": [{"_uid": "asc"}], "size": 1}}
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/query".format(index_id)
            ),
            json=body,
        )
        if res.json()["ranks"] == []:
            return 0, 0
        min_index = res.json()["ranks"][0]["_source"]["_uid"]

        body = {"query": {"include": ["_uid"], "sort": [{"_uid": "desc"}], "size": 1}}
        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/query".format(index_id)
            ),
            json=body,
        )
        max_index = res.json()["ranks"][0]["_source"]["_uid"]

        return int(min_index), int(max_index)

    def match(
            self,
            index_id: str,
            vector: str,
            candidates=5000,
            res_size=100,
            include=None,
            exclude=None,
            filter=None,
            threshold=0.80,
    ):
        search_query = {"size": res_size, "sort": [{"_score": "desc"}]}
        if include != None:
            search_query["include"] = include
        if exclude != None:
            search_query["exclude"] = exclude

        if filter != None:
            search_query["filter"] = filter

        search_query["match"] = {
            "vector": {
                "value": vector,
                "threshold": threshold,
                "candidates": candidates,
                "reverse": False,
            }
        }

        res = requests.post(
            url=os.path.join(
                self.host_url, "r2base/v1/search/{}/query".format(index_id)
            ),
            json={"query": search_query},
        )
        try:
            res = res.json()["ranks"]
        except Exception as error:
            raise VQLError(550, msg="R2base ranking_query error[{}]".format(res.json()))
        return res