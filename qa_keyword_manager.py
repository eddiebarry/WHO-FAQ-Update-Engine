from collections import deque
from threading import Thread
from multiprocessing import Pool
import threading
from concurrent.futures import ThreadPoolExecutor


class QAKeywordManager:
    def __init__(self, search_engine, index):
        self.search_engine = search_engine
        self.index = index
        self.queue = deque()
        self.is_writing = {}
        self.pool = ThreadPoolExecutor(max_workers=4)
    
    def add_to_queue(self,question_array, index_info):
        self.queue.append( (question_array, index_info) )
        folder_id_path = index_info[0]
        if folder_id_path not in self.is_writing.keys():
            self.is_writing[folder_id_path]=threading.Lock()

        self.pool.submit(self.add_questions)
    
    def add_questions(self):
        question_array, index_info = self.queue.popleft()
        folder_id_path, project_id, version_id, version_number = index_info
        self.is_writing[folder_id_path].acquire()
        if self.index.getIndexDir() != folder_id_path:
            self.index.update_store_dir(folder_id_path) 

        question_array = self.transform_question_array(question_array)
        self.index.indexJsonArray(question_array)

        self.search_engine.update(self.index.getIndexDir())

        # TODO: setup from config
        end_url = "https://feature-train-bot-interakt-backend-labs-dev.apps.who.lxp.academy.who.int/api/train-bot-status"
        response = {
                "project_id": project_id,
                "version_id": version_id,
                "status": 'ok'
            }
        request.post(end_url, data=json.dumps(response))

        self.is_writing[folder_id_path].release()

    def transform_question_array(self, question_array):
        for qa_pair in question_array:
            keywords = qa_pair['keywords']
            for category_keywords in keywords:
                keyword_string = ""
                assert len(category_keywords.keys())==1
                
                for x in category_keywords:
                    keyword_string += " ".join(category_keywords[x])
                    if keyword_string != " ":
                        qa_pair[x] = keyword_string
            qa_pair.pop('keywords',None)

        return question_array