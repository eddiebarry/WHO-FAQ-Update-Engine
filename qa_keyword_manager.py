import json
import requests
from collections import deque
from threading import Thread
from multiprocessing import Pool
import threading
from concurrent.futures import ThreadPoolExecutor


class QAKeywordManager:
    def __init__(self, search_engine):
        self.search_engine = search_engine
        # self.index = index
        self.queue = deque()
        self.is_writing = {}
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.latest_project_id = 0
        self.latest_version_id = 0
    
    def add_to_queue(self,question_array, index_info):
        self.queue.append( (question_array, index_info) )
        folder_id_path, self.latest_project_id, self.latest_version_id, _ = index_info
        if folder_id_path not in self.is_writing.keys():
            self.is_writing[folder_id_path]=threading.Lock()

        self.pool.submit(self.add_questions)
    
    def add_questions(self):
        question_array, index_info = self.queue.popleft()
        folder_id_path, project_id, version_id, version_number = index_info
        self.is_writing[folder_id_path].acquire()
        question_array = self.transform_question_array(question_array)

        self.search_engine.index(project_id=project_id, version_id=version_id,\
            question_list= question_array)

        # TODO: setup from config
        end_url = "https://interakt-backend-labs-staging.apps.who.lxp.academy.who.int/api/train-bot-status"

        response = {
                "project_id": int(project_id),
                "version_id": int(version_id),
                "version_number": version_number,
                "status": 'ok'
            }
        
        print("post request sending to ", end_url)
        r = requests.post(end_url, json=response)
        print(r.json())

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