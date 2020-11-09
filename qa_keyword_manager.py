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
        folder_id_path, self.latest_project_id, self.latest_version_id, temp0, temp1 = index_info
        
        new_path = str(self.latest_project_id)
        if new_path not in self.is_writing.keys():
            self.is_writing[new_path]=threading.Lock()

        print("inside qa keyword manager")

        self.pool.submit(self.add_questions)
    
    def add_questions(self):
        question_array, index_info = self.queue.popleft()
        folder_id_path, project_id, version_id, \
            version_number, previous_versions = index_info

        new_path = str(project_id)
        self.is_writing[new_path].acquire()
        question_array = self.transform_question_array(question_array)

        print("inside add questions")

        print(previous_versions)
        self.search_engine.index_prev_versions(project_id=project_id,\
            version_id=version_id, previous_versions=previous_versions)
        
        print("previous questions added")
        self.search_engine.index(project_id=project_id, version_id=version_id,\
            question_list= question_array)
        print("new questions added")

        # TODO: setup from config
        end_url = "https://interakt-backend-labs-staging.apps.who.lxp.academy.who.int/api/train-bot-status"

        response = {
                "project_id": int(project_id),
                "version_id": int(version_id),
                "version_number": version_number,
                "status": 'ok'
            }
        
        print(response)
        print("post request sending to ", end_url)
        r = requests.post(end_url, json=response)
        print(r.json())

        self.is_writing[new_path].release()

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