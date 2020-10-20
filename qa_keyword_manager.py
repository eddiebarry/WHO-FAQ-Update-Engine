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
        # threading.Lock()
    
    def add_to_queue(self,question_array, folder_id_path):
        self.queue.append( (question_array, folder_id_path) )
        if folder_id_path not in self.is_writing.keys():
            self.is_writing[folder_id_path]=threading.Lock()
        # if not self.is_writing:
        #     self.is_writing = True
        self.pool.submit(self.add_questions)
        # thread = Thread(\
        #         target=self.add_questions
        #     )
        # thread.start()
    
    def add_questions(self):
        question_array, folder_id_path = self.queue.popleft()
        self.is_writing[folder_id_path].acquire()
        if self.index.getIndexDir() != folder_id_path:
            self.index.update_store_dir(folder_id_path) 

        question_array = self.transform_question_array(question_array)
        # store the jsons
        self.index.indexJsonArray(question_array)
        # self.index.print_all_contents()

        # update the search engine to use the new data
        self.search_engine.update(self.index.getIndexDir())
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