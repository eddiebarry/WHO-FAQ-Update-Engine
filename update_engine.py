import os, sys
from keyword_engine_manager import KeywordEngineManager
from qa_keyword_manager import  QAKeywordManager
from category_question_manager import CategoryQuestionManager


class UpdateEngine:
    """
    Manages Keyword Prediction data

    Manages QA_Keyword_Pairs for search

    Manages Questions per category data
    """
    def __init__(self, 
        keyword_engine_manager=None,
        qa_keyword_manager=None,
        category_question_manager=None):

        self.keyword_engine_manager = keyword_engine_manager
        self.qa_keyword_manager = qa_keyword_manager
        self.category_question_manager = category_question_manager
    
    def add_questions(self, question_array, project_info):
        data_hash_id, project_id, version_id, version_number = project_info
        unique_folder_id = "./data/"+data_hash_id
        index_info = [unique_folder_id, project_id, version_id, version_number]
        self.qa_keyword_manager.add_to_queue(question_array, index_info)

    def remove_questions(self, question_array):
        self.qa_keyword_manager.remove_questions(json_array)

    def add_keywords(self,keyword_array):
        self.qa_keyword_manager.add_keywords(keyword_array)

    def remove_keywords(self,keyword_array):
        self.qa_keyword_manager.remove_keywords(keyword_array)
    
    def add_category_questions(self, cat_q_array):
        self.category_question_manager.add_cat_questions(cat_q_array)

    def remove_category_questions(self, cat_q_array):
        self.category_question_manager.remove_cat_questions(cat_q_array)