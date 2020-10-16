class QAKeywordManager:
    def __init__(self, search_engine, index):
        self.search_engine = search_engine
        self.index = index
    
    def add_questions(self, question_array, folder_id_path):
        if self.index.getIndexDir() != folder_id_path:
            self.index.update_store_dir(folder_id_path) 

        question_array = self.transform_question_array(question_array)
        # store the jsons
        self.index.indexJsonArray(question_array)
        # self.index.print_all_contents()

        # update the search engine to use the new data
        self.search_engine.update(self.index.getIndexDir())

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