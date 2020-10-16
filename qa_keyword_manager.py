class QAKeywordManager:
    def __init__(self, search_engine, index):
        self.search_engine = search_engine
        self.index = index
    
    def add_questions(self, question_array, folder_id_path):
        if self.index.getIndexDir() != folder_id_path:
            self.index.update_store_dir(folder_id_path) 

        # store the jsons
        self.index.indexJsonArray(question_array)
        self.index.print_all_contents()

        # update the search engine to use the new data
        self.search_engine.update(self.index.getIndexDir())
