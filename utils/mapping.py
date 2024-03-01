import json


class MappingList:
    def __init__(self, name, file=None):
        self.name = name
        self.data_type = name
        self.file = file
        self.mappings = {}
        if file is not None:
            json_mapping = json.load(file)
            for case in json_mapping:
                case_id = case['cases'][0]['case_id']
                filename = case['file_name']
                mapping = Mapping(self.data_type, filename)
                self.mappings[case_id] = {}
                self.mappings[case_id][self.data_type] = mapping

    def add_mapping(self, case_id, mapping):
        if case_id not in self.mappings:
            self.mappings[case_id] = {}
            self.mappings[case_id][mapping.data_type] = mapping
        else:
            self.mappings[case_id][mapping.data_type] = mapping

    def remove_incomplete(self, num_data):
        self.mappings = {case_id: filenames for case_id, filenames in self.mappings.items() if len(filenames) >= num_data}

    def cut(self, max_cases):
        self.mappings = {case_id: self.mappings[case_id] for case_id in list(self.mappings.keys())[:max_cases]}

    def get_cases(self):
        return self.mappings.keys()

    def __str__(self):
        return f'{self.name} mapping ({len(self.mappings)} pairs)'


class Mapping:
    def __init__(self, data_type, filename):
        self.data_type = data_type
        self.filename = filename

    def __str__(self):
        return f'{self.data_type}, {self.filename}'
