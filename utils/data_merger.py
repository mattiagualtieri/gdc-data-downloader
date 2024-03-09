import os
from utils.data_parser import DataParser


class DataMerger:
    def __init__(self, base_dir, mapping_list, output):
        self.base_dir = base_dir
        self.mapping_list = mapping_list
        self.output = output
        self.parser = DataParser()

    def merge(self):
        print(f'Merging data into {self.output}')
        with open(self.output, 'w') as out:
            self.write_header(out)
            for case_id in self.mapping_list.mappings:
                out.write(case_id)
                case = self.mapping_list.mappings[case_id]
                for data_type in case:
                    mapping = case[data_type]
                    data_type_dir = os.path.join(self.base_dir, data_type)
                    file = DataMerger.search_file(data_type_dir, mapping.filename)
                    out.write(self.parser.parse(data_type, file))
                out.write('\n')
        print('Merge completed')

    def write_header(self, out):
        for case_id in self.mapping_list.mappings:
            out.write('case_id')
            out.write(';')
            case = self.mapping_list.mappings[case_id]
            for data_type in case:
                mapping = case[data_type]
                data_type_dir = os.path.join(self.base_dir, data_type)
                file = DataMerger.search_file(data_type_dir, mapping.filename)
                out.write(self.parser.parse(data_type, file, header=True))
            out.write('\n')
            return

    @staticmethod
    def search_file(directory, file_to_search):
        for subdir in os.listdir(directory):
            subdir_path = os.path.join(directory, subdir)
            if os.path.isdir(subdir_path):
                for filename in os.listdir(subdir_path):
                    file_path = os.path.join(subdir_path, filename)
                    if os.path.isfile(file_path) and filename == file_to_search:
                        return file_path
