import csv


class Manifest:
    def __init__(self, name, file):
        self.name = name
        self.file = file
        self.file_path = file.name
        self.filenames = []
        csv_reader = csv.reader(self.file, delimiter='\t')
        for row in csv_reader:
            self.filenames.append(row[1])

    def __str__(self):
        return f'{self.name} manifest ({len(self.filenames)} files)'
