import xml.etree.ElementTree as et
import csv


class DataParser:
    def parse(self, data_type, file, header=False):
        if data_type == 'labels':
            return self.parse_label(file, header)
        elif data_type == 'genexpr':
            return self.parse_genexpr(file, header)
        elif data_type == 'wsi':
            return self.parse_wsi(file, header)
        else:
            raise ValueError(f'Data type "{data_type}" not supported')

    @staticmethod
    def parse_label(file, header=False):
        if header:
            return 'overall_survival'
        namespace = {'clin_shared': 'http://tcga.nci/bcr/xml/clinical/shared/2.7'}
        tree = et.parse(file)
        root = tree.getroot()
        elems = root.findall('.//clin_shared:days_to_death', namespace)
        dtd = 0
        for elem in elems:
            if elem.text is not None:
                if int(elem.text) > 0:
                    dtd = int(elem.text)
        return f';{str(dtd)}'

    @staticmethod
    def parse_genexpr(file, header=False):
        result = []
        line_count = 0
        with open(file) as genexpr_file:
            csv_reader = csv.reader(genexpr_file, delimiter='\t')
            for row in csv_reader:
                line_count += 1
                if line_count >= 7:
                    if header:
                        result.append(';')
                        result.append(row[1])
                    else:
                        result.append(';')
                        result.append(row[8])
        return ''.join(result)

    @staticmethod
    def parse_wsi(file, header=False):
        if header:
            return ';wsi'
        return f';{file}'
