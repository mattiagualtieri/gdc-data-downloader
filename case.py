class Case:
    def __init__(self, case_id, genexpr_filename='', meth_filename=''):
        self.case_id = case_id
        self.genexpr_filename = genexpr_filename
        self.meth_filename = meth_filename

    def set_filename(self, filename, data_type):
        if data_type == 'genexpr':
            self.genexpr_filename = filename
        else:
            self.meth_filename = filename

    def get_filename(self, data_type):
        if data_type == 'genexpr':
            return self.genexpr_filename
        else:
            return self.meth_filename

    def is_complete(self):
        if not self.genexpr_filename or not self.meth_filename:
            return False
        return True

    def __str__(self):
        return f'{self.genexpr_filename}\t{self.meth_filename}'
