"""Main module."""
import openpyxl as op
import xlrd
import csv
import json

def __sanitise_column_name__(column_name):
    replace_map = {':': '', ' ': '_', ')': '', '(': '_', '.': '_', "'": '', '%': 'pct', '+': 'plus', '-': '_'}
    column_name = str(column_name).lower()
    for k in replace_map:
        column_name = column_name.replace(k, replace_map[k])
    return column_name


class Dataset(dict):

    def __init__(self, template):
        super().__init__()
        assert isinstance(template, dict) or isinstance(template, list), \
            "Type error: Template is not a dict or list"
        if isinstance(template, dict):
            assert all(isinstance(template[col], list) for col in template), \
                "Type error: Some dictionary entries are not lists"
            assert len(list(set([len(template[column]) for column in template]))) <= 1, \
                "Type Error: Columns must be equal length"
            for col in template:
                self[col] = template[col]
        else:
            assert all(isinstance(record, dict) for record in template), \
                "Type error: Some records are not dicts"
            record_profiles = list(set(tuple(record) for record in template))
            assert len(record_profiles) == 1, \
                "Type Error: Record entries do not have consistent headers"
            for col in record_profiles[0]:
                self[col] = [record[col] for record in template]

    def filter(self, filter_by, var=None):
        if callable(filter_by):
            applied = self.apply(filter_by)
            assert all(isinstance(item, bool) for item in applied), "Some returns are not boolean"
            return Dataset({col: [x[0] for x in zip(self[col], applied) if x[1]] for col in self})
        else:
            assert var is not None, "Var must be supplied unless func is callable"
            return Dataset({col: [x[0] for x in zip(self[col], self[var]) if x[1] == filter_by] for col in self})

    def apply(self, func):
        assert callable(func), "Apply requires a callable argument"
        arg_names = func.__code__.co_varnames[:func.__code__.co_argcount]
        assert all(arg_name in self for arg_name in arg_names), \
            "Function arguments do not correspond to available columns"
        return [func(**{arg: val for arg, val in zip(arg_names, arg_vals)}) for arg_vals in
                zip(*[self[arg] for arg in arg_names])]

    def long_apply(self, func):
        assert callable(func), "Long apply requires a callable argument"
        assert func.__code__.co_argcount == 1, "Functions for long apply must take one argument"
        arg_name = func.__code__.co_varnames[0]
        assert arg_name in self, "Function arguments do not correspond to available columns"
        return func(self[arg_name])

    def mutate(self, mutation, name=None):
        return self.__with__({name if name is not None else mutation.__name__: self.apply(mutation)})

    def squish(self, grouping_cols, headings, values,mutation=list):

        for var, name in zip([headings, values], ['Headings', 'Values']):
            assert isinstance(var, str), name + ' must be a string'
            assert var in self.columns, name + ' must be a column in this dataset'

        all_headings = sorted(set(self[headings]))
        rtn = self.group_by(grouping_cols=grouping_cols, other_cols=[headings, values])

        for heading in all_headings:
            rtn[str(heading)] = [mutation([v for v, h in zip(vals, heads) if h == heading])
                                 for vals, heads in zip(rtn[values], rtn[headings])]

        return rtn.select(grouping_cols + [str(h) for h in all_headings])

    def mutate_stretch(self, mutation, names):
        assert isinstance(names, list) or isinstance(names, dict), "Names should be a list or dict of string:function"
        results = self.apply(mutation)
        assert all([isinstance(r, list) for r in results]), "Some mutate results are not lists"
        assert all([len(r) == len(names) for r in results]), "Some results are not the same length as names"
        if isinstance(names, list):
            new = {name: list(res) for name, res in zip(names, zip(*results))}
        else:
            new = {name: [names[name](x) for x in res] for name, res in zip(names, zip(*results))}
        return Dataset({**self.__existing__, **new})

    def mutate_stack(self, mutation, name=None):
        result = self.apply(mutation)
        assert all([hasattr(r, '__iter__') for r in result]), "Some function results are not iterable"
        existing = {col: [val for val, res in zip(self[col], result) for _ in res] for col in self}
        new = {name if name is not None else mutation.__name__: [v for r in result for v in r]}
        return Dataset({**existing, **new})

    def with_id(self, name='id'):
        return self.__with__({name: list(range(self.__entry_length__))})

    def __with__(self, new):
        return Dataset({**self.__existing__, **new})

    def select(self, names):
        assert isinstance(names, list), "Type error: Not a list of names"
        return Dataset({name: self[name] for name in names})

    def group_by(self, grouping_cols, other_cols=None):
        assert (isinstance(grouping_cols, list)), "Type error: grouping_col should be a list"
        if other_cols is None:
            other_cols = [col for col in self.columns if col not in grouping_cols]
        assert (isinstance(other_cols, list)), "Type error: other_cols should be a list or None"
        assert all(col in self.columns for col in grouping_cols + other_cols), \
            "Records error: Missing columns"
        rtn = self.unique_by(grouping_cols)
        all_records = rtn.records
        for col in other_cols:
            rtn[col] = [
                [inr[col] for inr in self.records if all(rec[name] == inr[name] for name in rec)]
                for rec in all_records]
        return rtn

    # def group_by(self, names, mutations={}, trim=False):
    #     assert (isinstance(mutations, dict)), "Type error: Mutations is not a dict"
    #     assert (isinstance(names, list)), "Type error: Names should be a list"
    #     rtn = self.unique_by(names)
    #     all_records = rtn.records
    #     all_columns = list(mutations) + names if trim else self.columns
    #     for col in all_columns:
    #         if col not in names:
    #             primitive = [[inr[col] for inr in self.records if all(rec[name] == inr[name] for name in rec)] for rec
    #                          in all_records]
    #             rtn[col] = [mutations[col](p) for p in primitive] if col in mutations else primitive
    #     return rtn

    def unique_by(self, names):
        return Dataset({name: list(val) for name, val in zip(names, zip(*sorted(set(zip(*[self[n] for n in names])))))})

    def merge(self, right):
        assert isinstance(right, Dataset), "Type error: not a dataset"
        assert self.column_length + right.column_length == len(set(self.columns + right.columns))
        left_length, right_length = self.record_length, right.record_length
        return Dataset({
            **{col: [val for val in self[col] for _ in range(right_length)] for col in self.columns},
            **{col: [val for _ in range(left_length) for val in right[col]] for col in right.columns}
        })

    def generator(self, names=None):
        return zip(*[self[name] for name in (names if names is not None else self.columns)])


    @property
    def records(self):
        return [{col: row for col, row in zip(self.columns, row)} for row in zip(*[self[col] for col in self])]

    @property
    def first(self):
        return {col: self[col][0] for col in self}

    @property
    def record_length(self):
        return len(self.records)

    @property
    def columns(self):
        return [col for col in self]

    @property
    def column_length(self):
        return len(self.columns)

    @property
    def pretty(self):
        return json.dumps(self,indent=4)

    @property
    def __entry_length__(self):
        return len(self[[x for x in self][0]])

    @property
    def __existing__(self):
        return {col: self[col] for col in self}

def stack(list_of_datasets: list):
    assert isinstance(list_of_datasets, list), "Type error: Not a list"
    assert all([isinstance(element, Dataset) for element in list_of_datasets]), "Type error : Not a list of Datasets"
    cols = sorted([x for x in list_of_datasets[0]])
    assert (all([cols == sorted([x for x in y]) for y in list_of_datasets])), "Available columns do not correspond"
    return Dataset({col: [val for element in list_of_datasets for val in element[col]] for col in cols})


class SourceFile:

    def __init__(self, file_path, sheet_name="Sheet1", from_row=0, from_col=0,
                 file_type=None, to_row=None, to_col=None):
        self.file_path = file_path
        file_tokens = file_path.split('.')
        assert len(file_tokens) > 1, "Data Error: Please include file extension in path"
        self.file_type = file_tokens[-1] if file_type is None else file_type
        self.sheet_name = sheet_name
        self.from_row = from_row
        self.from_col = from_col
        self.to_row = to_row
        self.to_col = to_col

    @property
    def dataset(self):
        return getattr(self, self.file_type)

    @property
    def xlsx(self):
        workbook = op.load_workbook(filename=self.file_path, data_only=True)
        sheet = workbook[self.sheet_name]
        trim_col = len([x for x in sheet.columns]) if self.to_col is None else self.to_col
        return Dataset({
            __sanitise_column_name__(col[self.from_row].value):
                [cell.value for cell in col[(self.from_row + 1):self.to_row]]
            for index, col in enumerate(sheet.columns) if index < trim_col
        })

    @property
    def xls(self):
        workbook = xlrd.open_workbook(self.file_path)
        sheet = workbook.sheet_by_name(self.sheet_name)
        col_limit = sheet.ncols if self.to_col is None else self.to_col
        columns = [sheet.col_values(col) for col in range(self.from_col, col_limit)]
        return Dataset({
            __sanitise_column_name__(col[self.from_row]):
                [cell if cell != '' else None for cell in col[(self.from_row + 1):self.to_row]]
            for col in columns
        })

    @property
    def csv(self):
        reader = csv.reader(open(self.file_path))
        _ = [next(reader, None) for x in range(self.from_row)]
        headers = [__sanitise_column_name__(w) for w in next(reader, None)][0:self.to_col]
        rawdata = [list(d) for d in zip(*[l for l in reader])][0:self.to_col]
        return Dataset({h: d for h, d in zip(headers, rawdata)})
