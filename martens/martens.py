"""Main module."""
import deprecation
import openpyxl as op
import xlrd
import csv
import json
import datetime
import re
import inspect


# Main purpose of martens, dataset class
class Dataset(dict):

    # To initialise a Dataset should be either:
    # -> A dict of {string : list} of equal length or
    # -> A list of dicts of {column_name : value,}
    # In the latter case the engine will generate None values for any missing column names in each record
    def __init__(self, template, sanitise_names=False):
        super().__init__()
        assert isinstance(template, dict) or isinstance(template, list), \
            "Type error: Template is not a dict or list"
        if isinstance(template, dict):
            assert all(isinstance(template[col], list) for col in template), \
                "Type error: Some dictionary entries are not lists"
            assert len(list(set([len(template[column]) for column in template]))) <= 1, \
                "Type Error: Columns must be equal length"
            for col in template:
                if sanitise_names:
                    self[__sanitise_column_name__(col)] = template[col]
                else:
                    self[col] = template[col]
        else:
            assert all(isinstance(record, dict) for record in template), \
                "Type error: Some records are not dicts"
            record_profiles = list(set(tuple(record) for record in template))
            for col in record_profiles[0]:
                self[col] = [record[col] if col in record else None for record in template]

    def slice(self, start=None, stop=None, step=None):
        return Dataset({col: self[col][slice(start, stop, step)] for col in self.columns})

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
        params = inspect.signature(func).parameters
        assert all(param in self for param in params), \
            "Function arguments do not correspond to available columns"
        return [func(**{arg: val for arg, val in zip(params, arg_vals)}) for arg_vals in
                zip(*[self[param] for param in params])]

    def long_apply(self, func):
        assert callable(func), "Long apply requires a callable argument"
        params = inspect.signature(func).parameters
        assert all(param in self for param in params), \
            "Function arguments do not correspond to available columns"
        return func(**{param: self[param] for param in params})

    def window_apply(self, func, window_size=1):
        assert callable(func), "Window apply requires a callable argument"
        params = inspect.signature(func).parameters
        assert len(params) == 1, "Window function can only accept one argument"
        name = next(iter(params))
        return [func(self[name][max(0, i - window_size + 1):i + 1]) for i in range(self.record_length)]

    def mutate(self, mutation, name=None):
        return self.__with__({name if name is not None else mutation.__name__: self.apply(mutation)})

    def long_mutate(self, mutation, name=None):
        result = self.long_apply(mutation)
        assert isinstance(result, list), "Some returns are not lists"
        assert len(result) == self.record_length, "Some returns are not same length as record length"
        return self.__with__({name if name is not None else mutation.__name__: result})

    def window_mutate(self, mutation, window_size=1, name=None):
        result = self.window_apply(mutation, window_size=window_size)
        return self.__with__({name if name is not None else mutation.__name__: result})

    def replace(self, mutation, included_names=None, excluded_names=None):
        if included_names is not None:
            names = included_names
        elif excluded_names is not None:
            names = [n for n in self.columns if n not in excluded_names]
        else:
            return self
        return self.__with__({name: [mutation(c) for c in self[name]] for name in names})

    # This is the pivot table where you make a column into lots of headings
    def column_squish(self, grouping_cols, headings, values, prefix=''):
        for var, name in zip([headings, values], ['Headings', 'Values']):
            assert isinstance(var, str), name + ' must be a string'
            assert var in self.columns, name + ' must be a column in this dataset'
        rtn = self.group_by(grouping_cols=grouping_cols, other_cols=[headings, values])
        new_headings = sorted(set(self[headings]))
        for heading in new_headings:
            rtn[prefix + heading] = [next(
                (value for heading_inner, value in zip(rec[headings], rec[values])
                 if heading_inner == heading
                 ), None) for rec in rtn.records]
        return rtn.select(grouping_cols + [prefix + h for h in new_headings])

    # This is kind of the reverse pivot where you stack lots of headings on top of each other
    def headings_squish(self, grouping_cols, headings, value_name, heading_name):
        return stack([Dataset({
            **{g: self[g] for g in grouping_cols},
            heading_name: [h] * len(self[h]),
            value_name: self[h]
        }) for h in headings])

    # These variants of mutate deal with functions that output multiple value where you want multiple columns
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

    # ... or where you want to stack the results
    # TODO: Can I use either with or existing in this section
    def mutate_stack(self, mutation, name=None, save_len=None):
        result = self.apply(mutation)
        assert all([hasattr(r, '__iter__') for r in result]), "Some function results are not iterable"
        existing = {col: [val for val, res in zip(self[col], result) for _ in res] for col in self}
        new = {name if name is not None else mutation.__name__: [v for r in result for v in r]}
        if save_len is not None:
            new[save_len] = [len(r) for r in result for _ in r]
        return Dataset({**existing, **new})

    # TODO: Check if the records are all dicts
    # This function is great for stretching out record data into
    def record_stretch(self, name, drop=True):
        seen = set()
        all_keys = []
        for record in self[name]:
            for key in record:
                if key not in seen:
                    seen.add(key)
                    all_keys.append(key)
        new = {key: [rec[key] if key in rec else None for rec in self[name]] for key in all_keys}
        return Dataset({**self.__without__([name] if drop else []), **new})

    # TODO: Check if all the records are lists
    # This is where you have a column which just has lists and you need those lists over multiple rows
    def column_stack(self, name):
        existing = {col: [val for val, res in zip(self[col], self[name]) for _ in res] for col in self if
                    col not in name}
        new = {name: [val for rec in self[name] for val in rec]}
        return Dataset({**existing, **new})

    def json_explode(self, name):
        in_scope_columns = [name]
        rtn = self
        while in_scope_columns:
            col = in_scope_columns.pop()
            if all(isinstance(element, dict) for element in rtn[col]):
                old_cols = rtn.columns
                rtn = rtn.record_stretch(col)
                in_scope_columns.extend([col for col in rtn.columns if col not in old_cols])
            elif all(isinstance(element, list) for element in rtn[col]):
                rtn = rtn.column_stack(col)
                in_scope_columns.append(col)
        return rtn

    # Adding a simple ID to a dataset
    def with_id(self, name='id'):
        return self.__with__({name: list(range(self.__entry_length__))})

    # Adding a simple constant to a dataset
    def with_constant(self, value, name):
        return self.__with__({name: [value] * self.record_length})

    # Adding multiple constants to a dataset using (name, value) dict
    def with_constants(self, input_dict):
        assert isinstance(input_dict, dict), "input_dict should be a dictionary"
        rtn = self
        for key in input_dict:
            rtn = rtn.with_constant(input_dict[key], key)
        return rtn

    def __with__(self, new):
        return Dataset({**self.__existing__, **new})

    def select(self, names):
        assert isinstance(names, list), "Type error: Not a list of names"
        return Dataset({name: self[name] for name in names})

    def drop(self, names):
        assert isinstance(names, list), "Type error: Not a list of names"
        return Dataset({name: self[name] for name in self.columns if name not in names})

    # Neat little sorting function
    def sort(self, names, reverse=False):
        sort_order = names + [col for col in self.columns if col not in names]
        sorted_data = sorted(zip(*[self[col] for col in sort_order]), reverse=reverse, key=lambda x: x[0:len(names)])
        rtn = {c: list(v) for c, v in zip(sort_order, zip(*sorted_data))}
        return Dataset({col: rtn[col] for col in sort_order})

    def group_by(self, grouping_cols, other_cols=None, count='count'):
        assert isinstance(grouping_cols, list), "Type error: grouping_col should be a list"
        if other_cols is None:
            other_cols = [col for col in self.columns if col not in grouping_cols]
        assert isinstance(other_cols, list), "Type error: other_cols should be a list or None"
        assert isinstance(count, str), "Type error: with_count should be a string"

        sorts = self.sort(grouping_cols)

        last_grouped = None
        rtn = dict()

        for col in grouping_cols + other_cols + [count]:
            rtn[col] = list()

        for rec in sorts.records:
            grouped = [rec[g] for g in grouping_cols]
            if grouped == last_grouped:
                for o in other_cols:
                    rtn[o][-1].append(rec[o])
                rtn[count][-1] = rtn[count][-1] + 1
            else:
                for g in grouping_cols:
                    rtn[g].append(rec[g])
                for o in other_cols:
                    rtn[o].append([rec[o]])
                rtn[count].append(1)
            last_grouped = grouped

        return Dataset(rtn)

    def unique_by(self, names):
        return Dataset({name: list(val) for name, val in zip(names, zip(*sorted(set(zip(*[self[n] for n in names])))))})

    @deprecation.deprecated("Use merge instead")
    def merge_by_key(self, right, key_column, how='inner'):
        return self.merge(right, on=[key_column], how=how)

    def merge(self, right, on=None, how='inner'):

        # TODO: handle error if user tries to merge on columns that don't exist
        # TODO: handle error if user tries to merge a dataset containing only the key columns
        assert isinstance(right, Dataset), "Type error: Right is not a dataset"
        assert isinstance(on, list), "Type error: Keys are not a list"
        assert how in ['inner', 'left', 'right', 'full'], "Expecting how to be 'inner', 'left', 'right' or 'full'"

        if on is None:
            return self.full_outer_merge(right)

        def tuple_key(cols):
            return tuple(cols[i] for i in range(len(on)))

        left_sorted = self.sort(on)
        right_sorted = right.sort(on)

        left_keys = set(tuple(rec[key] for key in on) for rec in left_sorted.records)
        right_keys = set(tuple(rec[key] for key in on) for rec in right_sorted.records)

        left_columns = left_sorted.columns
        right_columns = right_sorted.columns

        left_zipped = list(zip(*[left_sorted[col] for col in left_columns]))
        right_zipped = list(zip(*[right_sorted[col] for col in right_columns]))

        left_zip_len = len(left_zipped)
        right_zip_len = len(right_zipped)

        rtn = Dataset({col: [] for col in left_columns + [c for c in right_columns if c not in left_columns]})

        if how == 'inner':
            all_keys = sorted(left_keys & right_keys)
        elif how == 'right':
            all_keys = sorted(right_keys)
        elif how == 'left':
            all_keys = sorted(left_keys)
        else:
            all_keys = sorted(left_keys | right_keys)

        left_organised, right_organised = [], []
        for which, zipped, zipped_len in [('left', left_zipped, left_zip_len), ('right', right_zipped, right_zip_len)]:
            index = 0
            for key in all_keys:
                to_add = []
                while index < zipped_len:
                    key_tuple = tuple_key(zipped[index])
                    if key_tuple < key:
                        index = index + 1
                    elif key_tuple == key:
                        to_add.append(zipped[index])
                        index = index + 1
                    else:
                        break

                if not to_add and how not in [which, 'inner']:
                    to_add = [[None] * len(left_columns)]

                if which == 'left':
                    left_organised.append(to_add)
                else:
                    right_organised.append(to_add)

        for key, left_to_add, right_to_add in zip(all_keys, left_organised, right_organised):
            for left_add in left_to_add:
                for right_add in right_to_add:
                    for index, col in enumerate(on):
                        rtn[col].append(key[index])

                    for index, col in enumerate(left_columns):
                        if col not in on:
                            rtn[col].append(left_add[index])

                    for index, col in enumerate(right_columns):
                        if col not in on:
                            rtn[col].append(right_add[index])

        return rtn

    def full_outer_merge(self, right):
        assert isinstance(right, Dataset), "Type error: not a dataset"
        assert self.column_length + right.column_length == len(set(self.columns + right.columns))
        left_length, right_length = self.record_length, right.record_length
        return Dataset({
            **{col: [val for val in self[col] for _ in range(right_length)] for col in self.columns},
            **{col: [val for _ in range(left_length) for val in right[col]] for col in right.columns}
        })

    def rename(self, rename_map):
        return Dataset({(rename_map[c] if c in rename_map else c): self[c] for c in self.columns})

    def rename_and_select(self, rename_map):
        return Dataset({rename_map[c]: self[c] for c in rename_map})

    def generator(self, names=None):
        return zip(*[self[name] for name in (names if names is not None else self.columns)])

    def write_csv(self, file_path):
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.columns)
            writer.writeheader()
            for record in self.records:
                writer.writerow(record)

    def fill_none(self, value):
        return Dataset({col: [value if val is None else val for val in self[col]] for col in self.columns})

    def __str__(self):
        columns = self.columns
        print_widths = [max([len(val.__str__()) for val in self[col]] + [len(col)]) + 1 for col in columns]
        rtn = '|'
        for column, width in zip(columns, print_widths):
            rtn = rtn + column.ljust(width) + '|'
        rtn = rtn + '\n'
        for record in self.records:
            rtn = rtn + '|'
            for column, width in zip(columns, print_widths):
                rtn = rtn + record[column].__str__().ljust(width) + '|'
            rtn = rtn + '\n'
        return rtn

    @property
    def headings_camel_to_snake(self):
        return Dataset({__camel_to_snake__(col): self[col] for col in self.columns})

    @property
    def headings_lower(self):
        return Dataset({col.lower(): self[col] for col in self.columns})


    @property
    def rows(self):
        return [row for row in zip(*[self[col] for col in self])]

    @property
    def records(self):
        return [{col: val for col, val in zip(self.columns, row)} for row in zip(*[self[col] for col in self])]

    @property
    def records_sparse(self):
        return [{col: val for col, val in zip(self.columns, row) if val is not None} for row in
                zip(*[self[col] for col in self])]

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
        return json.dumps(self, indent=4)

    @property
    def __entry_length__(self):
        return len(self[[x for x in self][0]])

    @property
    def __existing__(self):
        return {col: self[col] for col in self}

    def __without__(self, without):
        return {col: self[col] for col in self if col not in without}


# A class used to parse data from source files and access the Dataset
class SourceFile:

    def __init__(self, file_path, sheet_name="Sheet1", from_row=1, from_col=1,
                 file_type=None, to_row=None, to_col=None, date_columns=None, using_range=None):
        self.file_path = file_path
        file_tokens = file_path.split('.')
        assert len(file_tokens) > 1, "Data Error: Please include file extension in path"
        self.file_type = file_tokens[-1] if file_type is None else file_type
        self.sheet_name = sheet_name
        self.from_row = from_row
        self.from_col = from_col
        self.to_row = to_row
        self.to_col = to_col
        self.date_columns = [] if date_columns is None else date_columns
        if using_range is not None:
            self.from_row, self.from_col, self.to_row, self.to_col = parse_excel_range(using_range)

    @property
    def dataset(self):
        return getattr(self, self.file_type)

    def conditional_xls_float_to_date(self, value, book, index):
        return datetime.datetime(
            *xlrd.xldate_as_tuple(value, book.datemode)).date() if index in self.date_columns else value

    @property
    def xlsx(self):
        workbook = op.load_workbook(filename=self.file_path, data_only=True)
        sheet = workbook[self.sheet_name]
        trim_col = len([x for x in sheet.columns]) if self.to_col is None else self.to_col
        return Dataset({
            __sanitise_column_name__(col[self.from_row - 1].value):
                [cell.value for cell in col[self.from_row:self.to_row]]
            for index, col in enumerate(sheet.columns) if index < trim_col
        })

    @property
    def xls(self):
        book = xlrd.open_workbook(self.file_path)
        sheet = book.sheet_by_name(self.sheet_name)
        col_limit = sheet.ncols if self.to_col is None else self.to_col
        columns = [sheet.col_values(col) for col in range(self.from_col - 1, col_limit)]
        return Dataset({
            __sanitise_column_name__(col[self.from_row - 1]):
                [
                    self.conditional_xls_float_to_date(cell, book, index)
                    if cell != '' else None for cell in col[self.from_row:self.to_row]]
            for index, col in enumerate(columns)
        })

    @property
    def csv(self):
        reader = csv.reader(open(self.file_path))
        _ = [next(reader, None) for _ in range(self.from_row - 1)]
        headers = [__sanitise_column_name__(w) for w in next(reader, None)][(self.from_col - 1):self.to_col]
        rawdata = [list(d) for d in zip(*[r for r in reader])][(self.from_col - 1):self.to_col]
        return Dataset({h: d for h, d in zip(headers, rawdata)})


def __sanitise_column_name__(column_name):
    replace_map = {
        ':': '',
        ' ': '_',
        ')': '',
        '(': '_',
        '.': '_',
        "'": '',
        '%': 'pct',
        '+': 'plus',
        '-': '_',
        '\ufeff': '',
        '"': ''
    }
    column_name = str(column_name)
    for k in replace_map:
        column_name = column_name.replace(k, replace_map[k])
    return column_name


def __camel_to_snake__(camel):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', camel).lower()


def initialise(id_length, id_name='id'):
    return Dataset({id_name: list(range(id_length))})


def average(input_list):
    return sum(input_list) / len(input_list)


def stack(list_of_datasets: list):
    assert isinstance(list_of_datasets, list), "Type error: Not a list"
    assert all([isinstance(element, Dataset) for element in list_of_datasets]), "Type error : Not a list of Datasets"
    cols = sorted([x for x in list_of_datasets[0]])
    assert (all([cols == sorted([x for x in y]) for y in list_of_datasets])), "Available columns do not correspond"
    return Dataset({col: [val for element in list_of_datasets for val in element[col]] for col in cols})


def excel_column_name_to_number(col):
    num = 0
    for c in col:
        num = num * 26 + ord(c.upper()) - ord('A') + 1
        return num


def parse_excel_range(excel_range):
    pattern = r'^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$'
    if match := re.match(pattern, excel_range):
        start_col_str, start_row_str, end_col_str, end_row_str = match.groups()
        start_col = excel_column_name_to_number(start_col_str)
        end_col = excel_column_name_to_number(end_col_str)
        start_row = int(start_row_str)
        end_row = int(end_row_str)
        return start_row, start_col, end_row, end_col
    else:
        return 1, 1, None, None
