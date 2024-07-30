#!/usr/bin/env python
import pytest

from martens import martens


@pytest.mark.parametrize("file_path", [
    './tests/test_data/file_example_XLSX_10.xlsx',
    './tests/test_data/file_example_XLS_10.xls',
])
def test_total_ages_women(file_path):
    total_ages_women = martens.SourceFile(file_path=file_path) \
        .dataset.headings_lower.filter(lambda gender: gender == 'Female') \
        .long_apply(lambda age: sum(age))
    assert total_ages_women == 263
