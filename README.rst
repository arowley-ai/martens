=======
Martens
=======

.. image:: https://img.shields.io/pypi/v/martens.svg
        :target: https://pypi.python.org/pypi/martens

.. image:: https://readthedocs.org/projects/martens/badge/?version=latest
        :target: https://martens.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status

Succinct small scale data manipulation

* Free software: MIT license
* Documentation: https://martens.readthedocs.io.

Usage
-----
To use Martens in a project::

    import martens

The package is available freely on pypi under MIT licence.

About
-----
Martens is a python package for data manipulation in python.
It is designed for data that is too small,for example,
to worry about uploading into a cloud data warehouse for ease of processing
but which is still useful to you.
The kind of data that was probably passed to you in a spreadsheet
or csv file which needs to be transformed quickly into what you want.

The primary aim of Martens is to enable data manipulation code that is:

* Flexible
* Succinct
* Easily Readable and maintainable
* Lightweight

And finally, reasonably performant. That is to say, the intent and philosophy
is not to rely on libraries like numpy which may boost performance compared to
base python. Rather, martens fits neatly around concepts from core python.
This comes with benefits to flexibility and a minimal build profile.

The design is heavily inspired by `dplyr <https://dplyr.tidyverse.org/>`_
from the R universe.

Example code
------------
Importing data is simple::

    source_data = martens.SourceFile(file_path=file_path).dataset

Generally speaking, martens will infer file type from the file extension provided
but you can specify a file type to override it. Access the underlying dataframe
with dataset property.

Dataframes
##########

A martens dataframe is really just a dict of equal length lists and string keys.
Any column of the dataframe can be accessed as follows and the result is always
a list::

    source_data['age']

There's no such thing as a dataframe index in martens, they are not at all useful
for the type of data that martens is designed to parse. But, we can quickly add a
standard, integer based column to use as an id in later steps::

    source_data.with_id('person_id')



Filtering with functions
########################

Filtering is best done with functions or lambdas (but doesn't have to be)::

    source_data.dataset.filter(lambda gender: gender == 'Male')

The key innovation of martens is that argument names of a function
are used within the dataframe. These functions will then operate on data
within the columns with corresponding names to it's own arguments.
That is, argument names of your functions are important and determine
how that function will interact with the dataframe.
This allows for succinct, readable and flexible
code. For example, you can use any function to filter so long as
its argument names correspond to existing columns
in the dataframe and the function returns something that is
ultimately able to be resolved to either true or false.

Mutate and apply
################

Similarly we can quickly create new columns on the fly using data from existing functions::

    source_data.mutate(lambda age: 365*age,'age_in_days')

Again, there is significant flexibility here. Any arbitrary function with any
arbitrary return value will do, as long as all of it's arguments
can be resolved using existing columns of the dataframe.

If you just want the output without adding to the dataframe, use apply::

    source_data.apply(lambda age: 7*age)

Stack, stretch and squish
#########################
Sometimes, we don't want to simply create a new column with the required features.
If the output of your function resolves to a list, you can choose
to stack the output vertically. This will produce a new dataframe
with additional rows and the existing columns expanded (repeated)::

    source_data.mutate_stack(lambda age: list(range(age)),)

We might instead want to create multiple new columns simultaneously::

    source_data.mutate_stretch(some_function_returning_tuple_of_2,names=['A','B'])


More complex code
#################
If you are using martens the way it was intended, your code will tend to have large
blocks of three plus lines of code with each new operation just being a method
of the dataframe from the the previous line. That is, chaining commands is common::

    def solve()
        data = mt.Dataset({'line': [x for x in data_input.split('\n')]})
        num_match = lambda line: [match for match in re.finditer(r'\b\d+\b', line)]
        num_matches = data.with_id('num_line_no') \
            .mutate_stack(num_match, 'match').with_id('num_id') \
            .mutate(lambda match: int(match.group()), name='num_match') \
            .mutate(lambda match: match.start(), name='num_start') \
            .mutate(lambda match: match.end(), name='num_end')
        chr_match = lambda line: [m.start() for m in re.finditer(r'[^.0-9]', line)]
        chr_matches = data.with_id('chr_line_no') \
            .mutate_stack(chr_match, 'chr_match') \
            .with_id('chr_id').select(['chr_line_no', 'chr_match', 'chr_id'])
        all_matches = num_matches.merge(chr_matches) \
            .filter(lambda chr_line_no, num_line_no: abs(chr_line_no - num_line_no) <= 1) \
            .filter(lambda chr_match, num_start, num_end: num_start - 1 <= chr_match <= num_end)
        gear_match = all_matches.group_by(['chr_id'], other_cols=['num_id', 'num_match']) \
            .mutate(lambda num_id: len(num_id), 'num_count') \
            .filter(lambda num_count: num_count >= 2) \
            .mutate(lambda num_match: prod(num_match), 'gear_ratio')
        return {
            'part one': sum(all_matches.unique_by(['num_id', 'num_match'])['num_match']),
            'part two': sum(gear_match['gear_ratio'])
        }

Extensibility
-------------
A martens dataframe can often be used in place of a pandas dataframe or similar
in another package. For example in plotly ::

    import plotly.express as px
    px.bar(dataframe,x='column1',y='column2')

What's next
-----------
This is just the beginning of this project, I hope it is useful to someone, somewhere.
There are many, many feature and speed improvements that I would like to implement.
Of course, feedback is welcome, raise an issue or otherwise get in touch and I'll do my best
to respond.


