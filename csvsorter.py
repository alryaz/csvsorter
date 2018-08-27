# Standard Library imports
import csv
import heapq
import logging
from multiprocessing import Pool
import os
import shutil
import sys

# PYPI Package imports
from natsort import ns, natsort_keygen
from pprintpp import pformat


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stderr)
console_handler.setLevel(logging.DEBUG)
LOG.addHandler(console_handler)


class CsvSortError(Exception):
    pass

def csvsort(input_filename, columns, column_types=str, key_funcs=str, output_filename='', max_size=100,
            has_header=True, delimiter=',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8', num_parallel=4,
            direction='ascending', tmp_dir_root=None):
    """Sort the CSV file on disk rather than in memory
    The merge sort algorithm is used to break the file into smaller sub files and

    :param input_filename: the CSV filename to sort
    :param columns: a list of column to sort on (can be 0 based indices or header keys)
    :param column_types: a function or list of functions to cast the sorted columns before sorting. If provided a single function, it is applied to all columns
    :param key_funcs: a function of lsit of functions to use as the value for **sorting** the columns.
    :param output_filename: optional filename for sorted file. If not given then input file will be overriden.
    :param max_size: the maximum size (in MB) of CSV file to load in memory at once
    :param has_header: whether the CSV contains a header to keep separated from sorting
    :param delimiter: character used to separate fields, default ','
    :param quoting: type of quoting used in the output
    :param encoding: file encoding used in input/output files
    :param num_parallel: how many chunks to sort in memory at once, default: 4
    :param direction: Set the sorting order: 'ascending' or 'descending'
    :param tmp_dir_root: Root directory to use for the .csvsorter.PID_NUMBER working directories.
    """
    tmp_dir_root = tmp_dir_root or os.getcwd()
    tmp_dir = os.path.join(tmp_dir_root, '.csvsorter.{}'.format(os.getpid()))
    os.makedirs(tmp_dir, exist_ok=True)

    # if column_types is not a list, make it one
    if not isinstance(column_types, list):
      column_types = [column_types] * len(columns)

    # if key_funcs is not a list, makt it one
    if not isinstance(key_funcs, list):
        key_funcs = [key_funcs] * len(columns)

    assert(len(column_types) == len(columns))
    assert(len(key_funcs) == len(columns))

    # max per parallel sort
    num_parallel = os.cpu_count()
    max_size /= num_parallel

    try:
        with open(input_filename, 'r', encoding=encoding) as input_fp:
            reader = csv.reader(input_fp, delimiter=delimiter)
            if has_header:
                header = next(reader)
            else:
                header = None

            columns = parse_columns(columns, header)

            # sort files in memory concurrently
            filenames = csvsplit(reader, max_size, encoding, tmp_dir)
            with Pool(num_parallel) as pool:
                tasks = [(memorysort, f, columns, column_types, key_funcs, encoding, direction) for f in filenames]
                pool.map(pool_helper, tasks)

            # merge sorted files
            sorted_filename = mergesort(filenames, columns, column_types, key_funcs, tmp_dir=tmp_dir, encoding=encoding, num_parallel=num_parallel, direction=direction)

        # XXX make more efficient by passing quoting, delimiter, and moving result
        # generate the final output file
        with open(output_filename or input_filename, 'w', newline='\n', encoding=encoding) as output_fp:
            writer = csv.writer(output_fp, delimiter=delimiter, quoting=quoting)
            if header:
                writer.writerow(header)
            with open(sorted_filename, 'r', encoding=encoding) as sorted_fp:
                rows = csv.reader(sorted_fp)
                writer.writerows(rows)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def parse_columns(columns, header):
    """check the provided column headers
    """
    for i, column in enumerate(columns):
        if isinstance(column, int):
            if (header and column >= len(header)):
                raise CsvSortError('Column index is out of range: "{}"'.format(column))
        else:
            # find index of column from header
            if header:
                if column in header:
                    columns[i] = header.index(column)
                else:
                    raise CsvSortError('Column name is not found in header: "{}"'.format(column))
            else:
                raise CsvSortError('CSV needs a header to find index of this column name: "{}"'.format(column))
    return columns


def csvsplit(reader, max_size, encoding, tmp_dir):
    """Split into smaller CSV files of maximum size and return the list of filenames
    """
    max_size = max_size * 1024 * 1024 # convert to bytes
    writer = None
    current_size = 0
    split_filenames = []
    fout = None

    # break CSV file into smaller merge files
    for row in reader:
        if not writer:
            filename = os.path.join(tmp_dir, 'split{}.csv'.format(len(split_filenames)))
            fout = open(filename, 'w', newline='\n', encoding=encoding)
            writer = csv.writer(fout)
            split_filenames.append(filename)

        writer.writerow(row)
        current_size += sys.getsizeof(row)
        if current_size > max_size:
            writer = None
            fout.close()
            current_size = 0

    if not fout.closed:
      fout.close()
    return split_filenames


def memorysort(filename, columns, col_types, key_funcs, encoding, direction):
    """Sort this CSV file in memory on the given columns
    """
    with open(filename, encoding=encoding) as input_fp:
        rows = list(csv.reader(input_fp))
    N = len(columns)
    func = lambda row : [key_funcs[n](row[columns[n]]) for n in range(N)]
    natsort_key = natsort_keygen(key=func, alg=ns.IGNORECASE)
    if direction == 'descending':
        #rows.sort(key=func, reverse=True)
        rows.sort(key=natsort_key, reverse=True)
    elif direction == 'ascending':
        #rows.sort(key=func)
        rows.sort(key=natsort_key)
    with open(filename, 'w', newline='\n', encoding=encoding) as output_fp:
        writer = csv.writer(output_fp)
        writer.writerows(rows)


def memorysort_helper(memsort_args):
    """ A helper function for memorysort() that just unpacks a tuple of of args into the function arguments.
    """
    memorysort(*memsort_args)


def yield_csv_rows(filename, columns, col_types, encoding):
    """Iterator to sort CSV rows
    """
    with open(filename, 'r', encoding=encoding) as fp:
        for row in csv.reader(fp):
            # cast fields to final desired types
            for c in range(len(columns)):
                row[columns[c]] = col_types[c](row[columns[c]])
            yield row

def merge(filenames, output, columns, col_types, key_funcs, encoding, direction, last_merge=False):
    """ Merge 'filenames' into 'output'
    """
    LOG.debug('\nmerging {} \nto {}\n'.format(pformat(filenames), output))
    if len(filenames) == 1:
        LOG.debug("moving {} to {}".format(filenames[0], output))
        shutil.move(filenames[0], output)
        return

    with open(output, 'w', newline='\n', encoding=encoding) as output_fp:
        writer = csv.writer(output_fp)
        rows = (yield_csv_rows(filename, columns, col_types, encoding) for filename in filenames)
        N = len(columns)
        keyfunc = lambda row: [key_funcs[n](row[columns[n]]) for n in range(N)]
        natsort_key = natsort_keygen(key=keyfunc, alg=ns.IGNORECASE)
        if direction == 'descending':
            # NOTE: heapq.merge() only as the reverse keyword argument as of Python 3.5
            writer.writerows(heapq.merge(*rows, key=natsort_key, reverse=True))
        elif direction == 'ascending':
            writer.writerows(heapq.merge(*rows, key=natsort_key))

def pool_helper(args):
    """ A helper function that accepts a tuple whose first element is a function, and the remaining are its positional arguments.
    """
    func = args[0]
    func(*args[1:])


def mergesort(sorted_filenames, columns, col_types, key_funcs, nway=2, tmp_dir='', encoding='utf-8', num_parallel=4, direction='ascending'):
    """Merge the sorted csv files into a single output file.
    """
    merge_n = 0
    last_merge = False
    LOG.debug("start: {}".format(pformat(sorted_filenames)))
    while len(sorted_filenames) > 1:
        # Build current level of the merge tree
        LOG.debug("loop: {}".format(pformat(sorted_filenames)))
        tasks = []
        outputs = []
        N = len(sorted_filenames)
        LOG.debug("N: {}".format(N))
        for i in range(0, N, nway):
            files = sorted_filenames[i:min(N,i+nway)]
            outputs.append(os.path.join(tmp_dir, 'merge{}.csv'.format(merge_n)))
            if N == 2:
                LOG.debug("last merge?")
                last_merge = True
            tasks.append([merge, files, outputs[-1], columns, col_types, key_funcs, encoding, direction, last_merge])
            merge_n += 1
        assert(tasks[-1][1][-1] == sorted_filenames[-1])

        with Pool(num_parallel) as pool:
            pool.map(pool_helper, tasks)

        # delete the now sorted files
        for filename in sorted_filenames:
            try:
                os.remove(filename)
            except FileNotFoundError:
                LOG.debug("filename {} already removed?".format(filename))
                pass

        # for next iteration
        sorted_filenames = outputs

    LOG.debug("end: {}".format(pformat(sorted_filenames[0])))
    return sorted_filenames[0]

