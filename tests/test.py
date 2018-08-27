
import os
import uuid
import unittest

from .context import csvsorter
from ..csvsorter import memorysort

class TestCSVSorter(unittest.TestCase):

    def setUp(self):
        # Create CSV file 5MB in size, reverse sorted
        self.tmp_name = str(uuid.uuid4().hex) + '.csv'
        self.num_lines = 200000
        with open(self.tmp_name, 'w') as fout:
            for line in range(self.num_lines, 0, -1):
                print('{},{},{},{}'.format(line+4, line+3, line+2, line+1), file=fout)

    def tearDown(self):
        os.remove(self.tmp_name)


    def check_file_sorted(self, sort_cols=None, type_funcs=str, skip_header=False):
        with open(self.tmp_name, 'r') as fin:
            if skip_header:
                fin.readline()

            prev_line = fin.readline().split(',')
            if not sort_cols:
                sort_cols = list(range(len(prev_line)))
            N = len(sort_cols)

            if not isinstance(type_funcs, list):
                type_funcs = [type_funcs] * N

            #prev_line = [type_funcs[x](prev_line[sort_cols[x]]) for x in range(N)]
            prev_line = [int(prev_line[sort_cols[x]]) for x in range(N)]
            for line in fin:
                line = line.split(',')
                #line = [type_funcs[x](line[sort_cols[x]]) for x in range(N)]
                line = [int(line[sort_cols[x]]) for x in range(N)]
                self.assertTrue(prev_line <= line)
                prev_line = line


    def test_memorysort_allcols(self):
        direction = 'ascending'
        key_functions = [str] * 4
        memorysort(self.tmp_name, [0,1,2,3], [str] * 4, key_functions, 'utf-8', direction)
        self.check_file_sorted()

    def test_memorysort_onecol(self):
        memorysort(self.tmp_name, [3], [str], [str], 'utf-8', 'ascending')
        self.check_file_sorted(sort_cols=[3])

    def test_csvsort(self):
        # sort and force merges
        csvsorter.csvsort(self.tmp_name, [0,1,2,3], max_size=10, has_header=False)
        self.check_file_sorted()

        # make sure all the lines are here after merging
        linecount = 0
        with open(self.tmp_name, 'r') as fin:
            for line in fin:
                linecount += 1
        self.assertEqual(self.num_lines, linecount)


    def test_csvsort_int(self):
        # sort and force merges
        csvsorter.csvsort(self.tmp_name, [0,1,2,3], column_types=int, max_size=10, has_header=False)
        self.check_file_sorted(type_funcs=int)

        # make sure all the lines are here after merging
        linecount = 0
        with open(self.tmp_name, 'r') as fin:
            for line in fin:
                linecount += 1
        self.assertEqual(self.num_lines, linecount)

    def test_csvsort_mixtypes(self):
        # sort and force merges
        csvsorter.csvsort(self.tmp_name, [0,2], column_types=[str,int], max_size=10, has_header=False)
        self.check_file_sorted(sort_cols=[0,2], type_funcs=[str,int])

        # make sure all the lines are here after merging
        linecount = 0
        with open(self.tmp_name, 'r') as fin:
            for line in fin:
                linecount += 1
        self.assertEqual(self.num_lines, linecount)

    def test_csvsort_twocols(self):
        # sort and force merges
        csvsorter.csvsort(self.tmp_name, [1,3], max_size=10, has_header=False)
        self.check_file_sorted(sort_cols=[1,3])

        # make sure all the lines are here after merging
        linecount = 0
        with open(self.tmp_name, 'r') as fin:
            for line in fin:
                linecount += 1
        self.assertEqual(self.num_lines, linecount)

    def test_header(self):
        # sort and force merges
        csvsorter.csvsort(self.tmp_name, [3], max_size=10, has_header=True)
        self.check_file_sorted(sort_cols=[3], skip_header=True)

        # make sure all the lines are present (header not missing)
        with open(self.tmp_name, 'r') as fin:
            header = fin.readline()
            self.assertEqual(header, '{},{},{},{}\n'.format(
                    self.num_lines+4, self.num_lines+3, self.num_lines+2,
                    self.num_lines+1))
            linecount = 1
            for line in fin:
                linecount += 1
        self.assertEqual(self.num_lines, linecount)


