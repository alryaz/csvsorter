
import os
import unittest

from .context import csvsorter

import uuid

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


  def check_file_sorted(self):
    with open(self.tmp_name, 'r') as fin:
      prev_line = fin.readline()
      for line in fin:
        self.assertTrue(prev_line < line)
        prev_line = line

  def check_col_sorted(self, col):
    with open(self.tmp_name, 'r') as fin:
      prev_line = fin.readline().split(',')
      for line in fin:
        line = line.split(',')
        self.assertTrue(prev_line[col] <= line[col])
        prev_line = line


  def test_memorysort_allcols(self):
    csvsorter.memorysort(self.tmp_name, [0,1,2,3], 'utf-8')
    self.check_file_sorted()

  def test_memorysort_onecol(self):
    csvsorter.memorysort(self.tmp_name, [3], 'utf-8')
    self.check_col_sorted(3)

  def test_csvsort(self):
    # sort and force merges
    csvsorter.csvsort(self.tmp_name, [0,1,2,3], max_size=1, has_header=False)
    self.check_file_sorted()

    # make sure all the lines are here after merging
    linecount = 0
    with open(self.tmp_name, 'r') as fin:
      for line in fin:
        linecount += 1
    self.assertEqual(self.num_lines, linecount)
      
  def test_csvsort_onecol(self):
    # sort and force merges
    csvsorter.csvsort(self.tmp_name, [3], max_size=1, has_header=False)
    self.check_col_sorted(3)

    # make sure all the lines are here after merging
    linecount = 0
    with open(self.tmp_name, 'r') as fin:
      for line in fin:
        linecount += 1
    self.assertEqual(self.num_lines, linecount)
      

