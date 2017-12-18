#!/usr/bin/env python
# -*- coding: utf-8 -*-

from optparse import OptionParser

import csvsorter

def main():
    parser = OptionParser()
    parser.add_option('-c', '--column', dest='columns', action='append', help='column of CSV to sort on')
    parser.add_option('-s', '--size', '-s', dest='max_size', type='float', default=100, help='maximum size of each split CSV file in MB (default 100)')
    parser.add_option('-n', '--no-header', dest='has_header', action='store_false', default=True, help='set CSV file has no header')
    parser.add_option('-d', '--delimiter', default=',', help='set CSV delimiter (default ",")')
    parser.add_option('-e', '--encoding', default='utf-8', help='encoding to use for input/output files')
    args, input_files = parser.parse_args()

    if not input_files:
        parser.error('What CSV file should be sorted?')
    elif not args.columns:
        parser.error('Which columns should be sorted on?')
    else:
        # escape backslashes
        args.columns = [int(column) if column.isdigit() else column for column in args.columns]
        csvsorter.csvsort(input_files[0], columns=args.columns, max_size=args.max_size, has_header=args.has_header, delimiter=args.delimiter, encoding=args.encoding)


if __name__ == '__main__':
    main()
