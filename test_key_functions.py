import io
import csv
from key_functions import alphanum_squash_spaces, alphanum_simple, alphanum_regex, alphanum_simple_squash
from pprintpp import pprint
from natsort import natsorted, ns

CSV_DATA = """plan_name,product_line,product,subscriber_id,member_id,fname,minitial,lname,gender,dob,address1,address2,city,state,zip,phone
Test Plan,2,1,777,1,bob,,brody,1,,200 Backer St,,city65,IL,62207,
Test Plan,2,1,777,2,bob,,walker,1,,200 N 7th St,,city65,IL,62201,
Test Plan,2,1,8888,32,bob,,smith,1,,3400 State St,,city3,IL,62002,
Test Plan,2,1,141414,4,jane,,smith,1,,3400 State St,,city3,IL,62002,
Test Plan,2,1,8888,5,simon,,smith,1,,3400 State St,,city3,IL,62002,
Test Plan,2,1,999,6,andy,,james,1,,100 Main St,,city2,IL,62239,
Test Plan,2,1,999,7,bob,,james,1,,100 Main St,,city2,IL,62239,
Test Plan,2,1,121212,8,steve,,black,1,,77 Hickory Ave,,city7,IL,62062,
Test Plan,2,1,121212,10,fred,,black,1,,77 Hickory Ave,,city7,IL,62062,
Test Plan,2,1,141414,21,john,,smooth,1,,400 White Ave,,city900,IL,62024,
Test Plan,2,1,796801,26,bob,,bumpy,1,,300 Gray Drive,,city101,IL,62207,
Test Plan,2,1,3434,83,steve,,flat,1,,201 Central St.,,city88,IL,62087,
Test Plan,2,1,3434,101,phil,,flat,1,,201 Central St.,,city88,IL,62087,
Test Plan,2,1,1000,001,mary,,jones,2,,P O BOX 733,,city701,IL,62087,
Test Plan,2,1,1000,002,ned,,jones,2,,PO BOX 733,,city701,IL,62087,
Test Plan,2,1,1000,004,xander,,jones,2,,P.O. BOX 733,,city701,IL,62087,
Test Plan,2,1,1000,003,billy,,jones,2,,POBOX 733,,city701,IL,62087,
Test Plan,2,1,2000,001,wally,,smythe,2,,North 11th Street,,city67,IL,62087,
Test Plan,2,1,3000,001,francis,,gold,2,,N. Second Street,,city68,IL,62087,
Test Plan,2,1,4000,001,robert,,buffet,2,,100 1st. 32nd St.,,city10,IL,62087,
"""

def print_row(row):
    sort_columns = ['address1', 'address2', 'city', 'state', 'zip', 'dob', 'fname']
    print(" ".join(["{0:18}".format(row[x]) for x in sort_columns]))

def sort_key(row, func, single_string=False):
    #sort_key = [alphanum_squash_spaces(row[x]) for x in sort_columns]
    sort_key = [func(row[x]) for x in sort_columns]

    # Stringify the results to allow Python to compare **everything**
    sort_key = [str(x) for x in sort_key]
    if single_string:
        sort_key = "".join(sort_key)

    print(sort_key)
    return sort_key


if __name__ == "__main__":
    sort_columns = ['address1', 'address2', 'city', 'state', 'zip', 'dob', 'fname']

    list_to_sort = []

    data_textio = io.StringIO(CSV_DATA)
    data_csv = csv.DictReader(data_textio)
    for row in data_csv:
        #pprint(row)
        list_to_sort.append(row)

    for row in list_to_sort:
        print_row(row)

    #func = lambda row: [alphanum_squash_spaces(row[x]) for x in sort_columns]

    print("========================")
    print('squash_spaces')
    func = alphanum_squash_spaces
    sorted_list = sorted(list_to_sort, key=lambda row: sort_key(row, func))
    print("------------------------")
    for row in sorted_list:
        print_row(row)

    print("========================")
    print('simple')
    func = alphanum_simple
    sorted_list = sorted(list_to_sort, key=lambda row: sort_key(row, func))
    print("------------------------")
    for row in sorted_list:
        print_row(row)

    print("========================")
    print('simple_squash')
    func = alphanum_simple_squash
    sorted_list = sorted(list_to_sort, key=lambda row: sort_key(row, func))
    print("------------------------")
    for row in sorted_list:
        print_row(row)

    print("========================")
    print('regex')
    func = alphanum_regex
    sorted_list = sorted(list_to_sort, key=lambda row: sort_key(row, func))
    print("------------------------")
    for row in sorted_list:
        print_row(row)

    print("========================")
    print('str_simple')
    func = alphanum_simple
    sorted_list = sorted(list_to_sort, key=lambda row: sort_key(row, func, True))
    print("------------------------")
    for row in sorted_list:
        print_row(row)

    print("========================")
    print('using natsort')
    # Need the *replace(' ', '')* to catch stuff like "PO BOX" vs "P O BOX"
    # Need the *replace('.', '')* to catch stuff like "P.O. BOX" vs "PO BOX"
    # PROBLEM: stripping spaces between numbers creates a new number, and this wrong! Ex. 100 1st Main St -> 1001stmainst.
    sorted_list = natsorted(list_to_sort, alg=ns.IGNORECASE, key=lambda row: [row[x].replace('.', '') for x in sort_columns])
    #sorted_list = natsorted(list_to_sort, key=lambda row: [row[x] for x in sort_columns])
    print("------------------------")
    for row in sorted_list:
        print_row(row)


