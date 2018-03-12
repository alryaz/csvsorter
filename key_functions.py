import datetime
import re

def _tryint(s):
    try:
        return int(s)
    except:
        return s

def alphanum_squash_spaces(s):
    """ Turn a string into a list of string and number chunks.
        "z 23 a" -> ["z", 23, "a"]
        "100 Main Street" -> [100, "Main", "Street"]
        "P O Box 743" -> ["POBox", 743]
    """
    try:
        s = s.replace(' ', '').lower()
    except:
        pass
    return [ _tryint(c) for c in re.split('([0-9]+)', s) ]

def alphanum_simple(s):
    """ Turn a string into a list of string and number chunks.
        "z 23 a" -> ["z", 23, "a"]
        "100 Main Street" -> [100, "Main", "Street"]
        "z23a" -> "z23a"
        "P O Box 743" -> ["P", "O", "Box", 743]
    """
    return [ _tryint(c) for c in s.split() ]

def alphanum_simple_squash(s):
    """ Turn a string into a list of string and number chunks.
        "z 23 a" -> ["z", 23, "a"]
        "100 Main Street" -> [100, "Main", "Street"]
        "z23a" -> "z23a"
        "P O Box 743" -> ["P", "O", "Box", 743]
    """
    try:
        s = s.replace(' ', '').lower()
    except:
        pass
    return [ _tryint(c) for c in s.split() ]

def alphanum_regex(s):
    """ Turn a string into a list of string and number chunks.
        "z23a" -> ["z", 23, "a"]
    """
    return [ _tryint(c) for c in re.split('([0-9]+)', s) ]
