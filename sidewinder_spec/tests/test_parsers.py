from sidewinder_spec.utils.parsers import parse_new_spec_file, \
    parse_new_spec_run
from pprint import pprint
import os

spec_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'spec.log')


def test_parse_new_spec_file():
    a = parse_new_spec_file(spec_file)
    assert len(a) == 2


def test_parse_new_spec_run():
    a = parse_new_spec_file(spec_file)
    assert len(a) == 2
    b = parse_new_spec_run(a)
    for c in b:
        assert len(c) == 3

if __name__ == '__main__':
    test_parse_new_spec_file()
    test_parse_new_spec_run()
