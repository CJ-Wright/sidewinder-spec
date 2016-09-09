from sidewinder_spec.utils.parsers import parse_new_spec_file, \
    parse_new_spec_run
from pprint import pprint
import os

spec_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         'spec.log')

def test_parse_new_spec_file():
    a = parse_new_spec_file(spec_file)
    print(a)


def test_parse_new_spec_run():
    a = parse_new_spec_file(spec_file)
    b = parse_new_spec_run(a)
    pprint(b)


if __name__ == '__main__':
    test_parse_new_spec_file()
    test_parse_new_spec_run()
