from sidewinder_spec.utils.parsers import parse_new_spec_file, \
    parse_new_spec_run
from pprint import pprint


def test_parse_new_spec_file():
    a = parse_new_spec_file('spec.log')
    print(a)


def test_parse_new_spec_run():
    a = parse_new_spec_file('spec.log')
    b = parse_new_spec_run(a)
    pprint(b)


if __name__ == '__main__':
    test_parse_new_spec_file()
    test_parse_new_spec_run()
