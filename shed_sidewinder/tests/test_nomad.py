from pkg_resources import resource_filename as rs_fn
from shed_sidewinder.nomad import parse


def test_nomad_parser():
    fp = rs_fn('shed_sidewinder', 'data/nomad')
    c = 0
    for n, d in parse(fp):
        assert n in {'start', 'descriptor', 'event', 'stop'}
        assert isinstance(d, dict)
        c = 1
    assert c == 1
