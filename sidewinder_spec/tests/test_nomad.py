from pkg_resources import resource_filename as rs_fn
from sidewinder_spec.nomad import parse


def test_nomad_parser():
    fp = rs_fn('sidewinder_spec', 'data/nomad')
    for n, d in parse(fp):
        assert n in {'start', 'descriptor', 'event', 'stop'}
        assert isinstance(d, dict)
    assert n
