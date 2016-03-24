import pyFAI
import tifffile
from filestore import HandlerBase
from filestore.api import register_handler
import fabio


class TiffHandler(HandlerBase):
    specs = {'TIFF'} | HandlerBase.specs

    def __init__(self, name):
        self._name = name

    def __call__(self):
        return tifffile.imread(self._name)


class PyFAIGeoHandler(HandlerBase):
    specs = {'pyFAI-geo'} | HandlerBase.specs

    def __init__(self, name):
        self._name = name

    def __call__(self):
        return pyFAI.load(self._name)


class Fit2DMaskHandler(HandlerBase):
    specs = {'MSK'} | HandlerBase.specs

    def __init__(self, name):
        self._name = name

    def __call__(self):
        return fabio.open(self._name).data


handlers = [TiffHandler, PyFAIGeoHandler, Fit2DMaskHandler]
for handler in handlers:
    for spec in handler.specs:
        register_handler(spec, handler)
