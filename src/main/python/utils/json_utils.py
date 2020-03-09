import numpy
import datetime
import json
import pandas as pd


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return float(obj)
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif pd.isnull(numpy.datetime64(obj)):
            return str(obj)
        elif isinstance(obj[0], numpy.float) and (isinstance(obj, list) or isinstance(obj, numpy.array)):
            return [str(obj[0])]
        elif isinstance(obj, pd.Series):
            return obj.to_string()
        return super().default(obj)

    def iterencode(self, value):
        for chunk in super().iterencode(value):
            yield chunk.encode("utf-8")


json_encoder = _JSONEncoder()
