from copy import deepcopy
import json
import os
import uuid
BASE = {
    "Source": {
      "Identifier": {
        "ID": "<ZenodoDOI>",
        "IDScheme": "doi"
      },
      "Type": {
        "Name": "unknown"
      }
    },
    "RelationshipType": {
      "Name": "IsRelatedTo",
      "SubType": "IsIdenticalTo",
      "SubTypeSchema": "DataCite"
    },
    "Target": {
      "Identifier": {
        "ID": "<ID>",
        "IDScheme": "<Scheme>"
      },
      "Type": {
        "Name": "unknown"
      }
    },
    "LinkPublicationDate": "2018-01-01",
    "LinkProvider": [
      {
        "Name": "Zenodo"
      }
    ]
}
VERSION = deepcopy(BASE)
VERSION['RelationshipType'] = {
    'Name': 'IsRelatedTo',
    'SubType': 'HasVersion',
    'SubTypeSchema': 'DataCite',
}
IDENTITY = deepcopy(BASE)
IDENTITY['RelationshipType'] = {
    "Name": "IsRelatedTo",
    "SubType": "IsIdenticalTo",
    "SubTypeSchema": "DataCite"
}

CITES = deepcopy(BASE)
CITES['RelationshipType'] = {'Name': 'References'}


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def write_payloads(prefix, iterable, size=None, chunk_size=100):
    """
    prefix = 'events/zenodo/version'
    """
    # 5 leading zeros will fit <10 million objects (in chunks of 100)
    try:
        size = len(iterable)
    except Exception:
        pass
    leading_zeros = str(len(str(size // chunk_size))) if size else '5'
    filename_fmt = '{0:0' + leading_zeros + 'd}.json'
    for idx, chunk in enumerate(chunks(iterable, chunk_size)):
        with open(os.path.join(prefix, filename_fmt.format(idx)), 'w') as fp:
            json.dump(list(chunk), fp, indent=2)
