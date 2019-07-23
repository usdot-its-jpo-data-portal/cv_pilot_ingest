'''
Helper class for loading data to Socrata datasets on data.transportation.gov.

'''
import boto3
import copy
import itertools
import json
import os
from sodapy import Socrata


class SocrataDataset(object):
    def __init__(self, dataset_id, socrata_client=None, socrata_params={}):
        self.dataset_id = dataset_id
        self.socrata_client = socrata_client
        self.col_dtype_dict = self.get_col_dtype_dict()
        if not socrata_client and socrata_params:
            self.socrata_client = Socrata(**socrata_params)

    def get_col_dtype_dict(self):
        '''
        Retrieve data dictionary of a Socrata data set in the form of a dictionary,
        with the key being the column name and the value being the column data type

    	Parameters:
    		socrata_client: connected sodapy Socrata client
            dataset_id: ID of Socrata data set

    	Returns:
    		data dictionary of a Socrata data set in the form of a dictionary,
            with the key being the column name and the value being the column data type
        '''
        dataset_col_meta = self.socrata_client.get_metadata(self.dataset_id)['columns']
        col_dtype_dict = {col['name']: col['dataTypeName'] for col in dataset_col_meta}
        return col_dtype_dict

    def mod_dtype(self, rec, col_dtype_dict=None, float_fields=[]):
        '''
        Make sure the data type of each field in the data record matches the data type
        of the field in the Socrata data set.

    	Parameters:
    		rec: dictionary object of the data record
            col_dtype_dict: data dictionary of a Socrata data set in the form of a dictionary,
            with the key being the column name and the value being the column data type
            float_fields: list of fields that should be a float

    	Returns:
    		dictionary object of the data record, with number, string, and boolean fields
            modified to align with the data type of the corresponding Socrata data set
        '''
        col_dtype_dict = col_dtype_dict or self.col_dtype_dict

        identity = lambda x: x
        dtype_func = {'number': float, 'text': str, 'checkbox': bool}
        out = {}
        for k,v in rec.items():
            if k in float_fields and k in col_dtype_dict:
                out[k] = float(v)
            elif k in col_dtype_dict:
                if v is not None and v is not '':
                    out[k] = dtype_func.get(col_dtype_dict.get(k, 'nonexistentKey'), identity)(v)
        return out

    def clean_and_upsert(self, recs):
        out_recs = [self.mod_dtype(r) for r in recs]
        uploadResponse = self.socrata_client.upsert(self.dataset_id, out_recs)
        return uploadResponse
