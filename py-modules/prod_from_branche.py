#!/usr/bin/env python
# coding: utf-8

# Packages
import requests
import pandas as pd
import random
import time
import numpy as np
import os

# Endpoints
CVR_VIRK_URL = 'http://distribution.virk.dk/cvr-permanent/virksomhed/_search'
CVR_PROD_URL = 'http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search'
CVR_PROD_SCROLL = 'http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search?scroll=1m'
CVR_SCROLL = 'http://distribution.virk.dk/_search/scroll'

HEADERS = {
    'Content-Type': 'application/json',
}

# Parameters
FIELDS = ["VrproduktionsEnhed.pNummer", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteNavn", 
          "VrproduktionsEnhed.aarsbeskaeftigelse.intervalKodeAntalAnsatte",
          "VrproduktionsEnhed.aarsbeskaeftigelse.aar",
          "VrproduktionsEnhed.aarsbeskaeftigelse.sidstOpdateret",
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteBeliggenhedsadresse", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteCvrNummerRelation", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchekode", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchetekst"]

# Functions
def req_prod_branche_single(branchecode, year, fields = FIELDS, from_ = 0, size = 2500, api_endpoint = CVR_PROD_URL, auth = (AUTH_USER, AUTH_PASS), headers = HEADERS):
    query_body = {"_source": fields,
                  "query": {
                      "bool": {
                          "must": [
                              {"match": {"VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchekode": branchecode}},
                              {"match": {"VrproduktionsEnhed.aarsbeskaeftigelse.aar": year}}
                          ],
                          "must_not": [
                              {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteBeliggenhedsadresse.gyldigTil"}},
                              {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteNavn.periode.gyldigTil"}}
                          ]
                      }
                  },
                  "from": from_,
                  "size": size
                 }
    
    
    query = str(query_body).replace('\'', '\"')
    
    response = requests.post(api_endpoint, headers = headers, auth = auth, data = query)
    
    data_response = dict(response.json())

    data = pd.json_normalize(data_response['hits']['hits'])
    
    return(data)


def req_prod_branche(branchecode, year, auth, wildcard = True, fields = FIELDS, api_endpoint = CVR_PROD_SCROLL, headers = HEADERS):
    """Function for getting production number information based on DB07 branchecodes using virk.dk 'system-til-system' adgang. Only retrieves information from active companies/production numbers.

    Args:
        branchecode (str): Branchecode to search for. Note that this should be a string.
        year (int): Year for data to get.
        auth (tuple): Username and password for 'system-til-system' adgang.
        wildcard (bool, optional): Whether to treat branchecode as a prefix for wildcard search. When True, includes all branchecode beginning with the specified digits. Defaults to True.
        fields (list, optional): What fields from the database to include. Fields should be specified as they are named in the database. Defaults to predefined FIELDS list in module.
    """
    
    if wildcard:
        query_body = {"_source": fields,
                  "query": {
                      "bool": {
                          "must": [
                              {"wildcard": {"VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchekode": f"{branchecode}*"}}
                          ],
                          "must_not": [
                              {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteBeliggenhedsadresse.gyldigTil"}},
                              {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteNavn.periode.gyldigTil"}}
                          ]
                      }
                  },
                  "size": 3000
                 }
        
    else:
        query_body = {"_source": fields,
                      "query": {
                          "bool": {
                              "must": [
                                  {"match": {"VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchekode": branchecode}}
                              ],
                              "must_not": [
                                  {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteBeliggenhedsadresse.gyldigTil"}},
                                  {"exists": {"field": "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteNavn.periode.gyldigTil"}}
                              ]
                          }
                      },
                      "size": 3000
                     }
    
    query = str(query_body).replace('\'', '\"')
    
    response = requests.post(api_endpoint, headers = headers, auth = auth, data = query)
    response_dict = dict(response.json())
    
    total_hits = response_dict['hits']['total']
    
    if total_hits >= 3000:
        data = pd.json_normalize(response_dict['hits']['hits'])
        
        remains = total_hits
        scroll_id = response_dict['_scroll_id']
        scroll_url = f"{CVR_SCROLL}?scroll=1m&scroll_id={str(scroll_id)}"
        
        while remains > 0:
            
            response = requests.post(scroll_url, headers = HEADERS, auth = auth)
            
            response_dict = dict(response.json())
            req_data = pd.json_normalize(response_dict['hits']['hits'])
            
            data = data.append(req_data, ignore_index = True)
            
            remains = remains - 3000
            
            time.sleep(random.uniform(0.5, 1.0))
            
    else:
        
        data = pd.json_normalize(response_dict['hits']['hits'])
    
    data = data.fillna(np.nan)
    data = data.dropna(subset = ['_source.VrproduktionsEnhed.aarsbeskaeftigelse'])
    data_long = data.explode('_source.VrproduktionsEnhed.aarsbeskaeftigelse').reset_index(drop = True)
    
    besk_data = pd.json_normalize(data_long['_source.VrproduktionsEnhed.aarsbeskaeftigelse']).add_prefix('_source.VrproduktionsEnhed.aarsbeskaeftigelse.')
    data_merged = data_long.merge(besk_data, left_index = True, right_index = True).drop(columns = '_source.VrproduktionsEnhed.aarsbeskaeftigelse')
    data_merged = data_merged.fillna(np.nan)
    
    data_merged = data_merged.loc[data_merged['_source.VrproduktionsEnhed.aarsbeskaeftigelse.aar'] == year, :]
    
    data_merged = data_merged.drop_duplicates().reset_index(drop = True)
        
    return(data_merged)
