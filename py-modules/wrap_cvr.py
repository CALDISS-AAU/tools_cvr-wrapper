import requests
import pandas as pd
import random
import time

CVR_VIRK_URL = 'http://distribution.virk.dk/cvr-permanent/virksomhed/_search'
CVR_PROD_URL = 'http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search'
CVR_PROD_SCROLL = 'http://distribution.virk.dk/cvr-permanent/produktionsenhed/_search?scroll=1m'
CVR_SCROLL = 'http://distribution.virk.dk/_search/scroll'

HEADERS = {
    'Content-Type': 'application/json',
}

FIELDS = ["VrproduktionsEnhed.pNummer", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteNavn", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteAarsbeskaeftigelse.intervalKodeAntalAnsatte",
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteAarsbeskaeftigelse.sidstOpdateret", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteBeliggenhedsadresse", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteCvrNummerRelation", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchekode", 
          "VrproduktionsEnhed.produktionsEnhedMetadata.nyesteHovedbranche.branchetekst"]


def req_prod_branche_single(branchecode, auth, fields = FIELDS, from_ = 0, size = 2500, api_endpoint = CVR_PROD_URL, headers = HEADERS):
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
                  "from": from_,
                  "size": size
                 }
    
    
    query = str(query_body).replace('\'', '\"')
    
    response = requests.post(api_endpoint, headers = headers, auth = auth, data = query)
    
    data_response = dict(response.json())

    data = pd.json_normalize(data_response['hits']['hits'])
    
    return(data)


def req_prod_branche(branchecode, auth, fields = FIELDS, api_endpoint = CVR_PROD_SCROLL, headers = HEADERS):
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
            
            time.sleep(random.uniform(0.1, 0.3))
            
        data = data.drop_duplicates().reset_index(drop = True)

    else:
        
        data = pd.json_normalize(response_dict['hits']['hits'])
        
    return(data)