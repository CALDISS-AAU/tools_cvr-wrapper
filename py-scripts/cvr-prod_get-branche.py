#!/usr/bin/env python
# coding: utf-8

# Packages
import requests
import pandas as pd
import random
import time
import numpy as np
import os
from os.path import join
import sys

sys.path.append(join('..', 'py-modules'))

from prod_from_branche import * # import custom functions for querying CVR data

# Set path for exported data file
data_dir = join('')
outname = 'cvr-prod_branches_ansatte2018.csv'
out_path = join(data_dir, outname)


# Authentication - UPDATE (username and password received from virk.dk)
AUTH_USER = ''
AUTH_PASS = ''


# Set branche numbers (as list)
branche_codes = ['01',
                 '02',
                 '03',
                 '06',
                 '08',
                 '09',
                 '10',
                 '11',
                 '12',
                 '13',
                 '14',
                 '15',
                 '16',
                 '17',
                 '18',
                 '19',
                 '20',
                 '21',
                 '22',
                 '23',
                 '24',
                 '25',
                 '26',
                 '27',
                 '28',
                 '29',
                 '30',
                 '31',
                 '32',
                 '33',
                 '45',
                 '46',
                 '47',
                 '55',
                 '56']


# Collect data from branche numbers
combined_data = pd.DataFrame() # empty data frame to add data to

year_collect = 2018 # set year to collect data for

for c, code in enumerate(branche_codes, start = 1): # iterate over branche numbers

    data = req_prod_branche(branchecode = code, year = year_collect, auth = (AUTH_USER, AUTH_PASS), wildcard = True) # query branchecode using branchecode, year and authentication
        ## the wildcard option means it uses the branche codes as prefixes searching for all branchecode starting with these digits (corresponds to querying "hovedbranchekoder")

    ## add queried data to compbined data file    
    combined_data = combined_data.append(data, ignore_index = True)
    
    ## wait before next query
    time.sleep(random.uniform(0.5, 1.0))
    
    ## progress bar
    progress = "|{0}| {1:.2f} %".format(("="*int(c/len(branche_codes) * 50)).ljust(50), c/len(branche_codes) * 100)
    print(progress, end = "\r")


# export data to csv
combined_data.to_csv(out_path, sep = ';', index = False)