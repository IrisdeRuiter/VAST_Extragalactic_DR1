#!/usr/bin/env python3

'''
This script produces cutout_dict.pkl and lightcurve_dict.pkl that contain
the download links per lightcurve table and cutout.
Once these files exist, there is no need to rerun this.


These scripts are put together based on the tutorials for the CSIRO DAP API:
https://research.csiro.au/dap/developer-tools/web-services-python/

Retrieve a file from a collection in the DAP.

'''

import json
import csv
import requests
import time
import pickle

import os
import requests
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed



def encodeIdentifier(identifier, **kwargs):
    '''
    Take a DAP persistent URL (DOI, Handle, etc.), extract the
    identifier and then encode the slashes appropriately for
    the DAP web service.
    
    The original DAP web service (without a version number) used
    the "~" character instead of slashes.  This was changed in 
    version 2 to use URL encoded slashes "%2F"
    
    kwargs:
        baseURL     (e.g. baseURL="https://ws.data.csiro.au/")
        version     (e.g. version=2)
    
    version takes precedence over baseURL
    
    Examples:
    >>> doi = "https://doi.org/10.4225/08/59475c67be7a4"
    >>> encodeIdentifier(doi, version=1)
    '10.4225~08~59475c67be7a4'
    >>> encodeIdentifier(doi, version=2)
    '10.4225/08/59475c67be7a4'
    >>> encodeIdentifier(doi, baseURL="https://ws.data.csiro.au/")
    '10.4225~08~59475c67be7a4'
    >>> encodeIdentifier(doi, baseURL="https://data.csiro.au/dap/ws/v2/")
    '10.4225/08/59475c67be7a4'
    >>>
    '''
    
    baseURL = kwargs.get("baseURL")
    version = kwargs.get("version")
    
    if baseURL:
        if baseURL == "https://ws.data.csiro.au/":
            encodeChar = "~"
        elif baseURL == "https://data.csiro.au/dap/ws/v2/":
            encodeChar = "/"
        else:
            # default to v2 since v1 is being deprecated.
            encodeChar = "/"
    
    # specifying a version means baseURL effectively gets ignored.
    if version:
        if version == 1:
            encodeChar = "~"
        elif version == 2:
            encodeChar = "/"
        else:
            # default to v2 since v1 is being deprecated.
            encodeChar = "/"
    
    # default to v2 since v1 is being deprecated.
    if not baseURL and not version:
        version = 2
        encodeChar = "/"
    
    # Extract the ID.
    # This assumes that the URLs are from the persistent link on the DAP UI
    # collection landing pages
    # DOI URLs
    identifier = identifier.replace("https://doi.org/", "")
    identifier = identifier.replace("http://doi.org/", "")
    identifier = identifier.replace("https://dx.doi.org/", "")
    identifier = identifier.replace("http://dx.doi.org/", "")
    
    # Handle URLs
    identifier = identifier.replace("http://hdl.handle.net/", "")
    identifier = identifier.replace("https://hdl.handle.net/", "")
    identifier = identifier.replace("?index=1", "")
    
    # DAP URLs
    identifier = identifier.replace("https://data.csiro.au/dap/landingpage?pid=", "")
    identifier = identifier.replace("http://data.csiro.au/dap/landingpage?pid=", "")
    if ("https://data.csiro.au/collections/#/collection/CI" in identifier
        or "https://data.csiro.au/collections/#collection/CI" in identifier
        or "https://data.csiro.au/collections/collection/CI" in identifier
        or "https://data.csiro.au/collection/" in identifier):
        identifier = identifier.replace("https://data.csiro.au/collections/#/collection/CI", "")
        identifier = identifier.replace("https://data.csiro.au/collections/#collection/CI", "")
        identifier = identifier.replace("https://data.csiro.au/collections/collection/CI", "")
        identifier = identifier.replace("https://data.csiro.au/collection/", "")
        params = identifier.find("?")
        if params >= 0:
            identifier = identifier[:params]
    
    # What is left should be just the identifier.
    # Encode any slashes
    if version == 1:
        identifier = identifier.replace("/", encodeChar)
    
    return identifier


def create_lightcurves_dict():
    
    filename_url = {}
    r2 = requests.get(f'{baseURL}collections/{fedora_PID}/files.json?folder=%2Flightcurves&page=1&sb=filename&size=5')
    
    folder_dataPage = r2.json()
    
    files = folder_dataPage.get("file")
    
    filenames_sub = [f["filename"] for f in files]
    
    for filename in filenames_sub:
        
        #folder = filename.split('/')[0]
        file = filename.split('/')[-1]
        
        url = f'https://data.csiro.au/dap/ws/v2/collections/{fedora_PID}/downloadfile?fileName=%2Flightcurves%2F{file}'
        filename_url[file] = url

    return filename_url


def find_cutout_folders():
    r2 = requests.get(f'https://data.csiro.au/dap/ws/v2/collections/{fedora_PID}/folders')
    foldersPage = r2.json()
    
    cutout_directories = []
    for i in range(len(foldersPage.get("folders")[0]['folders'][0]['folders'])):
        cutout_dir_name = foldersPage.get("folders")[0]['folders'][0]['folders'][i]['name']
        cutout_directories.append(cutout_dir_name)

    return cutout_directories


def create_cutout_dict(folder2: str, baseURL: str, fedora_PID: str, session: requests.Session) -> dict:
    """
    Build a {filename: url} dictionary for one 'folder2' under /cutouts/.
    """

    # There are max 5000 files per folder
    size = 5000
    folder_page_url = f'{baseURL}collections/{fedora_PID}/files.json?folder=%2Fcutouts%2F{folder2}&page=1&sb=filename&size={size}'
    r = session.get(folder_page_url, timeout=20)
    
    data = r.json()

    files = data.get("file", []) or []
    out = {}

    for entry in files:
        filename = entry.get("filename")  # as returned by the API (e.g. "cutouts/<folder2>/<name>" or similar)
        
        source_ID = filename.split('/')[-1].split('_')[0]

        download_url = f"https://data.csiro.au/dap/ws/v2/collections/{fedora_PID}/downloadfile?fileName={filename}"

        # Key by the API's filename (keeps original path context); change to 'basename' if you prefer unique names only.
        out[source_ID] = download_url

    return out



def build_cutout_url_index(
    cutout_directories: list[str],
    baseURL: str,
    fedora_PID: str,
    max_workers: int = 8,
) -> dict:
    """
    Run create_cutout_dict over all folders in parallel and merge results.
    Later folders overwrite earlier keys on collision.
    """
    merged: dict = {}
    with requests.Session() as session:
        # Optional: session headers/auth here, e.g. session.headers.update({...})
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {
                ex.submit(create_cutout_dict, folder2, baseURL, fedora_PID, session): folder2
                for folder2 in cutout_directories
            }
            for fut in as_completed(futures):
                folder2 = futures[fut]
                try:
                    merged.update(fut.result())
                except Exception as e:
                    print(f"[WARN] Failed to index folder '{folder2}': {e}")

    return merged




### MAIN



#VAST Extragalactic DR1: light curve database and cutouts
collectionDOI = 'https://doi.org/10.25919/nh9d-t846' 

baseURL = "https://data.csiro.au/dap/ws/v2/"
endpoint = "collections/{id}"
headers = {"Accept": "application/json"}

# Encode DOI
encodedID = encodeIdentifier(collectionDOI)

url = baseURL + endpoint.format(id=encodedID)

# Fetch collection metadata
r = requests.get(url, headers=headers)
dataPage = r.json()
fedora_PID = dataPage["id"]['identifier']




filename_url = create_lightcurves_dict()
with open("lightcurves_dict.pkl", "wb") as f:
    pickle.dump(filename_url, f)



cutout_folders = find_cutout_folders()

url_index = build_cutout_url_index(
    cutout_directories=cutout_folders,
    baseURL=baseURL,
    fedora_PID=fedora_PID,
    max_workers=20,
)

print(f"Total files indexed: {len(url_index)}")


with open("cutouts_dict.pkl", "wb") as f:
    pickle.dump(url_index, f)