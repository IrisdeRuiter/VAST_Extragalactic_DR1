import requests

def get_file(url, directory, name):
    
    with requests.get(url, allow_redirects=True, stream=True) as r:
        #r.raise_for_status()
        with open(directory + name, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f'Saved {name} to {directory}')
