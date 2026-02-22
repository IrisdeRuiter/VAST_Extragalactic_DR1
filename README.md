# VAST_Extragalactic_DR1

If you make use of this of the examples in this repo, please cite:

```bibtex
@article{2026,
       author = {{de Ruiter}, Iris and {Dobie}, Dougal and {Murphy}, Tara and others},
        title = "{The ASKAP Variables and Slow Transients (VAST) Extragalactic Survey – Data Release 1}",
 howpublished = {PASA},
         year = 2026,
        month = {},
          eid = {},
       adsurl = {},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
```
---


### Notes/updates


---

### What is this?

This repository contains a Jupyter notebook that shows how use the high-level data products from [VAST Extragalactic DR1: light curve database and cutouts](https://doi.org/10.25919/nh9d-t846), hosted on CSIRO's Data Access Portal (DAP).
The python scripts in `DAP_tools/` use the [DAP API](https://research.csiro.au/dap/developer-tools/) to pull the light curve data and cutouts from the DAP. The Jupyter notebook reads these files and visualises light curves and cutouts. Note that running this notebook will result in downloading the lightcurve products (~500 MB). 

User should only work in `VAST_Extragalactic_DR1.ipynb`, which should be accessible with a modest amount of Python experience. Please raise an issue if this notebook does not work for you, or if anything in the notebook is unclear.


### Getting started
Navigate to the folder where you want to initialize your VAST working directory and run 

   ```bash
   git clone https://github.com/IrisdeRuiter/VAST_Extragalactic_DR1.git
   ```
Alternatively, click on the green code button in this repo, and download its contents as zip file.

### Setting up the python environment to run the Jupyter notebook
Before running the notebook you should use the vast_environment.yaml file to setup the correct python environment using conda.

Running: 

```bash
conda env create -f vast_environment.yml --solver=libmamba
```
will create a new conda environment named: `vast_env`

The libmamba solver will significantly speed up the install. This solver is the default solver for Conda versions 23.10+.
If you work with an older conda install, consider upgrading, and otherwise try installing without the solver specification.


Activate the environment using
```bash
conda activate vast_env
```

This environment contains basic python and astronomy packages, as well as [vaex](https://vaex.readthedocs.io/en/latest/), which we need to read in the large light curve database tables efficiently.
