# Developer Notes 



## Release Steps
Typically releases are done for each new version of OpenFAST

1. Create a pull request from main to dev
2. Make sure the input files in the `data` directory are compatible with the new OpenFAST version
3. Change the file VERSION  (and/or setup.py) and push to the pull request
4. Merge pull request to main
5. Tag the commit using `git tag -a vX.X.X` and push to remote: `git push --tags``.
6. Upload to pypi and conda (see below)
7. Merge main to dev


## Upload a new version for pip
Detailled steps are provided further below.

### Summary 
Remember to change VERSION file and/or setup.py 
```bash
python setup.py sdist
twine upload dist/*     # upload to pypi
```



### (Step 0 : create an account on pypi)

### Step 1: go to your repo
Go to folder
```bash
cd path/to/openfast_toolbox
```

### Step 2: change version in setup.py and tag it
change VERSION in setup.py 
```
git add setup.py VERSION
git commit "Version X.X.X"
git tag -a 
```

### Step 3: Create a source distribution
```bash
python setup.py sdist
```

### Step 4: Install twine
```bash
pip install twine
```

### Step 5: Ubplot to pypi
Run twine to upload to Pypi (will ask for username and password)
```bash
twine upload dist/*
```


## Upload a new version to Conda 
TODO TODO TODO

conda-forge, 
make a pull request there.
 - setup build script (https://conda-forge.org/docs/maintainer/adding_pkgs.html). 
    see e.g. FLORIS (https://github.com/conda-forge/floris-feedstock/blob/master/recipe/meta.yaml).
 - make a pull request to https://github.com/conda-forge/staged-recipes/ 
