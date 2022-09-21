To Run

New Setup:

```
conda env create -f environment.yml
Get setup_local_credentials.sh from Chris
```

```
conda activate yahootosheets_backend
source setup_local_credentials.sh
flask run
```

To Run Tests

```
python -m unittest flaskr/test_yahoo_response_parser.py
```