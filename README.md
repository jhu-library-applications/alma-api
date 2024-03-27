# alma-api

These are scripts used by the JHU Libraries that use the Alma API. 

To use them, you'll need to have an API key. The API key can be found 
in the Ex Libris Developer's Portal.

# Installing dependencies

1. Create a new Python virtual env

```
python3 -m venv
```

2. Activate the virtual env 
```
source env/bin/activate
```

3. Update pip in the virtualenv 

```
python3 -m pip install --upgrade pip
```

4. Install packages from `requirements.txt`

```
pip install -r requirements.txt
```

# Adding dependencies

1. After activating the virtualenv install additional dependencies with `pip`

2. Update the requirements.txt and commit the changes 

```
python3 -m pip freeze > requirements.txt
```