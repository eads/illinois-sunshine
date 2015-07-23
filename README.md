# Illinois Sunshine

Keep an eye on money in Illinois politics. Built for the Illinois Campaign for Political Reform.

## Setup

**Install OS level dependencies:** 

* Python 3.4
* PostgreSQL 9.4 +

**Install app requirements**

We recommend using [virtualenv](http://virtualenv.readthedocs.org/en/latest/virtualenv.html) and [virtualenvwrapper](http://virtualenvwrapper.readthedocs.org/en/latest/install.html) for working in a virtualized development environment. [Read how to set up virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/).

Once you have virtualenvwrapper set up,

```bash
mkvirtualenv illinois-sunshine
git clone https://github.com/datamade/illinois-sunshine.git
cd illinois-sunshine
pip install -r requirements.txt
cp illinois-sunshine/app_config.py.example illinois-sunshine/app_config.py
```

In `app_config.py`, put your Postgres user in `DB_USER` and password in `DB_PW`.

  NOTE: Mac users might need this [lxml workaround](http://stackoverflow.com/questions/22313407/clang-error-unknown-argument-mno-fused-madd-python-package-installation-fa).

Afterwards, whenever you want to work on illinois-sunshine,

```bash
workon illinois-sunshine
```

## Setup your database

Before we can run the website, we need to create a database.

```bash
createdb illinois_sunshine
```

Then, we run the `etl.py` script to download our data from the IL State Board of Elections and load it into the database.

```bash
python etl.py --download --load_data --recreate_views
```

This command will take between 15-45 min depending on your internet connection.

You can run `etl.py` again to get the latest data from the IL State Board of Elections. It is updated daily. Other useful flags are:

```
 --download               Downloading fresh data
 --cache                  Cache downloaded files to S3
 --load_data              Load data into database
 --recreate_views         Recreate database views
 --chunk_size CHUNK_SIZE  Adjust the size of each insert when loading data
 ```

## Running Illinois Sunshine

``` bash
git clone git@github.com:datamade/illinois-sunshine.git
cd illinois-sunshine

# to run locally
python runserver.py
```

navigate to http://localhost:5000/

## Team

* Eric van Zanten - developer
* Derek Eder - developer

## Errors / Bugs

If something is not behaving intuitively, it is a bug, and should be reported.
Report it here: https://github.com/datamade/illinois-sunshine/issues

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit, do not mess with rakefile, version, or history.
* Send a pull request. Bonus points for topic branches.

## Copyright

Copyright (c) 2015 DataMade and Illinois Campaign for Political Reform. Released under the [MIT License](https://github.com/datamade/illinois-sunshine/blob/master/LICENSE).
