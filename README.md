# zachdj-p0

This repo contains various gyrations of word count implemented with Spark's Python API.
Completed for CSCI8360: Data Science Practicum at the University of Georgia.

The project is divided into four subprojects.  Given a set of documents, we can:
    1. Perform a case-insensitive count of all words and report the forty most frequent words as a JSON dictionary (sp1.py)
    2. Report the forty most-frequent words excluding stopwords (sp2.py)
    3. Report the forty most-frequent words while stripping leading/trailing punctuation (sp3.py)
    4. Report the top 5 words in each document based on their TF-IDF score (sp4.py)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

This project is managed using the [Conda](https://conda.io/docs/) package manager.
Conda is the easiest and fastest way to get setup.

### Installing Dependencies

The environment.yml file is used by Conda to create a virtual environment that includes all the project's dependencies (including Python!)

Navigate to the project directory and run the following command

```
conda env create
```

This will create a virtual environment named "zachdj-p0".  Activate the virtual environment with the following command

```
conda activate zachdj-p0
```

After the environment has been activated, the subprojects can be run as follows

```
python sp1.py
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Python 3.6](https://www.python.org/)
* [PySpark](https://spark.apache.org/docs/0.9.0/python-programming-guide.html) - Python API for [Apache Spark](https://spark.apache.org/)
* [Conda](https://conda.io/docs/) - Package Manager

## Contributing

There are no specific guidelines for contributing.  If you see something that could be improved, send a pull request!

## Versioning

This project uses the [GitFlow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) workflow
to organize branches and "releases".

## Authors

* [**Zach Jones**](https://github.com/zachdj)

See the [contributors](CONTRIBUTORS.md) file for details.

## License

This project is licensed under the GNU GPL v3 - see the [LICENSE.md](LICENSE.md) file for details

