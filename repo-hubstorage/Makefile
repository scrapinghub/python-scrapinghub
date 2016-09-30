.PHONY: release release-minor release-path release-major upload test test-all lint clean

VERSION?=minor

release:
	bumpversion $(VERSION)

release-minor: release

release-patch:
	make release VERSION=patch

release-major:
	make release VERSION=major

upload: clean
	python setup.py sdist bdist_wheel upload

test:
	py.test tests/

test-all:
	tox

lint:
	flake8 hubstorage

clean:
	rm -rf build/ dist/ *.egg-info htmlcov/
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
