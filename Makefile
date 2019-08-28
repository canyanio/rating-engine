.PHONY:
venv:
	virtualenv -p python3 venv --no-site-packages

.PHONY: setup
setup:
	pip install -r requirements.txt
	pip install --editable .

.PHONY: dist
dist:
	python3 setup.py sdist bdist_wheel

.PHONY: clean
clean:
	rm -fr build dist *.egg-info

.PHONY: black
black:
	black --skip-string-normalization rating_engine *.py

.PHONY: black-check
black-check:
	black --check --skip-string-normalization rating_engine *.py

.PHONY: flake8
flake8:
	flake8 --ignore=E501,E402,W503 rating_engine *.py

.PHONY: mypy
mypy:
	mypy rating_engine

.PHONY: pylint
pylint:
	pylint rating_engine *.py

.PHONY: pycodestyle
pycodestyle:
	pycodestyle --ignore=E501,W503,E402,E701 rating_engine *.py

.PHONY: check
check: black-check flake8 mypy pylint pycodestyle

.PHONY: test
test:
	py.test -p no:warnings

.PHONY: coverage
coverage:
	coverage run -m py.test -p no:warnings
	coverage report
	coverage html
	coverage xml

.PHONY: engine
engine:
	rating-engine

.PHONY: engine-dev
engine-dev:
	rating-engine --debug

.PHONY: dockerfile
dockerfile:
	docker build -t canyan/rating-engine:latest .
