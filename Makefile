venv::
	rm -rf venv
	python -m venv venv
	venv/bin/pip install -U pip

pip::
	venv/bin/pip install -r requirements.txt
	venv/bin/pip install -r requirements.dev.txt

tests::
	PYTHONPATH=. venv/bin/pytest -rP tests/ -vvv --cov=data_flow --cov-report html --cov-report term

lint::
	venv/bin/flake8 data_flow/
