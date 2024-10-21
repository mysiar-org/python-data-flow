venv::
	rm -rf venv
	python -m venv venv
	venv/bin/pip install -U pip

pip::
	venv/bin/pip install -r requirements.txt
	venv/bin/pip install -r requirements.dev.txt

tests::
	PYTHONPATH=. venv/bin/pytest --cov=mysiar_data_flow --cov-report html --cov-report term -rP tests/ -vvv

lint::
	venv/bin/pflake8 mysiar_data_flow/ tests/

build::
	rm -rf dist
	venv/bin/poetry build


upload-test::
	$(MAKE) build
	venv/bin/python -m twine upload -u $${PYPI_USER} -p $${PYPI_PASS_TEST} --verbose --repository testpypi dist/*

upload::
	$(MAKE) build
	. venv/bin/activate && python -m twine upload -u $${PYPI_USER} -p $${PYPI_PASS} --verbose dist/*
