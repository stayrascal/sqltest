

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '.coverage' -exec rm -f {} +
	find . -name '.pytest_cache' -exec rm -fr {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '.cache' -exec rm -fr {} +