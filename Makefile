.PHONY: coverage
coverage:
	coverage run -m pytest
	coverage report
