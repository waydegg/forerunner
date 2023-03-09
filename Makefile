.DEFAULT_GOAL := all

.PHONY: build
build:
	hatch build

.PHONY: publish
publish:
	hatch publish --repo main --user __token__ --auth ${PYPI_TOKEN}

.PHONY: clean
clean:
	rm -rf dist/*

.PHONY: all
all: clean build publish
