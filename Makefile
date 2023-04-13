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

.PHONY: lint
lint:
	autoflake --remove-all-unused-imports --recursive --in-place src/forerunner

.PHONY: all
all: clean lint build publish
