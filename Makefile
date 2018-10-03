# python -m pip install -r requirements.txt --target myapp

PYTHON:=$(shell which python3)
PYS:=$(shell find . -name '*.py'|grep -v '^./build/')
LINTS:=$(foreach py, $(PYS), $(dir $(py)).$(basename $(notdir $(py))).lint)

SHELL:=/bin/bash

.PHONY: clean ziptmp lint

all: lint build/o4


build/o4.zip: cli/requirements.txt $(wildcard cli/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r cli/requirements.txt --target $@
	rm -fr $@/*.dist-info
	find $@ -type d -name __pycache__ | xargs rm -fr
	cp -a $< $@

build/o4: build/o4.zip
# Only python3.7 has compress, but it's backwards compatible
	python3.7 -m zipapp -c -p '/usr/bin/env python3' -m o4_sync:main $< -o $@

.%.lint: %.py
	pyflakes $< || true
	yapf -i $<
	@touch $@


lint: $(LINTS)

clean:
	@echo "CLEAN --------------------------------------------"
	rm -f $(LINTS)
	rm -fr build
