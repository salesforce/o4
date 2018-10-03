# python -m pip install -r requirements.txt --target myapp

PYTHON:=$(shell which python3)
PYS:=$(shell find . -name '*.py'|grep -v '^./build/')
LINTS:=$(foreach py, $(PYS), $(dir $(py)).$(basename $(notdir $(py))).lint)

SHELL:=/bin/bash

.PHONY: clean ziptmp lint

all: lint build/o4 build/gatling build/manifold

build/o4.za: cli/requirements.txt $(wildcard cli/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/gatling.za: gatling/requirements.txt $(wildcard gatling/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/manifold.za: gatling/requirements.txt $(wildcard gatling/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/%: build/%.za
	rm -fr $</*.dist-info
	find $< -type d -name __pycache__ | xargs rm -fr
# Only python3.7 has compress, but it's backwards compatible
	${PYTHON} -m zipapp -p '/usr/bin/env python3' -m $(notdir $@):main $< -o $@


.%.lint: %.py
	pyflakes $< || true
	yapf -i $<
	@touch $@


lint: $(LINTS)

clean:
	@echo "CLEAN --------------------------------------------"
	rm -f $(LINTS)
	rm -fr build
