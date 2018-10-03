# python -m pip install -r requirements.txt --target myapp

PYTHON:=$(shell which python3)
PYS:=$(shell find . -name '*.py'|grep -v '^./build/')
LINTS:=$(foreach py, $(PYS), $(dir $(py)).$(basename $(notdir $(py))).lint)
EXES:=build/o4 build/gatling build/manifold

SHELL:=/bin/bash

.PHONY: clean lint install uninstall

all: lint $(EXES)

build/o4.za: o4/requirements.txt $(wildcard o4/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/gatling.za: gatling/requirements.txt $(wildcard gatling/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/manifold.za: gatling/requirements.txt $(wildcard gatling/*.py) $(wildcard manifold/*.py)
	mkdir -p $@
	${PYTHON} -m pip install -r $< --target $@
	cp -a $^ $@

build/%.za/version.py: %/version.py versioning.py
	${PYTHON} versioning.py -r $(dir $@)/requirements.txt -z $(dir $@) -o $<
	cp -a $< $@

build/%: build/%.za build/%.za/version.py
	rm -fr $</*.dist-info
	find $< -type d -name __pycache__ | xargs rm -fr
# Only python3.7 has compress, but it's backwards compatible
	${PYTHON} -m zipapp -p '/usr/bin/env python3' -m $(notdir $@):main $< -o $@

.%.lint: %.py
	pyflakes $< || true
	yapf -i $<
	@touch $@

install: $(EXES)
	install -d /usr/local/bin
	install $^ /usr/local/bin

uninstall: $(EXES)
	rm -f $(foreach exe, $^, /usr/local/bin/$(notdir $(exe)))


lint: $(LINTS)

clean:
	@echo "CLEAN --------------------------------------------"
	rm -f $(LINTS)
	rm -fr build
