PYTHON:=venv/bin/python
VULTURE:=venv/bin/vulture
YAPF:=venv/bin/yapf
PYFLAKES:=venv/bin/pyflakes
PIP:=venv/bin/pip
PYS:=$(shell find . -name '*.py'|grep -v '^./build/' | grep -v ^'./venv/'|grep -v '/version.py$$')
O4_SRC:=o4/requirements.txt $(shell find o4 -name '*.py'|grep -v version.py)
GATLING_SRC:=gatling/requirements.txt $(shell find gatling -name '*.py'|grep -v '/version.py$$')
MANIFOLD_SRC:=$(shell find manifold -name '*.py'|grep -v version.py)

ifeq (,$(wildcard venv))
X:=$(shell python3 -m venv venv)
Y:=$(shell ${PIP} install --upgrade pip)
Z:=$(shell ${PIP} install -r requirements.txt)
endif

PYTHON_MAJ:=$(shell ${PYTHON} -c 'import sys; print(sys.version_info.major)')
PYTHON_MIN:=$(shell ${PYTHON} -c 'import sys; print(sys.version_info.minor)')
ifneq ($(PYTHON_MAJ), 3)
	$(error "*** ERROR: Python must be version 3.")
endif
ZA_C:=$(if $(filter 0 1 2 3 4 5 6, $(PYTHON_MIN)),,-c)
LINTS:=$(foreach py, $(PYS), $(dir $(py)).$(basename $(notdir $(py))).lint)
EXES:=build/o4 build/gatling build/manifold

SHELL:=/bin/bash

.PHONY: clean lint install uninstall

all: lint $(EXES)

o4/version.py: $(O4_SRC) versioning.py
	${PYTHON} versioning.py -r $< -o $@ $^

gatling/version.py: $(GATLING_SRC) versioning.py
	${PYTHON} versioning.py -r $< -o $@ $^

manifold/version.py: $(GATLING_SRC) $(MANIFOLD_SRC) versioning.py
	${PYTHON} versioning.py -r $< -o $@ $^

build/o4.za: $(O4_SRC) o4/version.py
	mkdir -p $@
	${PIP} install -U -r $< --target $@
	cp -a $^ $@

build/gatling.za: $(GATLING_SRC) gatling/version.py
	mkdir -p $@
	${PIP} install -U -r $< --target $@
	cp -a $^ $@

build/manifold.za: $(GATLING_SRC) $(MANIFOLD_SRC) manifold/version.py
	mkdir -p $@
	${PIP} install -U -r $< --target $@
	cp -a $^ $@

build/%: build/%.za
	rm -fr $</*.dist-info
	find $< -type d -name __pycache__ | xargs rm -fr
# Only python3.7 has compress, but it's backwards compatible
	${PYTHON} -m zipapp $(ZA_C) -p '/usr/bin/env python3' -m $(notdir $@):main $< -o $@

.%.lint: %.py venv
	${PYFLAKES} $< || true
	${VULTURE} $< || true
	${YAPF} -i $<
	@touch $@

install: $(EXES)
	install -d /usr/local/bin
	install $^ /usr/local/bin

uninstall: $(EXES)
	rm -f $(foreach exe, $^, /usr/local/bin/$(notdir $(exe)))

lint: $(LINTS)

venv:
	python3 -m venv venv
	${PIP} install --upgrade pip
	${PIP} install -U -r requirements.txt

clean:
	@echo "CLEAN --------------------------------------------"
	rm -f $(LINTS)
	rm -fr build


##
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
