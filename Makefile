$(info $(SHELL))

PY = python3
VENV = venv
BIN=$(VENV)/BIN

#Check for Windows installation
ifeq ($(OS), Windows_NT)
	BIN=$(VENV)/Scripts
	PY=python
endif

run:
	$(PY) run.py $(ARGS)

install: requirements.txt
	pip install -r requirements.txt

build: setup.py 
	$(PY) setup.py build bdist_wheel

clean:
	if exist build rd /s /q build
	if exist dist rd /s /q dist
	if exist boxoffice_etl.egg-info rd /s /q boxoffice_etl.egg-info

