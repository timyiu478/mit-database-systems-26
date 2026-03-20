.PHONY: FORCE starter all

pset1.pdf: pset1.tex FORCE
	../bin/latexrun pset1

starter: pset1.zip

pset1.zip: make_starter.sh
	./make_starter.sh

all: pset1.pdf starter

clean:
	rm -f pset1.zip pset1.pdf
