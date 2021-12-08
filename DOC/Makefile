clean:
	rm -f $$(ls *.* | grep -v '\.pdf\|\.tex\|\.md\|\.bib')
	rm -rf auto

squeaky:
	make clean
	rm -f *.pdf

paper:
	make clean
	pdflatex paper_chequeDishonour.tex
	pdflatex paper_chequeDishonour.tex
	pdflatex paper_chequeDishonour.tex
	makeglossaries paper_chequeDishonour
	pdflatex paper_chequeDishonour.tex
	pdflatex paper_chequeDishonour.tex
	biber paper_chequeDishonour
	pdflatex paper_chequeDishonour.tex
	pdflatex paper_chequeDishonour.tex
	pdflatex paper_chequeDishonour.tex
	make clean
