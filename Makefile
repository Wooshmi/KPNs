OCAMLBUILD=ocamlbuild -classic-display \
		-tags annot,debug,thread \
		-libs unix,str
TARGET=byte

example:
	$(OCAMLBUILD) example.$(TARGET)
	$(OCAMLBUILD) hamilton.$(TARGET)
	$(OCAMLBUILD) automata.$(TARGET)

clean:
	$(OCAMLBUILD) -clean

realclean: clean
	rm -f *~

cleanall: realclean
