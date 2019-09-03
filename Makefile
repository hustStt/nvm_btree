all:
	rm -rf test
	make -C src
	# ln -s src/single_test test
	ln -s src/mult_test test
clean:
	rm -rf test
	make clean -C src

install:
	make -C src $@

uninstall:
	make -C src $@