all:
	rm -rf test
	make -C src
	ln -s src/test test
clean:
	rm -rf test
	make clean -C src