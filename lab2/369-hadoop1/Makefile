one:
	make clean
	./gradlew run --args="AccessLog input_access_log/ one1/"
	./gradlew run --args="AccessLog2 one1/ one2/"
	cat one2/part-r-00000

two:
	make clean
	./gradlew run --args="Query2Part1 input_access_log/ two1/"
	./gradlew run --args="Query2Part2 two1/ two2/"
	cat two2/part-r-00000

three:
	make clean
	./gradlew run --args="Query3 input_access_log/ three/"
	cat three/part-r-00000

four:
	make clean
	./gradlew run --args="Query4Part1 input_access_log/ four1/"
	./gradlew run --args="Query4Part2 four1/ four2/"
	cat four2/part-r-00000

five:
	make clean
	./gradlew run --args="Query5 input_access_log/ five/"
	cat five/part-r-00000

six:
	make clean
	./gradlew run --args="Query6Part1 input_access_log/ six1/"
	./gradlew run --args="Query6Part2 six1/ six2/"
	cat six2/part-r-00000

clean:
	rm -rf one1
	rm -rf one2
	rm -rf two1
	rm -rf two2
	rm -rf three
	rm -rf four1
	rm -rf four2
	rm -rf five
	rm -rf six1
	rm -rf six2
