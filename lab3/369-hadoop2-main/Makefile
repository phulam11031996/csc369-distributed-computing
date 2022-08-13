queryone:
	./gradlew run --args="FirstQueryPart1 inputs/access.log inputs/hostname_country.csv one1/"
	./gradlew run --args="FirstQueryPart2 one1/ one2/"
	./gradlew run --args="FirstQueryOutput one2/ oneoutput/"

querytwo:
	./gradlew run --args="SecondQueryPart1 inputs/access.log inputs/hostname_country.csv two1/"
	./gradlew run --args="SecondQueryPart2 two1/ two2/"

clean:
	rm -rf one1
	rm -rf one2
	rm -rf oneoutput
	rm -rf two1
	rm -rf two2

run1:
	make clean
	make queryone

run2:
	make clean
	make querytwo

querytwotemp:
	./gradlew run --args="SecondQueryOutput two2/ output/"

temp:
	rm -rf output/
	make querytwotemp