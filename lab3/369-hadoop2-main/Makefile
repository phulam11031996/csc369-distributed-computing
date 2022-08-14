queryone:
	./gradlew run --args="FirstQueryPart1 inputs/access.log inputs/hostname_country.csv one1/"
	./gradlew run --args="FirstQueryOutput one1/ oneoutput/"

querytwo:
	./gradlew run --args="SecondQueryPart1 inputs/access.log inputs/hostname_country.csv two1/"
	./gradlew run --args="SecondQueryPart2 two1/ two2/"

clear:
	rm -rf two1
	rm -rf twooutput
	rm -rf one1
	rm -rf oneoutput

run1:
	make clear
	make queryone

run2:
	make clear
	make querytwo
	
temp:
	rm -rf twooutput
	./gradlew run --args="SecondQueryOutput two2/ twooutput/"