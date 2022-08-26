queryone:
	./gradlew run --args="FirstQueryPart1 inputs/access.log inputs/hostname_country.csv one1/"
	./gradlew run --args="FirstQueryPart2 one1/ one2/"
	./gradlew run --args="FirstQueryOutput one2/ oneoutput/"
	cat oneoutput/part-r-00000

querytwo:
	./gradlew run --args="SecondQueryPart1 inputs/access.log inputs/hostname_country.csv two1/"
	./gradlew run --args="SecondQueryPart2 two1/ two2/"
	./gradlew run --args="SecondQueryOutput two2/ twooutput/"
	cat twooutput/part-r-00000

clear:
	rm -rf two1
	rm -rf two2
	rm -rf twooutput
	rm -rf one1
	rm -rf one2
	rm -rf oneoutput

run1:
	make clear
	make queryone

run2:
	make clear
	make querytwo
	
temp:
	rm -rf oneoutput/
	./gradlew run --args="FirstQueryOutput one2/ oneoutput/"