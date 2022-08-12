queryone:
	./gradlew run --args="FirstQueryPart1 inputs/access.log inputs/hostname_country.csv one1/"
	./gradlew run --args="FirstQueryPart2 one1/ one2/"
	./gradlew run --args="FirstQueryOutput one2/ oneoutput/"

clean:
	rm -rf one1
	rm -rf one2
	rm -rf oneoutput

run1:
	make clean
	make queryone
