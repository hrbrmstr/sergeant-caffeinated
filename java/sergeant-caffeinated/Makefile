.PHONY: clean pkg deps run init

pkg:
	mvn --quiet package
	cp target/sergeant-caffeinated-1.0-SNAPSHOT.jar ../../inst/java

clean:
	mvn clean

deps:
	mvn dependency:copy-dependencies -DoutputDirectory=deps

init:
	mvn archetype:generate -DgroupId=is.rud.sergeant-caffeinated -DartifactId=sergeant-caffeinated -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false