
test-maxwell:
	cd maxwell && mvn clean test

install:
	cd maxwell && mvn clean install -DskipTests


.PHONY:`install test-maxwell
