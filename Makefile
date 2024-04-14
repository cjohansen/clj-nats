test:
	clojure -M:dev -m kaocha.runner

clj-nats.jar:
	rm -f clj-nats.jar && clojure -M:jar

deploy: clj-nats.jar
	mvn deploy:deploy-file -Dfile=clj-nats.jar -DrepositoryId=clojars -Durl=https://clojars.org/repo -DpomFile=pom.xml

.PHONY: test deploy
