src/io/nats/client/impl/CljNatsKeyValue.class:
	clojure -T:build compile

test:
	clojure -M:dev -m kaocha.runner

clean:
	rm -f clj-nats.jar

clj-nats.jar: src/io/nats/client/impl/CljNatsKeyValue.class
	clojure -M:jar

deploy: clean clj-nats.jar
	mvn deploy:deploy-file -Dfile=clj-nats.jar -DrepositoryId=clojars -Durl=https://clojars.org/repo -DpomFile=pom.xml

.PHONY: clean test deploy
