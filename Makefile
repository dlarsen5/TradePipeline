build-flink:
	cd api/flink && mvn clean package

submit-flink:
	cd api/flink && flink -m localhost:8081 -c org.neutron.TradeJob target/flink-project_2.11-0.1-SNAPSHOT-jar-with-dependencies.jar

run: build-flink
	docker-compose --env-file .env up

clean:
	find . -name "__pycache__" | xargs  rm -rf
	find . -name "*.pyc" | xargs rm -rf
