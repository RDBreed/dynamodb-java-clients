build_application:
	./mvnw clean compile
run_application:
	docker-compose up -d && ./mvnw spring-boot:run -Dactive.profile=local
test_application:
	./mvnw clean verify