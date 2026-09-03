# Build stage: compiles the jar inside a container with Maven + JDK,
# so nothing needs to be installed on the host to build this image.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Runtime stage: only the JRE and the built jar - no Maven, no source,
# no build tools left in the final image.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]