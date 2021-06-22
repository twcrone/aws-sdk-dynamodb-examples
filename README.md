# Getting Started
AWS v1 and v2 SDK usage for DynamoDB

## Download DynamodDB Locally
https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz

## Run DynamoDB locally
`java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb`

OR use alternate method from here https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html

## Run tests
TBD

`./gradlew test`

## Run app
Creates 'customers' table in local DB if doesn't exist

TBD


### Reference Documentation
For further reference, please consider the following sections:

* [Official Gradle documentation](https://docs.gradle.org)
* [Spring Boot Gradle Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.5.1/gradle-plugin/reference/html/)
* [Create an OCI image](https://docs.spring.io/spring-boot/docs/2.5.1/gradle-plugin/reference/html/#build-image)

### Additional Links
These additional references should also help you:

* [Gradle Build Scans – insights for your project's build](https://scans.gradle.com#gradle)

