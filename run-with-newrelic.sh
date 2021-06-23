# NEWRELIC_RELEASE is full path to the newrelic.jar (e.g. "/path/to/file.jar")
# NEWRELIC_SNAPSHOT is full path to a snapshot build of the newrelic.jar file
# with ability to attach remote debugger
DEFAULT_JAR=build/libs/*-SNAPSHOT*.jar
yml="${1:-newrelic}"
env="${2:-staging}"
build="${3:-snapshot}"
app="${4:-$DEFAULT_JAR}"

config="$HOME/conf/${yml}.yml"

if [ $build == "release" ]
	then agent=$NEWRELIC_RELEASE
elif [ $build == "snapshot" ]
	then agent=$NEWRELIC_SNAPSHOT
else
	echo "WTF is $build?!"
	exit 1
fi

echo "Running New Relic Java Agent"
echo "CONFIG: $config"
echo "ENV:    $env"
echo "AGENT:  $agent"
echo "APP:    $app"

java -Dnewrelic.config.file=$config -Dnewrelic.environment=$env -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -javaagent:$agent -jar $app