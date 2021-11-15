# Dataflow Java

- Original create:
  - [GCP inicio rápido de uso de Java y Apache Maven](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven)
  - [Create template](https://cloud.google.com/dataflow/docs/guides/templates/creating-templates#java:-sdk-2.x_7)
- pom.xml original and all templates [DataflowTemplates](https://github.com/GoogleCloudPlatform/DataflowTemplates)

```bash
# create
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=2.33.0 \
    -DgroupId=org.example \
    -DartifactId=word-count-beam \
    -Dversion="0.1" \
    -Dpackage=org.apache.beam.bigtable \
    -DinteractiveMode=false

cd word-count-beam/;
```

## Run this code

```bash
# service account json
credentials.json

# var enviroment
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials.json"

# compile
mvn clean compile;

# local
mvn compile exec:java -Dexec.mainClass=org.apache.beam.bigtable.WordCount -Dexec.args="--project=test1-328716 \
    --inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt \
    --gcpTempLocation=gs://test1-328716-spanner/temp/ \
    --output=gs://test1-328716-spanner/output/salida";


# send dataflow
mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=org.apache.beam.bigtable.WordCount \
    -Dexec.args="--project=test1-328716 \
                 --inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt \
                 --output=gs://test1-328716-spanner/output/salida \
                 --gcpTempLocation=gs://test1-328716-spanner/temp/ \
                 --runner=DataflowRunner \
                 --region=us-central1";


# create template
mvn compile exec:java \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.mainClass=org.apache.beam.bigtable.WordCount \
    -Dexec.args="--runner=DataflowRunner \
                 --project=test1-328716 \
                 --inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt \
                 --output=gs://test1-328716-spanner/output/salida \
                 --stagingLocation=gs://test1-328716-spanner/staging \
                 --templateLocation=gs://test1-328716-spanner/templates/wordcount.json \
                 --region=us-central1";


gcloud dataflow jobs run "jobjava001" \
--gcs-location="gs://test1-328716-spanner/templates/wordcount.json" \
--region="us-central1" \
--project="test1-328716" \
--parameters="inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt,output=gs://test1-328716-spanner/output/salida";

```