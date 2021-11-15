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

## Run this code Word-count

```bash
PROJECT_ID="co-myproject-rnkofer-dev";
BT_INSTANCE="rnkofertasbigtable-dev";
BT_TABLE="OutputRankingOfertasColumns";
BUCKET_INPUT_FILE="gs://apache-beam-samples/shakespeare/kinglear.txt";
BUCKET_OUTPUT_FILE="gs://co-myproject-temporal-ranking-dev/df-salida-java/output.txt";
BUCKET_OUTPUT_DIR="gs://co-myproject-temporal-ranking-dev/df-output-java/";
BUCKET_TEMP="gs://co-myproject-temporal-ranking-dev/df-temp-java/";
BUCKET_STAGING="gs://co-myproject-solicitudes-ranking-dev/templates/df-staging-java";
BUCKET_TPL1="gs://co-myproject-solicitudes-ranking-dev/templates/WordCount.json";
BUCKET_TPL2="gs://co-myproject-solicitudes-ranking-dev/templates/BigTableCount.json";
BUCKET_TPL3="gs://co-myproject-solicitudes-ranking-dev/templates/BigTableClean.json";
REGION="us-west1";


# service account json
credentials.json

# var enviroment
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials.json"

# compile
mvn clean compile;

# local
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.WordCount \
-Dexec.args="--inputFile=${BUCKET_INPUT_FILE} \
--gcpTempLocation=${BUCKET_TEMP} \
--output=${BUCKET_OUTPUT_DIR} \
--project=${PROJECT_ID}";

# send dataflow
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.WordCount \
-Dexec.args="--inputFile=${BUCKET_INPUT_FILE} \
--output=${BUCKET_OUTPUT_DIR} \
--gcpTempLocation=${BUCKET_TEMP} \
--runner=DataflowRunner \
--project=${PROJECT_ID} \
--region=${REGION}";


# create template
mvn compile exec:java \
-Dexec.cleanupDaemonThreads=false \
-Dexec.mainClass=org.apache.beam.bigtable.WordCount \
-Dexec.args="--inputFile=${BUCKET_INPUT_FILE} \
--output=${BUCKET_OUTPUT_DIR} \
--stagingLocation=${BUCKET_STAGING} \
--templateLocation=${BUCKET_TPL1} \
--runner=DataflowRunner \
--project=${PROJECT_ID} \
--region=${REGION}";


gcloud dataflow jobs run "jobjava001" \
--gcs-location="${BUCKET_TPL1}" \
--region="${REGION}" \
--project="${PROJECT_ID}" \
--parameters="inputFile=${BUCKET_INPUT_FILE},output=${BUCKET_OUTPUT_DIR}";

```


## Run this code BigTableCount

```bash
# service account json
credentials.json

# var enviroment
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials.json"

# compile
mvn clean compile;

# local
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableCount \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE}";

# send dataflow
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableCount \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE} \
--runner=DataflowRunner \
--project=${PROJECT_ID} \
--region=${REGION}";

# template
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableCount \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE} \
--stagingLocation=${BUCKET_STAGING} \
--gcpTempLocation=${BUCKET_TEMP} \
--templateLocation=${BUCKET_TPL2} \
--runner=dataflow \
--project=${PROJECT_ID} \
--region=${REGION}";

```


## Run this code BigTableClean

```bash
# service account json
credentials.json

# var enviroment
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials.json"

# compile
mvn clean compile;

# local
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableClean \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE}";

# send dataflow
mvn -Pdataflow-runner compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableClean \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE} \
--runner=DataflowRunner \
--project=${PROJECT_ID} \
--region=${REGION}";

# template
mvn compile exec:java \
-Dexec.mainClass=org.apache.beam.bigtable.BigTableClean \
-Dexec.args="--resultLocation=${BUCKET_OUTPUT_FILE} \
--bigtableProjectId=${PROJECT_ID} \
--bigtableInstanceId=${BT_INSTANCE} \
--bigtableTableId=${BT_TABLE} \
--stagingLocation=${BUCKET_STAGING} \
--gcpTempLocation=${BUCKET_TEMP} \
--templateLocation=${BUCKET_TPL3} \
--runner=dataflow \
--project=${PROJECT_ID} \
--region=${REGION}";

```
