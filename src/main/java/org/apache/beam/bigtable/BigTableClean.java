package org.apache.beam.bigtable;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

import org.apache.beam.sdk.options.Description;


public class BigTableClean {

  public interface CloudBigtableOptions extends PipelineOptions {

    @Description("The Google Cloud project ID for the Cloud Bigtable instance.")
    ValueProvider<String> getBigtableProjectId();
    void setBigtableProjectId(ValueProvider<String> value);
  
    @Description("The Google Cloud Bigtable instance ID .")
    ValueProvider<String> getBigtableInstanceId();
    void setBigtableInstanceId(ValueProvider<String> value);
  
    @Description("The Cloud Bigtable table ID in the instance." )
    ValueProvider<String> getBigtableTableId();
    void setBigtableTableId(ValueProvider<String> value);

    @Description("The Cloud Bigtable table ID in the instance." )
    ValueProvider<String> getResultLocation();
    void setResultLocation(ValueProvider<String> value);
    
  }

  // Converts a Long to a String so that it can be written to a file.
  static DoFn<Long, String> stringifier = new DoFn<Long, String>() {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(DoFn<Long, String>.ProcessContext context) throws Exception {
      context.output(context.element().toString());
    }
  };

  public static void main(String[] args) {
    CloudBigtableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CloudBigtableOptions.class);
    
    String PROJECT_ID = options.getBigtableProjectId().get();
    String INSTANCE_ID = options.getBigtableInstanceId().get();
    String TABLE_ID = options.getBigtableTableId().get();

    // [START bigtable_dataflow_connector_scan_config]
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());

    // CloudBigtableTableConfiguration contains the project, zone, cluster and table to connect to.
    // You can supply an optional Scan() to filter the rows that will be read.
    CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withScan(scan)
            .build();

    Pipeline p = Pipeline.create(options);

    p.apply(Read.from(CloudBigtableIO.read(config)))
        .apply(Count.<Result>globally())
        .apply(ParDo.of(stringifier))
        .apply(TextIO.write().to(options.getResultLocation()));
    // [END bigtable_dataflow_connector_scan_config]

    p.run();
    //p.run().waitUntilFinish(); // fail with this

    // Once this is done, you can get the result file via "gsutil cp <resultLocation>-00000-of-00001"
  }
}
