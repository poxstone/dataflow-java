package org.apache.beam.bigtable;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import com.google.common.collect.Lists;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;
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

    /**
     * Query Bigtable for all of the keys that start with the given prefix.
     */
  static class ScanPrefixDoFn extends AbstractCloudBigtableTableDoFn<String, byte[]> {
    private final String tableId;

    public ScanPrefixDoFn(CloudBigtableConfiguration config, String tableId) {
        super(config);
        this.tableId = tableId;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      Scan scan = new Scan()
            .setRowPrefixFilter(c.element().getBytes())
            .setFilter(new KeyOnlyFilter());

      Table table = getConnection().getTable(TableName.valueOf(tableId));

      for (Result result : table.getScanner(scan)) {
        c.output(result.getRow());
      }
    }
  }

    /**
     * Converts a row key into a delete mutation to be written to Bigtable.
     */
  static class DeleteKeyDoFn extends DoFn<byte[], Mutation> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(new Delete(c.element()));
    }
  }

  public static void main(String[] args) {
    CloudBigtableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CloudBigtableOptions.class);
  
      CloudBigtableTableConfiguration bigtableConfig = new CloudBigtableTableConfiguration.Builder()
          .withProjectId(options.getBigtableProjectId())
          .withInstanceId(options.getBigtableInstanceId())
          .withTableId(options.getBigtableTableId())
          .build();
  
      ArrayList<String> prefixes = Lists.newArrayList("Exito", "Carulla");
  
      // randomize the prefixes to avoid hotspoting a region.
      Collections.shuffle(prefixes);
  
      Pipeline pipeline = Pipeline.create(options);
  
      pipeline.apply("Main prefix", Create.of(prefixes))
          .apply("Validate rowkey", ParDo.of(new ScanPrefixDoFn(bigtableConfig, bigtableConfig.getTableId())))
          .apply("Create mutations (Delete)", ParDo.of(new DeleteKeyDoFn()));
          .apply("Delete keys", CloudBigtableIO.writeToTable(bigtableConfig));
  
      pipeline.run();
      //pipeline.run().waitUntilFinish();
    }
  
}