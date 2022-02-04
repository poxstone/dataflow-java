package org.apache.beam.bigtable;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Locale;

import com.google.common.collect.Lists;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableConfiguration;

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
    
	static Boolean isObsoleteDate(String key_row) throws ParseException {
		String[] key_split = key_row.split(".+#");
		String sdate_row = key_split[key_split.length - 1];
		
		if (!sdate_row.matches("^\\d{4}-\\d{1,2}-\\d{1,2}$")) {
			return false;
		}
		
		// init compare
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(new Date());
		
		String sdate_now = String.format("%s-%s-%s", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
		Long date_row = new SimpleDateFormat("yyyy-M-d", Locale.ENGLISH).parse(sdate_row).getTime();
		// time minus 1 day to day
		Long date_com = new SimpleDateFormat("yyyy-M-d", Locale.ENGLISH).parse(sdate_now).getTime() - 86400000;
		
		if (date_row < date_com) {
			return true;
		} else {
			return false;
		}
		
	}

  static Boolean isObsoleteDataDate(String data_row) throws ParseException {
		//System.out.println("data_row::::  " + data_row);
		String[] data_split = data_row.split(".+\\|");
		String sdate_row = data_split[data_split.length - 1];
		//System.out.println("data_split::::  " + sdate_row);
		if (!sdate_row.matches("^\\d{4}-\\d{1,2}-\\d{1,2}$")) {
			return false;
		}
		
		// init compare
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(new Date());
		
		String sdate_now = String.format("%s-%s-%s", calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DAY_OF_MONTH));
		Long date_row = new SimpleDateFormat("yyyy-M-d", Locale.ENGLISH).parse(sdate_row).getTime();
		// time minus 1 day to day
		Long date_com = new SimpleDateFormat("yyyy-M-d", Locale.ENGLISH).parse(sdate_now).getTime() - 86400000;
		
		if (date_row < date_com) {
			return true;
		} else {
			return false;
		}
		
	}

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException, ParseException {
      Scan scan = new Scan()
            .setRowPrefixFilter(c.element().getBytes());
            //.setFilter(new KeyOnlyFilter());

      Table table = getConnection().getTable(TableName.valueOf(tableId));

      for (Result result : table.getScanner(scan)) {
        byte[] keyRow = result.getRow();
        //byte[] keyData = result.getValue(byte[] family, byte[] qualifier);
        byte[] keyData = result.value();
        
        String keyRowString = new String(keyRow, StandardCharsets.UTF_8);
        String keyDataString = new String(keyData, StandardCharsets.UTF_8);// + "|2021-12-24";
        //System.out.println("Value:  " + keyDataString);
        //if (isObsoleteDate(keyRowString)) {
        if (isObsoleteDataDate(keyDataString)) {
          //System.out.println("Se borra:  " + keyRowString);
          c.output(result.getRow());
        } else {
          //System.out.println("no se borra:  " + keyRowString);
        }
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
        // IMPORTANT: the array list must be equal to startwith rowkey case sensitive
    ArrayList<String> prefixes = Lists.newArrayList("prefix1", "prefix2", "prefix3", "prefix4");
    
    // randomize the prefixes to avoid hotspoting a region.
    Collections.shuffle(prefixes);
    
    Pipeline pipeline = Pipeline.create(options);
    
    pipeline.apply("Main prefix", Create.of(prefixes))
    .apply("Validate rowkey", ParDo.of(new ScanPrefixDoFn(bigtableConfig, bigtableConfig.getTableId())))
    .apply("Create mutations (Delete)", ParDo.of(new DeleteKeyDoFn()))//;
    .apply("Delete keys", CloudBigtableIO.writeToTable(bigtableConfig));
    
    pipeline.run();
    //pipeline.run().waitUntilFinish();
  }
  
}
