package com.ngfs.unittestengine;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import com.ngfs.unittestengine.bolt.HdfsBolt;
import com.ngfs.unittestengine.bolt.format.DefaultFileNameFormat;
import com.ngfs.unittestengine.bolt.format.DelimitedRecordFormat;
import com.ngfs.unittestengine.bolt.format.FileNameFormat;
import com.ngfs.unittestengine.bolt.format.RecordFormat;
import com.ngfs.unittestengine.bolt.rotation.FileRotationPolicy;
import com.ngfs.unittestengine.bolt.rotation.FileSizeRotationPolicy;
import com.ngfs.unittestengine.bolt.rotation.FileSizeRotationPolicy.Units;
import com.ngfs.unittestengine.bolt.sync.CountSyncPolicy;
import com.ngfs.unittestengine.bolt.sync.SyncPolicy;




public class UnitTestTopology {

	public static void main(String[] args) {
		// Build Topology
		System.out.println("Storm In Action");
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("Unit-Test-Spout", new UnitTestSpout());
		topologyBuilder.setBolt("Unit-Test-Bolt", new UnitTestBolt()).shuffleGrouping("Unit-Test-Spout");

		StormTopology topology = topologyBuilder.createTopology();

		// Configuration
		Config conf = new Config();
		conf.setDebug(true);
		conf.put("fileToWrite", "/home/shahadathossain/UnitTestLog.txt");
		
		
		
		
//		RecordFormat format = new DelimitedRecordFormat()
//				.withFieldDelimiter(",");
//
//		// sync the filesystem after every 1k tuples
//		SyncPolicy syncPolicy = new CountSyncPolicy(1000);
//
//		// rotate files when they reach 5MB
//		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f,
//				Units.MB);
//
//		FileNameFormat fileNameFormatHDFS = new DefaultFileNameFormat()
//				.withPath("/user").withPrefix("record-").withExtension(".csv");
//
//		HdfsBolt hdfsBolt2 = new HdfsBolt().withFsUrl("hdfs://localhost:9000")
//				.withFileNameFormat(fileNameFormatHDFS)
//				.withRecordFormat(format).withRotationPolicy(rotationPolicy)
//				.withSyncPolicy(syncPolicy);

		//
		//RecordFormat format = new DelimitedRecordFormat()
		  //      .withFieldDelimiter(",");

		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
		        .withPath("/unit");

		HdfsBolt hdfsBolt2 = new HdfsBolt()
		        .withFsUrl("hdfs://localhost:9000")
		        .withFileNameFormat(fileNameFormat)
		     //   .withRecordFormat(format)
		        .withRotationPolicy(rotationPolicy)
		        .withSyncPolicy(syncPolicy);
		topologyBuilder.setBolt("HDFS2", hdfsBolt2,1).shuffleGrouping("Unit-Test-Bolt");
		System.out.println("HDFS Bolt Called..............................................................................");
		// Submit Topology to Cluster

		LocalCluster cluster = new LocalCluster();
		
		try {
			cluster.submitTopology("Unit-Test-Topology", conf, topology);
			//StormSubmitter.submitTopology("Unit-Test-Topology", conf, topology);
			Thread.sleep(1000);
		} catch (Exception e) {
		} finally {
			//cluster.shutdown();
		}

	}

}
