package org.grozeille;

import com.lucidworks.spark.SolrSupport;
import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.tika.language.LanguageIdentifier;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class IndexToSolr {

    public static void main(String[] args) throws Exception {

        Option inputOption  = OptionBuilder.withArgName( "input" )
                .isRequired()
                .hasArgs()
                .withDescription( "Input path to analyse." )
                .create( "i" );
        Option langOption  = OptionBuilder.withArgName( "lang" )
                .hasArgs()
                .withDescription( "Input path of lang." )
                .create( "l" );
        Option clusterOption  = OptionBuilder.withArgName( "cluster" )
                .hasArgs()
                .withDescription( "Input path of clusters." )
                .create( "k" );
        Option outputOption  = OptionBuilder.withArgName( "zkhost" )
                .isRequired()
                .hasArgs()
                .withDescription( "Output solr url." )
                .create( "z" );
        Option collectionOption  = OptionBuilder.withArgName( "collection" )
                .isRequired()
                .hasArgs()
                .withDescription( "Solr collection." )
                .create( "c" );


        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(langOption);
        options.addOption(outputOption);
        options.addOption(collectionOption);
        options.addOption(clusterOption);

        // create the parser
        CommandLineParser parser = new BasicParser();
        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "DocumentAnalysis", options );

            System.exit(-1);
        }


        String inputPath = line.getOptionValue("i");
        String langPath = line.getOptionValue("l");
        String clusterPath = line.getOptionValue("k");
        String zkHost = line.getOptionValue("z", "localhost:2181");
        String collection = line.getOptionValue("c", "ineodoc");
        int batchSize = 100;



        SparkConf sparkConf = new SparkConf().setAppName("IndexToSolr");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        try {
            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);
            inputDf = inputDf.selectExpr("path", "fileName", "extension", "body", "'' as lang", "-1 as cluster");

            if(langPath != null) {
                DataFrame langDf = sqlContext.read().format("com.databricks.spark.avro").load(langPath);
                inputDf = inputDf.join(langDf, inputDf.col("path").equalTo(langDf.col("path")))
                        .select(inputDf.col("path"), inputDf.col("fileName"), langDf.col("lang"), inputDf.col("extension"), inputDf.col("body"), inputDf.col("cluster"));
            }
            if(clusterPath != null){
                DataFrame clusterDf = sqlContext.read().format("com.databricks.spark.avro").load(clusterPath);
                inputDf = inputDf.join(clusterDf, inputDf.col("path").equalTo(clusterDf.col("path")))
                        .select(inputDf.col("path"), inputDf.col("fileName"), inputDf.col("lang"), inputDf.col("extension"), inputDf.col("body"), clusterDf.col("cluster"));
            }

            JavaRDD<SolrInputDocument> docs = inputDf.toJavaRDD().map((Function<Row, SolrInputDocument>) row -> {

                String path = row.getAs("path");
                String id = DigestUtils.sha256Hex(path);

                String body = row.getAs("body");
                String fileName = row.getAs("fileName");
                String lang = row.getAs("lang");
                String extension = row.getAs("extension");
                Integer cluster = row.getAs("cluster");

                SolrInputDocument doc = new SolrInputDocument();
                doc.setField("id", id);
                doc.setField("path", path);
                doc.setField("fileName", fileName);
                doc.setField("lang", lang);
                doc.setField("text", body);
                doc.setField("extension", extension);
                doc.setField("cluster", cluster);
                if("fr".equalsIgnoreCase(lang)){
                    doc.setField("text_fr", body);
                }
                else if("en".equalsIgnoreCase(lang)){
                    doc.setField("text_en", body);
                }

                return doc;
            });

            SolrSupport.indexDocs(zkHost, collection, batchSize, docs);

        }finally {
            sc.close();
        }
    }
}
