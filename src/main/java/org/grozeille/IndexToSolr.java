package org.grozeille;

import com.lucidworks.spark.SolrSupport;
import org.apache.commons.cli.*;
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
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class IndexToSolr {

    public static void main(String[] args) throws Exception {

        Option inputOption  = OptionBuilder.withArgName( "input" )
                .isRequired()
                .hasArgs()
                .withDescription( "Input path to analyse." )
                .create( "i" );
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
        options.addOption(outputOption);
        options.addOption(collectionOption);

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
        String zkHost = line.getOptionValue("z", "localhost:2181");
        String collection = line.getOptionValue("c", "ineodoc");
        int batchSize = 100;



        SparkConf sparkConf = new SparkConf().setAppName("IndexToSolr");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        try {
            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);

            JavaRDD<SolrInputDocument> docs = inputDf.toJavaRDD().map((Function<Row, SolrInputDocument>) row -> {
                SolrInputDocument doc = new SolrInputDocument();
                doc.setField("id", row.getString(0));
                doc.setField("text", new String((byte[]) row.get(2), "UTF-8"));

                return doc;
            });

            SolrSupport.indexDocs(zkHost, collection, batchSize, docs);

        }finally {
            sc.close();
        }
    }
}
