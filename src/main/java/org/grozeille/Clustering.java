package org.grozeille;

import org.apache.commons.cli.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.language.LanguageIdentifier;

import java.util.ArrayList;
import java.util.List;

public class Clustering {

    public static void main(String[] args) throws Exception {

        Option inputOption  = OptionBuilder.withArgName( "input" )
                .isRequired()
                .hasArgs()
                .withDescription( "Input path to analyse." )
                .create( "i" );
        Option outputOption  = OptionBuilder.withArgName( "output" )
                .isRequired()
                .hasArgs()
                .withDescription( "Output path for the result of the analysis." )
                .create( "o" );


        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);

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
        String outputPath = line.getOptionValue("o");


        SparkConf sparkConf = new SparkConf().setAppName("Clustering");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        try {
            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);

            JavaRDD<Row> inputRdd = inputDf.toJavaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    String id = row.getString(0);
                    String text = new String((byte[]) row.get(2), "UTF-8");
                    return RowFactory.create(id, text);
                }
            });

            // The schema is encoded in a string
            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("path", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("body", DataTypes.StringType, true));
            StructType schema = DataTypes.createStructType(fields);

            // Apply the schema to the RDD.
            DataFrame df = sqlContext.createDataFrame(inputRdd, schema);

            Tokenizer tokenizer = new Tokenizer().setInputCol("body").setOutputCol("words");
            DataFrame wordsData = tokenizer.transform(df);
            int numFeatures = 20;
            HashingTF hashingTF = new HashingTF()
                    .setInputCol("words")
                    .setOutputCol("rawFeatures")
                    .setNumFeatures(numFeatures);
            DataFrame featurizedData = hashingTF.transform(wordsData);
            IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
            IDFModel idfModel = idf.fit(featurizedData);
            DataFrame rescaledData = idfModel.transform(featurizedData);
            /*for (Row r : rescaledData.select("features", "path").take(3)) {
                Vector features = r.getAs(0);
                Double label = r.getDouble(1);
                System.out.println(features);
                System.out.println(label);
            }*/

            // Trains a k-means model
            KMeans kmeans = new KMeans()
                    .setK(10)
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction");
            KMeansModel model = kmeans.fit(rescaledData);

            // Shows the result
            Vector[] centers = model.clusterCenters();
            System.out.println("Cluster Centers: ");
            for (Vector center: centers) {
                System.out.println(center);
            }

            JavaRDD<Row> fileClassified = rescaledData.select("features", "path").toJavaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    Vector features = row.getAs(0);
                    String path = row.getAs(1);
                    Integer cluster = model.predict(features);

                    return RowFactory.create(path, cluster);
                }
            });

            // The schema is encoded in a string
            fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("path", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, true));
            schema = DataTypes.createStructType(fields);

            // Apply the schema to the RDD.
            df = sqlContext.createDataFrame(fileClassified, schema);

            df.write().format("com.databricks.spark.avro").save(outputPath);

        }finally {
            sc.close();
        }
    }
}
