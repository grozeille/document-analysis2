package org.grozeille;

import org.apache.commons.cli.*;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
import scala.Tuple2;

import java.io.*;
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
        Option modelOption  = OptionBuilder.withArgName( "model" )
                .isRequired()
                .hasArgs()
                .withDescription( "Output path for the model of the analysis." )
                .create( "m" );
        Option clusterOption  = OptionBuilder.withArgName( "cluster" )
                .isRequired()
                .hasArgs()
                .withDescription( "Number of cluster. Default 20." )
                .create( "c" );


        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(modelOption);
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
        String outputPath = line.getOptionValue("o");
        String modelPath = line.getOptionValue("m");
        Integer clusterNumber = Integer.parseInt(line.getOptionValue("c", "20"));


        SparkConf sparkConf = new SparkConf().setAppName("Clustering");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        try {
            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);

            //Tokenizer tokenizer = new Tokenizer().setInputCol("body").setOutputCol("words");
            //DataFrame wordsData = tokenizer.transform(df);

            DataFrame wordsData = tokenize(inputDf);

            int numFeatures = 1000;
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
                    .setK(clusterNumber)
                    .setMaxIter(40)
                    .setFeaturesCol("features")
                    .setPredictionCol("prediction");
            KMeansModel model = kmeans.fit(rescaledData);

            // save model
            File modelFile =  new File(new File(modelPath), "kmeans.ser");
            try(ObjectOutputStream oos =  new ObjectOutputStream(new FileOutputStream(modelFile))) {
                oos.writeObject(model);
            }

            // Shows the result
            Vector[] centers = model.clusterCenters();
            System.out.println("Cluster Centers: ");
            for (Vector center: centers) {
                System.out.println(center);
            }

            JavaRDD<Row> fileClassified = rescaledData.select("features", "path").toJavaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    Vector features = row.getAs("features");
                    String path = row.getAs("path");
                    Integer cluster = model.predict(features);

                    return RowFactory.create(path, cluster);
                }
            });

            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("path", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, true));
            StructType schema = DataTypes.createStructType(fields);
            DataFrame df = sqlContext.createDataFrame(fileClassified, schema);

            df.write().format("com.databricks.spark.avro").save(outputPath);

        }finally {
            sc.close();
        }
    }

    static private DataFrame tokenize(DataFrame inputDf){
        JavaRDD<Row> ngrams = inputDf.toJavaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String id = row.getAs("path");
                String text = row.getAs("body");
                Reader reader = new StringReader(text);

                List<String> result = new ArrayList<>();

                StandardTokenizer standardTokenizer = new StandardTokenizer();
                standardTokenizer.setReader(reader);
                final CharTermAttribute standardCharTermAttribute = standardTokenizer.addAttribute(CharTermAttribute.class);

                standardTokenizer.reset();
                while(standardTokenizer.incrementToken()){
                    result.add(standardCharTermAttribute.toString());
                }


                standardTokenizer = new StandardTokenizer();
                standardTokenizer.setReader(reader);
                final ShingleFilter tokenizer = new ShingleFilter(standardTokenizer, 3);
                final CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);

                tokenizer.reset();
                while(tokenizer.incrementToken()){
                    result.add(charTermAttribute.toString());
                }


                return RowFactory.create(id, result.toArray(new String[0]));

                    /*
                    return () -> new Iterator<Tuple2<String, String>>() {
                        @Override
                        public boolean hasNext() {
                            try {
                                return tokenizer.incrementToken();
                            } catch (IOException e) {
                                return false;
                            }
                        }

                        @Override
                        public Tuple2<String, String> next() {
                            return new Tuple2<>(id, charTermAttribute.toString());
                        }
                    };*/
            }
        });

        List<StructField> fields;
        fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("path", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("words", DataTypes.createArrayType(DataTypes.StringType), true));
        StructType schema = DataTypes.createStructType(fields);

        return inputDf.sqlContext().createDataFrame(ngrams, schema);
    }
}
