package org.grozeille;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.encryption.StandardDecryptionMaterial;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.hpsf.Property;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.bytedeco.javacpp.BytePointer;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.bytedeco.javacpp.tesseract.TessBaseAPI;

@Slf4j
public class DocumentAnalysisAvroEncryptedPdf {
    public static void main(String[] args) throws Exception {
        System.setProperty("jna.encoding", "UTF8");

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
        Option tesseractPathOption  = OptionBuilder.withArgName( "tesseract-path" )
                .isRequired()
                .hasArgs()
                .withDescription( "Tesseract path." )
                .create( "t" );
        Option tesseractLangOption  = OptionBuilder.withArgName( "tesseract-lang" )
                .isRequired()
                .hasArgs()
                .withDescription( "Tesseract default language. Default fra." )
                .create( "l" );

        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(tesseractPathOption);
        options.addOption(tesseractLangOption);

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
        String tesseractPath = line.getOptionValue("t");
        String tesseractLang = line.getOptionValue("l", "fra");


        SparkConf sparkConf = new SparkConf().setAppName("DocumentAnalysis");
        sparkConf.set("mapreduce.input.fileinputformat.input.dir.recursive","false");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        try {
            SQLContext sqlContext = new SQLContext(sc);

            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);
            JavaRDD<Row> output = inputDf.toJavaRDD().map(new Function<Row, Row>() {

                private final PdfOcrUtil pdfOcrUtil = new PdfOcrUtil(tesseractPath, tesseractLang);

                @Override
                public Row call(Row row) throws Exception {

                    if(!pdfOcrUtil.isTessInitialized()){
                        pdfOcrUtil.init();
                    }

                    StringBuilder outputText = null;

                    String path = row.getAs("path");
                    byte[] body = row.getAs("body");

                    String extension = FilenameUtils.getExtension(path);

                    if("pdf".equalsIgnoreCase(extension)) {

                        try {

                            try (ByteArrayInputStream stream = new ByteArrayInputStream(body)) {

                                // load all pages of the PDF and search for images
                                try (PDDocument document = PDDocument.load(stream)) {

                                    if(document.isEncrypted()) {
                                        try {

                                            // try to decrypt
                                            StandardDecryptionMaterial sdm = new StandardDecryptionMaterial("");
                                            document.openProtection(sdm);
                                            document.decrypt("");
                                            document.setAllSecurityToBeRemoved(true);
                                            log.info("Successfully decrypted PDF: " + path);

                                            // TODO

                                        } catch (Exception ex) {
                                            log.warn("Unable to decrypt PDF: " + path.toString());

                                            PDDocumentInformation info = document.getDocumentInformation();

                                            outputText = new StringBuilder();
                                            outputText.append(info.getTitle()).append("\n");
                                            outputText.append(info.getAuthor()).append("\n");
                                            outputText.append(info.getSubject()).append("\n");
                                            outputText.append(info.getKeywords()).append("\n");
                                            outputText.append(info.getCreator()).append("\n");
                                            outputText.append(info.getProducer()).append("\n");
                                            outputText.append(info.getCreationDate()).append("\n");
                                            outputText.append(info.getModificationDate()).append("\n");

                                            outputText.append(this.pdfOcrUtil.parsePdfOcr(path, document)).append("\n");
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception ex){
                            log.error("Unable to parse PDF document", ex);
                        }
                    }

                    return RowFactory.create(path, outputText == null ? null : outputText.toString());
                }
            });

            List<StructField> fields = new ArrayList<>();
            fields.add(DataTypes.createStructField("path", DataTypes.StringType, true));
            fields.add(DataTypes.createStructField("body", DataTypes.StringType, true));
            StructType schema = DataTypes.createStructType(fields);
            DataFrame outputDf = sqlContext.createDataFrame(output, schema);

            outputDf.where(outputDf.col("body").isNotNull()).write().format("com.databricks.spark.avro").save(outputPath);


        }finally {
            sc.close();
        }
    }
}
