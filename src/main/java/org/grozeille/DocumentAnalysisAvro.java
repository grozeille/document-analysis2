package org.grozeille;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.encryption.StandardDecryptionMaterial;
import org.apache.pdfbox.util.PDFTextStripper;
import org.apache.poi.POIXMLProperties;
import org.apache.poi.hpsf.Property;
import org.apache.poi.hpsf.PropertySetFactory;
import org.apache.poi.hpsf.SummaryInformation;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;

import java.io.*;
import java.text.SimpleDateFormat;

import static org.bytedeco.javacpp.tesseract.TessBaseAPI;

@Slf4j
public class DocumentAnalysisAvro {
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
        Option ocrOption  = OptionBuilder.withArgName( "ocr" )
                .withDescription( "OCR ?" )
                .create( "c" );
        Option tesseractPathOption  = OptionBuilder.withArgName( "tesseract-path" )
                .hasArgs()
                .withDescription( "Tesseract path." )
                .create( "t" );
        Option tesseractLangOption  = OptionBuilder.withArgName( "tesseract-lang" )
                .hasArgs()
                .withDescription( "Tesseract default language. Default fra." )
                .create( "l" );

        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(tesseractPathOption);
        options.addOption(tesseractLangOption);
        options.addOption(ocrOption);

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
        Boolean withOcr = line.hasOption("c");


        SparkConf sparkConf = new SparkConf().setAppName("DocumentAnalysis");
        sparkConf.set("mapreduce.input.fileinputformat.input.dir.recursive","false");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        try {
            SQLContext sqlContext = new SQLContext(sc);

            Accumulator<Integer> documentParsedAccumulator = sc.accumulator(0);
            Accumulator<Integer> documentErrorAccumulator = sc.accumulator(0);

            UDF2<String, byte[], String> extractTextUdf = new DocumentTextParserUdf(tesseractPath, tesseractLang, documentParsedAccumulator, documentErrorAccumulator, withOcr);
            sqlContext.udf().register("extractText", extractTextUdf, DataTypes.StringType);

            UDF1<String, String> extractFileNameUdf = (UDF1<String, String>) s -> FilenameUtils.getName(s);
            sqlContext.udf().register("extractFileName", extractFileNameUdf, DataTypes.StringType);

            UDF1<String, String> extractExtensionUdf = (UDF1<String, String>) s -> FilenameUtils.getExtension(s);
            sqlContext.udf().register("extractExtension", extractExtensionUdf, DataTypes.StringType);

            UDF1<String, String> detectLangUdf = (UDF1<String, String>) s -> {

                LanguageIdentifier identifier = new LanguageIdentifier(s);
                return identifier.isReasonablyCertain() ? identifier.getLanguage() : "";
            };
            sqlContext.udf().register("detectLang", detectLangUdf, DataTypes.StringType);

            DataFrame inputDf = sqlContext.read().format("com.databricks.spark.avro").load(inputPath);
            sqlContext.registerDataFrameAsTable(inputDf, "raw");

            sqlContext.sql("select R.path, R.body, R.fileName, R.extension, detectLang(R.body) as lang from (" +
                    "select path, extractText(path, body) as body, extractFileName(path) as fileName, extractExtension(path) as extension from raw) R")
                    .write().format("com.databricks.spark.avro").save(outputPath);


            log.info("Parsed: " + documentParsedAccumulator.value());
            log.info("Error: " + documentErrorAccumulator.value());
        }finally {
            sc.close();
        }
    }

    @RequiredArgsConstructor
    private static class DocumentTextParserUdf implements UDF2<String, byte[], String> {

        private transient DocumentTextParser documentTextParser;

        private final String tesseractPath;
        private final String tesseractLang;
        private final Accumulator<Integer> documentParsedAccumulator;
        private final Accumulator<Integer> documentErrorAccumulator;
        private final Boolean withOcr;

        @Override
        public String call(String path, byte[] body) throws Exception {
            if(documentTextParser == null){
                documentTextParser = new DocumentTextParser(tesseractPath, tesseractLang, documentParsedAccumulator, documentErrorAccumulator, withOcr);
            }
            return documentTextParser.call(path, body);
        }
    }

    @RequiredArgsConstructor
    @Slf4j
    private static class DocumentTextParser implements Function2<String, byte[], String>, Closeable {

        private transient Tika tika;
        private transient Metadata metadata;

        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private final PdfOcrUtil pdfOcrUtil;
        private final Accumulator<Integer> documentParsedAccumulator;
        private final Accumulator<Integer> documentErrorAccumulator;
        private final Boolean withOcr;

        public DocumentTextParser(String tesseractPath, String tesseractLang, Accumulator<Integer> documentParsedAccumulator, Accumulator<Integer> documentErrorAccumulator, Boolean withOcr){

            this.pdfOcrUtil = new PdfOcrUtil(tesseractPath, tesseractLang);
            this.withOcr = withOcr;
            this.documentParsedAccumulator = documentParsedAccumulator;
            this.documentErrorAccumulator = documentErrorAccumulator;
        }

        @Override
        public String call(String path, byte[] body) throws Exception {

            if(tika == null) {
                tika = new Tika();
                metadata = new Metadata();
                metadata.add(Metadata.CONTENT_ENCODING, "UTF-8");
            }
            if(withOcr && !this.pdfOcrUtil.isTessInitialized()){
                this.pdfOcrUtil.init();
            }

            try(ByteArrayInputStream stream = new ByteArrayInputStream(body)) {
                return parseDocument(path, stream);
            }
        }

        private String parseDocument(String path, InputStream inputStream) throws IOException, TikaException {

            log.info("Parsing file: "+path);

            String extension = FilenameUtils.getExtension(path);
            StringBuilder outputText = new StringBuilder();

            // not very good... but need to read it multiple times and tika is closing the stream at the end of the parsing...
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, out);
            IOUtils.closeQuietly(out);
            byte[] bytes = out.toByteArray();

            // add path to text
            //outputText.append(path.replace('.', ' ').replace('/', ' ').replace('\\', ' ').replace('_', ' ')).append("\n");

            // parse document
            try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

                try {
                    String text = tika.parseToString(stream, metadata);
                    outputText.append(text).append("\n");
                }catch (Exception ex){
                    log.error("Unable to parse file: "+path, ex);
                }
            }

            // special case for PDF: parse images inside (ocr)
            if("pdf".equalsIgnoreCase(extension)){

                try {

                    try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

                        // load all pages of the PDF and search for images
                        try (PDDocument document = PDDocument.load(stream)) {

                            PDDocumentInformation info = document.getDocumentInformation();

                            outputText.append(info.getTitle()).append("\n");
                            outputText.append(info.getAuthor()).append("\n");
                            outputText.append(info.getSubject()).append("\n");
                            outputText.append(info.getKeywords()).append("\n");
                            outputText.append(info.getCreator()).append("\n");
                            outputText.append(info.getProducer()).append("\n");
                            outputText.append(info.getCreationDate() != null ? dateFormat.format(info.getCreationDate().getTime()) : "").append("\n");
                            outputText.append(info.getModificationDate() != null ? dateFormat.format(info.getModificationDate().getTime()) : "").append("\n");

                            if(document.isEncrypted()) {
                                try {

                                    // try to decrypt
                                    StandardDecryptionMaterial sdm = new StandardDecryptionMaterial("");
                                    document.openProtection(sdm);
                                    document.decrypt("");
                                    document.setAllSecurityToBeRemoved(true);
                                    log.info("Successfully decrypted PDF: " + path);

                                    // if decrypted, parse the text
                                    try {
                                        PDFTextStripper stripper = new PDFTextStripper();
                                        outputText.append(stripper.getText(document)).append("\n");
                                    }catch(Exception ex){
                                        log.error("Unable to parse decrypted PDF: "+path.toString(), ex);
                                    }

                                    // then scan images
                                    if(withOcr) {
                                        outputText.append(this.pdfOcrUtil.parsePdfImages(path, document)).append("\n");
                                    }

                                } catch (Exception ex) {
                                    log.warn("Unable to decrypt PDF: " + path.toString());
                                    documentErrorAccumulator.add(1);
                                }
                            }
                            else {

                                // tika should have parsed the text if decrypted PDF, now try to scan images
                                if(withOcr) {
                                    outputText.append(this.pdfOcrUtil.parsePdfImages(path, document)).append("\n");
                                }

                            }
                        }
                    }
                }
                catch (Exception ex){
                    log.error("Unable to parse PDF document", ex);
                }
            }
            else if("docx".equalsIgnoreCase(extension)){
                try {
                    try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

                        try (XWPFDocument  document = new XWPFDocument(stream)) {
                            POIXMLProperties.CoreProperties props = document.getProperties().getCoreProperties();
                            outputText.append(props.getTitle()).append("\n");
                            outputText.append(props.getDescription()).append("\n");
                            outputText.append(props.getCreator()).append("\n");
                            outputText.append(props.getKeywords()).append("\n");
                            outputText.append(props.getSubject()).append("\n");
                        }
                    }
                }
                catch (Exception ex){
                    log.error("Unable to parse DOCX document", ex);
                }
            }
            else if("doc".equalsIgnoreCase(extension)){
                try {
                    try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                        SummaryInformation si = (SummaryInformation) PropertySetFactory.create(stream);

                        outputText.append(si.getTitle()).append("\n");
                        outputText.append(si.getLastAuthor()).append("\n");
                        outputText.append(si.getAuthor()).append("\n");
                        outputText.append(si.getKeywords()).append("\n");
                        outputText.append(si.getComments()).append("\n");
                        outputText.append(si.getSubject()).append("\n");
                        /*for(Property p : si.getProperties()){
                            if(p.getValue() != null) {
                                outputText.append(p.getValue().toString()).append("\n");
                            }
                        }*/
                    }
                }
                catch (Exception ex){
                    log.error("Unable to parse DOC document", ex);
                }
            }


            String text = outputText.toString();

            documentParsedAccumulator.add(1);
            return text;
        }

        @Override
        public void close() throws IOException {
            this.pdfOcrUtil.close();
        }
    }
}
