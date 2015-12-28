package org.grozeille;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.apache.pdfbox.util.ImageIOUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.bytedeco.javacpp.BytePointer;
import org.grozeille.avro.Document;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.bytedeco.javacpp.lept.*;
import static org.bytedeco.javacpp.tesseract.TessBaseAPI;

/**
 * Created by Mathias on 22/12/2015.
 */
@Slf4j
public class DocumentAnalysis {
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
        Option tesseractPathOption  = OptionBuilder.withArgName( "tesseract-path" )
                .isRequired()
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
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        List<String> pathList = new ArrayList<>();
        pathList.add(new File(inputPath).toURI().toURL().toString());
        pathList.addAll(scanSubfolders(inputPath));

        log.info("Folders to analyse:");
        pathList.forEach(log::info);

        /*List<JavaPairRDD<String, PortableDataStream>> allRdd = new ArrayList<>();
        for(String p : pathList){
            allRdd.add(sc.binaryFiles(p));
        }
        JavaPairRDD<String, PortableDataStream> filesRdd = sc.union(allRdd.toArray(new JavaPairRDD[0]));*/

        JavaPairRDD<String, PortableDataStream> filesRdd = sc.binaryFiles(StringUtils.join(pathList, ","));


        Accumulator<Integer> documentParsedAccumulator = sc.accumulator(0);
        Accumulator<Integer> documentErrorAccumulator = sc.accumulator(0);

        JavaRDD<Document> documentRdd = filesRdd.mapValues(new DocumentTextParser(tesseractPath, tesseractLang, documentParsedAccumulator, documentErrorAccumulator)).values().flatMap(documents -> documents);


        /*documentRdd.foreach((VoidFunction<Document>) d -> {
            System.out.println("------------------------------------------------------------------------");
            System.out.println("DOC : "+d.getPath());
            System.out.println("LANG: "+d.getLang());
            //System.out.println("");
            //System.out.println(d.getBody());
            //System.out.println("------------------------------------------------------------------------");
        });*/

        DataFrame documentDF = sqlContext.createDataFrame(documentRdd, Document.class);
        documentDF.write().format("com.databricks.spark.avro").save(outputPath);

        log.info("Parsed: "+documentParsedAccumulator.value());
        log.info("Error: "+documentErrorAccumulator.value());
    }

    private static Collection<? extends String> scanSubfolders(String inputPath) {
        File parent = new File(inputPath);
        List<String> folders = new ArrayList<>();
        String[] directories = parent.list((current, name) -> new File(current, name).isDirectory());
        if(directories != null) {
            for (String d : directories) {
                try {
                    folders.add(new File(parent, d).toURI().toURL().toString());
                } catch (MalformedURLException e) {
                    log.error("Unable to add folder: "+d, e);
                }
                folders.addAll(scanSubfolders(new File(parent, d).getAbsolutePath()));
            }
        }
        return folders;
    }

    @RequiredArgsConstructor
    @Slf4j
    private static class DocumentTextParser implements Function<PortableDataStream, List<Document>>, Closeable {

        private transient Tika tika;
        private transient TessBaseAPI tessBaseAPI;
        private transient boolean tessInitialized = false;

        private final String tesseractPath;
        private final String tesseractLang;
        private final Accumulator<Integer> documentParsedAccumulator;
        private final Accumulator<Integer> documentErrorAccumulator;

        @Override
        public List<Document> call(PortableDataStream portableDataStream) throws Exception {

            if(tika == null) {
                tika = new Tika();
            }
            if(tessBaseAPI == null){
                tessBaseAPI = new TessBaseAPI();

                if (tessBaseAPI.Init(tesseractPath, tesseractLang) != 0) {
                    log.error("Could not initialize tesseract.");
                }
                else {
                    tessInitialized = true;
                }
            }

            String path = portableDataStream.getPath();
            String extension = FilenameUtils.getExtension(path);


            if("zip".equalsIgnoreCase(extension)){

                List<Document> documents = new ArrayList<>();

                try(DataInputStream stream = portableDataStream.open()) {
                    ZipInputStream zis = new ZipInputStream(stream);

                    try {
                        ZipEntry entry = null;
                        while ((entry = zis.getNextEntry()) != null) {
                            if (!entry.isDirectory()) {
                                String entryPath = path + "/" + entry.getName();
                                documents.add(parseDocument(zis, entryPath));
                            }
                        }
                    }
                    catch (Exception ex){
                        log.error("Unable to read zip file "+path, ex);
                    }

                    return documents;
                }
            }
            else {
                try(DataInputStream stream = portableDataStream.open()) {
                    return Arrays.asList(parseDocument(stream, path));
                }
            }


        }

        private Document parseDocument(InputStream inputStream, String path) throws IOException, TikaException {

            log.info("Parsing file: "+path);

            String extension = FilenameUtils.getExtension(path);
            StringBuilder outputText = new StringBuilder();

            String lang;
            // not very good... but need to read it multiple times and tika is closing the stream at the end of the parsing...
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, out);
            IOUtils.closeQuietly(out);
            byte[] bytes = out.toByteArray();

            // parse document
            try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

                String text = tika.parseToString(stream);
                outputText.append(text).append("\n");
            }

            // special case for PDF: parse images inside (ocr)
            /*if("pdf".equalsIgnoreCase(extension) && tessInitialized){

                try(ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {

                    // load all pages of the PDF and search for images
                    try (PDDocument document = PDDocument.load(stream)) {

                        // try to decrypt
                        if(document.isEncrypted()) {
                            try {
                                StandardDecryptionMaterial sdm = new StandardDecryptionMaterial("");
                                document.openProtection(sdm);
                                document.decrypt("");
                                document.setAllSecurityToBeRemoved(true);
                                log.info("Successfully decrypted PDF: " + path);

                                outputText.append(parsePdfImages(path, document)).append("\n");
                            } catch (Exception ex) {
                                log.warn("Unable to decrypt PDF: " + path.toString());
                                documentErrorAccumulator.add(1);

                                outputText.append(parsePdfEncrypted(path, document)).append("\n");
                            }
                        }
                        else {
                            outputText.append(parsePdfImages(path, document)).append("\n");
                        }
                    }
                }
            }*/


            String text = outputText.toString();
            LanguageIdentifier identifier = new LanguageIdentifier(text);
            lang = identifier.getLanguage();

            documentParsedAccumulator.add(1);
            return new Document(path, lang, ByteBuffer.wrap(text.getBytes()));
        }

        private String parsePdfEncrypted(String path, PDDocument document) throws IOException {
            List<PDPage> list = document.getDocumentCatalog().getAllPages();
            StringBuilder outputText = new StringBuilder();
            for (PDPage page : list) {

                BufferedImage pageImg = page.convertToImage();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                ImageIO.write(pageImg, "tiff", outputStream);

                outputText.append(ocr(path, outputStream.toByteArray())).append("\n");
            }

            return outputText.toString();
        }

        private String parsePdfImages(String path, PDDocument document){
            List<PDPage> list = document.getDocumentCatalog().getAllPages();
            StringBuilder outputText = new StringBuilder();
            for (PDPage page : list) {
                PDResources pdResources = page.getResources();

                Map<String, PDXObject> pageImages = pdResources.getXObjects();
                if (pageImages != null) {

                    for (Map.Entry<String, PDXObject> e : pageImages.entrySet()) {
                        if (e.getValue() instanceof PDXObjectImage) {

                            try {
                                PDXObjectImage pdxObjectImage = (PDXObjectImage) e.getValue();
                                outputText.append(ocr(path, pdxObjectImage)).append("\n");
                            } catch (Exception ex) {
                                log.error("unable to do ocr on image for file: " + path, ex);
                            }
                        }
                    }
                }
            }

            return outputText.toString();
        }

        private String ocr(String path, PDXObjectImage pdxObjectImage){

            byte[] imageByteArray = null;

            try {
                // read the image
                ByteArrayOutputStream imageOutputStream = new ByteArrayOutputStream();
                pdxObjectImage.write2OutputStream(imageOutputStream);
                imageOutputStream.close();
                imageByteArray = imageOutputStream.toByteArray();
            } catch (Exception ex){
                log.error("Unable to extract image from pdf : "+path, ex);
                return "";
            }

            try {
                // convert PNG to BMP because of an issue on Windows with Leptonica
                ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(imageByteArray));
                Iterator<ImageReader> imageReaders = ImageIO.getImageReaders(iis);
                String format = "";
                if(imageReaders.hasNext()){
                    format = imageReaders.next().getFormatName();
                }

                if("png".equalsIgnoreCase(format) || "jpg".equalsIgnoreCase(format)  || "jpeg".equalsIgnoreCase(format)  || "gif".equalsIgnoreCase(format)){
                    BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageByteArray));
                    ByteArrayOutputStream imageOutputStream = new ByteArrayOutputStream();
                    ImageIOUtil.writeImage(image, "tiff", imageOutputStream);
                    imageByteArray = imageOutputStream.toByteArray();
                }
            } catch (Exception ex){
                log.error("Unable to convert image from pdf : "+path, ex);
                return "";
            }

            return ocr(path, imageByteArray);
        }

        private String ocr(String path, byte[] imageByteArray) {

            BytePointer outText = null;

            try{
                // do OCR
                PIX image = pixReadMem(imageByteArray, imageByteArray.length);
                if(image == null){
                    log.error("Could not read image.");
                    return "";
                }
                else {
                    tessBaseAPI.SetImage(image);
                    outText = tessBaseAPI.GetUTF8Text();
                    pixDestroy(image);

                    return outText == null ? "" : outText.getString();
                }
            }finally {
                if(outText != null) {
                    outText.deallocate();
                }
            }
        }

        @Override
        public void close() throws IOException {
            tessBaseAPI.End();
        }
    }
}
