package org.grozeille;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.cli.*;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.grozeille.avro.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by Mathias on 27/12/2015.
 */
@Slf4j
public class CopyToAvro {

    private static int cptFile = 0;

    private static String[] blacklist = new String[]{
      ".DS_Store", "Thumbs.db"
    };

    public static void main(String[] args) throws Exception {
        Option inputOption  = OptionBuilder.withArgName( "input" )
                .isRequired()
                .hasArgs()
                .withDescription( "Input path." )
                .create( "i" );
        Option outputOption  = OptionBuilder.withArgName( "output" )
                .isRequired()
                .hasArgs()
                .withDescription( "Output path for avro files." )
                .create( "o" );
        Option splitSizeOption  = OptionBuilder.withArgName( "split-size" )
                .hasArgs()
                .withDescription( "Size of avro files." )
                .create( "s" );

        Options options = new Options();
        options.addOption(inputOption);
        options.addOption(outputOption);
        options.addOption(splitSizeOption);

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
            formatter.printHelp( "CopyToAvro", options );

            System.exit(-1);
        }


        String inputPath = line.getOptionValue("i");
        String outputPath = line.getOptionValue("o");
        Long defaultSplitSize =  new Long(1024l*1024l*128l);
        String splitSizeString = line.getOptionValue("s", defaultSplitSize.toString());
        Long splitSize = defaultSplitSize;
        try {
            splitSize = Long.parseLong(splitSizeString);
        }catch (Exception ex){
            log.warn("Unable to parse default size: "+splitSizeString+". Use default value: "+defaultSplitSize);
        }

        File parentOutputFolder = new File(outputPath);
        if(parentOutputFolder.exists()){
            System.err.println("Folder "+outputPath+" already exist");
            log.error("Folder "+outputPath+" already exist");
            System.exit(-1);
        }

        parentOutputFolder.mkdirs();


        List<String> pathList = new ArrayList<>();
        pathList.add(new File(inputPath).toString());
        pathList.addAll(scanSubfolders(inputPath));
        log.info("Folders to analyse:");
        pathList.forEach(log::info);


        DataFileWriter<Document> dataFileWriter = createAvroDocumentFile(outputPath+"/result"+String.format("%03d", cptFile)+".avro");

        try {


            // list all files to write in avro
            for (String path : pathList) {

                File pathFile = new File(path);
                File[] files = pathFile.listFiles();
                if(files == null){
                    continue;
                }

                for (File child : files) {
                    if (child.isFile()) {

                        if(isBlacklisted(child)){
                            continue;
                        }

                        String extension = FilenameUtils.getExtension(child.getAbsolutePath());

                        if ("zip".equalsIgnoreCase(extension)) {

                            try (InputStream stream = new FileInputStream(child)) {
                                ZipArchiveInputStream zis = new ZipArchiveInputStream(stream);

                                scanZipArchive(dataFileWriter, child.getAbsolutePath(), zis, outputPath, splitSize);
                            }
                        } else {
                            Document document = toDocument(child);
                            dataFileWriter.append(document);

                            if(dataFileWriter.sync() >= splitSize){
                                dataFileWriter.close();
                                cptFile++;
                                dataFileWriter = createAvroDocumentFile(outputPath+"/result"+String.format("%03d", cptFile)+".avro");
                            }
                        }
                    }
                }
            }
        }finally {
            dataFileWriter.close();
        }
    }

    private static DataFileWriter<Document> scanZipArchive(DataFileWriter<Document> dataFileWriter, String parentPath, ZipArchiveInputStream zis, String outputPath, long splitSize) throws IOException {


        System.out.println("Scan zip file: "+parentPath);
        try {
            ArchiveEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {

                    String entryPath = parentPath + "/" + entry.getName();

                    if ("zip".equalsIgnoreCase(FilenameUtils.getExtension(entry.getName()))) {
                        ZipArchiveInputStream subZis = new ZipArchiveInputStream(zis);
                        dataFileWriter = scanZipArchive(dataFileWriter, entryPath, subZis, outputPath, splitSize);
                    } else {
                        Document document = toDocument(entryPath, zis);
                        dataFileWriter.append(document);
                        if (dataFileWriter.sync() >= splitSize) {
                            dataFileWriter.close();
                            cptFile++;
                            dataFileWriter = createAvroDocumentFile(outputPath + "/result" + String.format("%03d", cptFile) + ".avro");
                        }
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Unable to read zip file " + parentPath, ex);
        }

        return dataFileWriter;
    }

    private static boolean isBlacklisted(File file){
        for(String b : blacklist){
            if(b.equalsIgnoreCase(file.getName())){
                return true;
            }
        }

        return false;
    }

    private static DataFileWriter<Document> createAvroDocumentFile(String outputPath) throws IOException {
        Schema schema = ReflectData.get().getSchema(Document.class);
        File file = new File(outputPath);
        DatumWriter<Document> writer = new ReflectDatumWriter<>(Document.class);
        return new DataFileWriter<>(writer)
                .setCodec(CodecFactory.snappyCodec())
                .create(schema, file);
    }

    private static Document toDocument(String path, InputStream stream) throws IOException {
        Document doc = new Document();
        doc.setPath(path);
        doc.setLang("");
        ByteArrayOutputStream ous = new ByteArrayOutputStream();
        IOUtils.copy(stream, ous);
        doc.setBody(ByteBuffer.wrap(ous.toByteArray()));

        return doc;
    }

    private static Document toDocument(File file) throws IOException {
        try(InputStream stream = new FileInputStream(file)) {
            return toDocument(file.getAbsolutePath(), stream);
        }
    }

    private static Collection<? extends String> scanSubfolders(String inputPath) {
        File parent = new File(inputPath);
        List<String> folders = new ArrayList<>();
        String[] directories = parent.list((current, name) -> new File(current, name).isDirectory());
        if(directories != null) {
            for (String d : directories) {
                folders.add(new File(parent, d).toString());
                folders.addAll(scanSubfolders(new File(parent, d).getAbsolutePath()));
            }
        }
        return folders;
    }
}
