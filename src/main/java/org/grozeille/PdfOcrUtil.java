package org.grozeille;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.xobject.PDXObjectImage;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.tesseract;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Slf4j
public class PdfOcrUtil implements Serializable {

    private transient tesseract.TessBaseAPI tessBaseAPI;

    @Getter
    private transient boolean tessInitialized = false;
    private transient boolean tessInitializedWithError = false;

    private final String tesseractPath;
    private final String tesseractLang;

    public PdfOcrUtil(String tesseractPath, String tesseractLang){
        this.tesseractPath = tesseractPath;
        this.tesseractLang = tesseractLang;
    }

    public void init(){
        tessBaseAPI = new tesseract.TessBaseAPI();

        if (tessBaseAPI.Init(tesseractPath, tesseractLang) != 0) {
            tessInitialized = true;
            tessInitializedWithError = true;
            log.error("Could not initialize tesseract.");
        }
        else {
            tessInitialized = true;
            tessInitializedWithError = false;
        }
    }

    public String parsePdfOcr(String path, PDDocument document) throws IOException {
        if(tessInitializedWithError){
            return "";
        }

        List<PDPage> list = document.getDocumentCatalog().getAllPages();
        StringBuilder outputText = new StringBuilder();
        int pageCpt = 0;
        for (PDPage page : list) {

            log.info("parsing page "+pageCpt+" from file "+path);
            BufferedImage pageImg = page.convertToImage();

            try {
                outputText.append(ocr(pageImg)).append("\n");
            } catch (Exception ex) {
                log.error("unable to do ocr on page "+pageCpt+" for file: " + path, ex);
            }
            pageCpt++;
        }

        return outputText.toString();
    }

    public String parsePdfImages(String path, PDDocument document){
        if(tessInitializedWithError){
            return "";
        }

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
                            log.info("parsing image "+e.getKey()+" from file "+path);
                            outputText.append(ocr(pdxObjectImage)).append("\n");
                        } catch (Exception ex) {
                            log.error("unable to do ocr on image "+e.getKey()+" for file: " + path, ex);
                        }
                    }
                }
            }
        }

        return outputText.toString();
    }

    private String ocr(PDXObjectImage pdxObjectImage) throws IOException {
        if(tessInitializedWithError){
            return "";
        }

        // read the image
        ByteArrayOutputStream imageOutputStream = new ByteArrayOutputStream();
        pdxObjectImage.write2OutputStream(imageOutputStream);
        imageOutputStream.close();
        byte[] imageByteArray = imageOutputStream.toByteArray();
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageByteArray));

        return ocr(image);
    }

    public String ocr(BufferedImage image) throws IOException {
        if(tessInitializedWithError){
            return "";
        }

        // convert to tiff
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(image, "tiff", outputStream);
        outputStream.close();
        byte[] imageByteArray = outputStream.toByteArray();


        BytePointer outText = null;
        int bpp = image.getColorModel().getPixelSize();
        int bytespp = bpp / 8;
        int bytespl = (int) Math.ceil(image.getWidth() * bpp / 8.0);

        try{
            // do OCR
            tessBaseAPI.SetImage(imageByteArray, image.getWidth(), image.getHeight(), bytespp, bytespl);
            outText = tessBaseAPI.GetUTF8Text();

            return outText == null ? "" : outText.getString();
        }finally {
            if(outText != null) {
                outText.deallocate();
            }
        }
    }

    public void close() {
        if(tessInitializedWithError){
            return;
        }

        tessBaseAPI.End();
    }
}
