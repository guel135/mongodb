package pdf.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.bson.Document;
import org.bson.types.Binary;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Java MongoDB : Save image example
 */

public class PdfExample {

    public static void main( final String[] args )
    {
        final MongoClient mongoClient = new MongoClient( "localhost", 27017 );
        // final MongoDatabase db = mongoClient.getDatabase( "db" );
        final MongoDatabase db = mongoClient.getDatabase( "smallFiles" );

        final PdfExample pdfExample = new PdfExample();

        final String name = "file33433.pdf";
        final String name2 = "image4427jpg";
        final String name3 = "report14564.pdf";
        final String url = "C:\\Users\\u0400101\\Downloads\\mongoTest\\";

        final String filename = "C:\\Users\\u0400101\\Downloads\\TestRunReport-2016-06-30T08_53_17+02_00.pdf";
        final String filename2 = "C:\\Users\\u0400101\\Downloads\\vitaphone.jpg";
        final String filename3 = "C:\\DEV\\git-repo\\SS_RockBE\\de.vitasystems.platform.development.bootstrap\\resources\\report.pdf";

        // pdfExample.generateReports( db, 200000, "report", filename3, ".pdf" );
        pdfExample.retrieve( name3, url, db );

        mongoClient.close();

    }

    void generateReports( final MongoDatabase db, final double number, final String fileNamePrefix, final String fileUrl,
            final String fileExtension )
    {

        final FileInputStream file = null;
        try {
            final MongoCollection<Document> collection = db.getCollection( "files" );
            for( int i = 1; i < number; i++ ) {

                try {
                    final File pdfFile = new File( fileUrl );//

                    final byte b[] = new byte[ new FileInputStream( pdfFile ).available() ];
                    final Binary data = new Binary( b );

                    final Document document = new Document();
                    document.put( "name", fileNamePrefix + i + fileExtension );
                    document.put( "report", data );
                    collection.insertOne( document );
                    System.out.println( "Inserted record." + i );

                }
                catch( final IOException e ) {
                    System.out.println( "ioe " + e );
                }
            }

        }
        catch( final Exception e ) {
            System.out.println( "ioe " + e );
        }
    }

    void retrieve( final String name, final String url, final MongoDatabase db )
    {
        final byte c[];
        try
        {
            final long startingTime = System.nanoTime();

            final MongoCollection<Document> collection = db.getCollection( "files" );
            final FindIterable<Document> documents = collection.find( new Document( "name", name ) );

            final Document document = documents.first();

            final String fileName = url + (String)document.get( "name" );

            final Binary bsonBinary = (Binary)document.get( "report" );

            final byte[] b = bsonBinary.getData();

            final File file = new File( fileName );
            if( !file.exists() ) {
                file.createNewFile();
            }

            final OutputStream out = new FileOutputStream( fileName );
            out.write( b );

            out.flush();
            final long endTime = ( System.nanoTime() - startingTime ) / 1000000;
            System.out.println( "Photo of " + name + " retrieved and stored at " + fileName + "in " + endTime + "ms" );
            out.close();
        }
        catch( final IOException e ) {
            e.printStackTrace();
        }
    }
}
