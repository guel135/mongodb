package pdf.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSDownloadByNameOptions;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

public class FilesWithGridFS {

    public static void main( final String[] args ) throws IOException {

        final MongoClient mongoClient = new MongoClient( "localhost", 27017 );
        final MongoDatabase db = mongoClient.getDatabase( "files" );

        final String name = "filename4671.pdf";
        final String name2 = "filename17887.jpg";
        final String url = "C:\\Users\\u0400101\\Downloads\\mongoTest\\";

        final String filename = "C:\\Users\\u0400101\\Downloads\\TestRunReport-2016-06-30T08_53_17+02_00.pdf";
        final String filename2 = "C:\\Users\\u0400101\\Downloads\\vitaphone.jpg";

        // generateData( db, filename, ".pdf", 1000000 );
        retrieveData( name, url, db );

        mongoClient.close();
    }

    private static void generateData( final MongoDatabase db, final String filename, final String extension, final double number )
    {

        final long startingTime = System.nanoTime();
        for( int i = 0; i < number; i++ ) {

            try {

                // Create a gridFSBucket with a custom bucket name "files"
                final GridFSBucket gridFSBucket = GridFSBuckets.create( db, "fileCollection" );

                // Get the input stream
                final InputStream streamToUploadFrom = new FileInputStream( new File( filename ) );

                // Create some custom options
                final GridFSUploadOptions options = new GridFSUploadOptions()
                        .chunkSizeBytes( 1024 )
                        .metadata( new Document( "type", "file/" + extension ).append( "typemeasurement", "notype" ) );

                final ObjectId fileId = gridFSBucket.uploadFromStream( "filename" + i + extension, streamToUploadFrom, options );
                System.out.println( "insert number " + i );
            }
            catch( final MongoException e ) {
                e.printStackTrace();
            }
            catch( final IOException e ) {
                e.printStackTrace();
            }
        }
        final long endTime = ( System.nanoTime() - startingTime ) / 1000000;
        System.out.println( "generated " + number + " files in " + endTime + "ms" );
    }

    private static void listFiles( final MongoDatabase db ) {

        final GridFSBucket gridFSBucket = GridFSBuckets.create( db, "images" );

        gridFSBucket.find().forEach(
                new Block<GridFSFile>() {
                    @Override
                    public void apply( final GridFSFile gridFSFile ) {
                        System.out.println( gridFSFile.getFilename() );
                    }
                } );
    }

    private static void retrieveData( final String name, final String url, final MongoDatabase db ) throws IOException {

        final long startingTime = System.nanoTime();

        final GridFSBucket gridFSBucket = GridFSBuckets.create( db, "fileCollection" );

        final String filename = url + name;
        final FileOutputStream streamToDownloadTo = new FileOutputStream( filename );
        final GridFSDownloadByNameOptions downloadOptions = new GridFSDownloadByNameOptions();

        gridFSBucket.downloadToStreamByName( name, streamToDownloadTo, downloadOptions );
        streamToDownloadTo.close();

        final long endTime = ( System.nanoTime() - startingTime ) / 1000000;
        System.out.println( "File" + name + " retrieved and stored at " + name + " in " + endTime + "ms" );
    }

    private static void readFile( final String name, final String url, final MongoDatabase db ) throws IOException {

        final long startingTime = System.nanoTime();

        final GridFSBucket gridFSBucket = GridFSBuckets.create( db, "fileCollection" );

        gridFSBucket.find( com.mongodb.client.model.Filters.eq( "metadata.contentType", "image/png" ) ).forEach(
                new Block<GridFSFile>() {
                    @Override
                    public void apply( final GridFSFile gridFSFile ) {
                        System.out.println( gridFSFile.getFilename() );
                    }
                } );

        final long endTime = ( System.nanoTime() - startingTime ) / 1000000;
        System.out.println( "File" + name + " retrieved and stored at " + name + " in " + endTime + "ms" );
    }
}
