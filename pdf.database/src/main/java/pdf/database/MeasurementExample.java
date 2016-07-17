package pdf.database;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

public class MeasurementExample {

    /*
     * { "sensorId": "1234", "sensorModel": "LOOP", "sensorManufacturer": "vitasystems",
     * "sensorSerial": "345abc", gatewayId": "2345", "gatewayModel": "LISA", "gatewayManufacturer":
     * "vitaphone", "gatewaySerial": "9876543", "receiverType": "LisaReceiver", "patientPseudonym":
     * "pat01", "recordingTimestamp": "20150125T090553Z", "sensorTransmissionTimestamp":
     * "20150126T100553Z", "gatewayTransmissionTimestamp": "20150127T110553Z",
     * "measurementType":"ecg", "data":
     * "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"
     * }
     */
    // database is loaded with

    final static MongoClient mongoClient = new MongoClient( "localhost", 27017 );
    final static MongoDatabase db = mongoClient.getDatabase( "measurementDB" );

    public static void main( final String args[] ) {
        final long timeInit = System.currentTimeMillis();
        // generateDocuments( 1000000000 );
        findDocument( "sensorManufacturer", "manufacturer4834444" );

        final long finishTime = System.currentTimeMillis() - timeInit;
        System.out.println( "execution time was " + finishTime + " ms" );

    }

    public static Document createDocument( final int counter ) {

        final Document document = new Document();
        document.append( "sensorId", "sensorId" );
        document.append( "sensorModel", "LOOP" + counter );
        document.append( "sensorManufacturer", "manufacturer" + counter );
        document.append( "sensorSerial", "345abc" + counter );
        document.append( "gatewayId", "2345" );
        document.append( "gatewayModel", "LISA" );
        document.append( "gatewayManufacturer", "gateManufacturer" );
        document.append( "gatewaySerial", "9876543" );
        document.append( "receiverType", "LisaReceiver" );
        document.append( "patientPseudonym", "pat01" );
        document.append( "recordingTimestamp", "20150125T090553Z" );
        document.append( "sensorTransmissionTimestamp", "20150126T100553Z" );
        document.append( "gatewayTransmissionTimestamp", "20150127T110553Z" );
        document.append( "measurementType", "ecg" );
        document.append(
                "data",
                "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" );

        return document;

    }

    private static void findDocument( final String fieldName, final String filterName ) {
        try {

            final long timeInit = System.currentTimeMillis();
            final FindIterable<Document> iterable = db.getCollection( "measurements" ).find(
                    new Document( fieldName, filterName ) );
            final long finishTime = System.currentTimeMillis() - timeInit;
            System.out.println( "find in " + finishTime + " ms" );
            iterable.forEach( new Block<Document>() {
                @Override
                public void apply( final Document document ) {
                    System.out.println( document );
                }
            } );
            final long printTime = System.currentTimeMillis() - timeInit;
            System.out.println( "print in " + printTime + " ms" );
        }
        catch( final Exception e ) {
            System.out.println( e );
        }
    }

    private static void insertDocument( final Document document ) {
        db.getCollection( "measurements" ).insertOne( document );

    }

    private static void generateDocuments( final int numberDocuments ) {

        for( int i = 0; i < numberDocuments; i++ ) {
            insertDocument( createDocument( i ) );
            // System.out.println( "inserted " + i + " measurements" );
        }

    }
}
