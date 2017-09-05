package aws.example.dynamodb.mapper;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

public class DynamoDBMapperExample3 {
    static AmazonDynamoDB client;
    static final String ACCESS_KEY = "AKIAI3GH2ERUU2FWY7WQ";
    static final String SECRET_KEY = "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";

    public static void main(String[] args) {
        Regions usEast2 = Regions.US_EAST_2;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(usEast2)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();

        DynamoDBMapper mapper = new DynamoDBMapper(client);

        MusicItem keySchema = new MusicItem();
        keySchema.setArtist("No One You Know2");
        keySchema.setSongTitle("Call Me Today");
        // keySchema.setYear(2015);
        // mapper.save(keySchema);

        try {
            MusicItem result = mapper.load(keySchema);
            if (result != null) {
                System.out.println("The song was released in " + result.getYear());
            } else {
                System.out.println("No matching song was found");
            }
        } catch (Exception e) {
            System.err.println("Unable to retrieve data: ");
            System.err.println(e.getMessage());
        }
    }


    @DynamoDBTable(tableName = "Music2")
    public static class MusicItem {
        @DynamoDBHashKey(attributeName = "Artist")
        private String artist;

        @DynamoDBRangeKey(attributeName = "SongTitle")
        private String songTitle;

        @DynamoDBAttribute(attributeName = "AlbumTitle")
        private String albumTitle;

        @DynamoDBAttribute(attributeName = "Year")
        private int year;

        public String getArtist() {
            return artist;
        }

        public void setArtist(String artist) {
            this.artist = artist;
        }

        public String getSongTitle() {
            return songTitle;
        }

        public void setSongTitle(String songTitle) {
            this.songTitle = songTitle;
        }

        public String getAlbumTitle() {
            return albumTitle;
        }

        public void setAlbumTitle(String albumTitle) {
            this.albumTitle = albumTitle;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }
    }
}
