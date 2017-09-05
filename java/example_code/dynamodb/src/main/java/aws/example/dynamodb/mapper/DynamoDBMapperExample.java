package aws.example.dynamodb.mapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverted;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;
import com.amazonaws.util.StringUtils;

public class DynamoDBMapperExample {
    static AmazonDynamoDB client;
    static final String ACCESS_KEY = "AKIAI3GH2ERUU2FWY7WQ";
    static final String SECRET_KEY = "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";

    public static void main(String[] args) {
        // Region region = Region.getRegion(Regions.US_EAST_2);
        // BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(ACCESS_KEY,
        // SECRET_KEY);
        // client = new AmazonDynamoDBClient(basicAWSCredentials);
        // region.isServiceSupported(ServiceAbbreviations.Dynamodb);
        // client.setRegion(region);

        Regions usEast2 = Regions.US_EAST_2;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(usEast2)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();

        DimensionType dimType = new DimensionType();
        dimType.setHeight("100");
        dimType.setLen("99");
        dimType.setTickness("2");

        Book book = new Book();
        book.setId(50020);
        book.setTitle("aqin test");
        book.setISBN("999-99999999");
        book.setBookAuthors(new HashSet<String>(Arrays.asList("aqin1", "aqin2")));
        book.setDimensionType(dimType);

        DynamoDBMapper mapper = new DynamoDBMapper(client);
        mapper.save(book);

        Book bookRetrieved = mapper.load(Book.class, 50020);
        System.out.println("Book info: " + bookRetrieved);

        bookRetrieved.getDimensionType().setHeight("200");
        bookRetrieved.getDimensionType().setLen("1111");
        mapper.save(bookRetrieved);

        System.out.println("Update book info: " + mapper.load(Book.class, 50020));
    }

    @DynamoDBTable(tableName = "ProductCatalog")
    public static class Book {
        @DynamoDBHashKey(attributeName = "Id")
        private int id;

        @DynamoDBAttribute(attributeName = "Title")
        private String title;

        @DynamoDBAttribute(attributeName = "ISBN")
        private String ISBN;

        @DynamoDBAttribute(attributeName = "Authors")
        private Set<String> bookAuthors;

        @DynamoDBAttribute(attributeName = "Dimensions")
        @DynamoDBTypeConverted(converter = DimensionTypeConverter.class)
        private DimensionType dimensionType;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getISBN() {
            return ISBN;
        }

        public void setISBN(String iSBN) {
            ISBN = iSBN;
        }

        public Set<String> getBookAuthors() {
            return bookAuthors;
        }

        public void setBookAuthors(Set<String> bookAuthors) {
            this.bookAuthors = bookAuthors;
        }

        public DimensionType getDimensionType() {
            return dimensionType;
        }

        public void setDimensionType(DimensionType dimensionType) {
            this.dimensionType = dimensionType;
        }

        @Override
        public String toString() {
            return "Book [ISBN=" + ISBN + ", bookAuthors=" + bookAuthors + ", dimensionType= "
                    + dimensionType.getHeight() + " x " + dimensionType.getLen() + " x " + dimensionType.getTickness()
                    + ", Id=" + id + ", Title=" + title + "]";
        }
    }

    public static class DimensionType {
        private String len;
        private String height;
        private String tickness;

        public String getLen() {
            return len;
        }

        public void setLen(String len) {
            this.len = len;
        }

        public String getHeight() {
            return height;
        }

        public void setHeight(String height) {
            this.height = height;
        }

        public String getTickness() {
            return tickness;
        }

        public void setTickness(String tickness) {
            this.tickness = tickness;
        }
    }

    public static class DimensionTypeConverter implements DynamoDBTypeConverter<String, DimensionType> {

        @Override
        public String convert(DimensionType arg0) {
            DimensionType itemDimensions = (DimensionType) arg0;
            if (null == itemDimensions) {
                return null;
            }

            return String.format("%s x %s x %s", itemDimensions.getLen(), itemDimensions.getHeight(),
                    itemDimensions.getTickness());
        }

        @Override
        public DimensionType unconvert(String arg0) {
            DimensionType itemDimension = new DimensionType();
            if (StringUtils.isNullOrEmpty(arg0)) {
                return itemDimension;
            }

            String[] data = arg0.split("x");
            itemDimension.setLen(data[0].trim());
            itemDimension.setHeight(data[1].trim());
            itemDimension.setTickness(data[2].trim());
            return itemDimension;
        }

    }
}
