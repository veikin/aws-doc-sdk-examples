package aws.example.dynamodb.mapper;

import java.util.ArrayList;
import java.util.List;

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

public class DynamoDBMapperExample4 {
    static AmazonDynamoDB client;
    static final String ACCESS_KEY = "AKIAI3GH2ERUU2FWY7WQ";
    static final String SECRET_KEY = "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";

    public static void main(String[] args) {
        Regions usEast2 = Regions.US_EAST_2;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(usEast2)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();

        DimensionType dimType1 = new DimensionType();
        dimType1.setHeight("100");
        dimType1.setLen("99");
        dimType1.setTickness("2");

        DimensionType dimType2 = new DimensionType();
        dimType2.setHeight("100");
        dimType2.setLen("99");
        dimType2.setTickness("2");

        List<DimensionType> dimensionTypes = new ArrayList<DimensionType>();
        dimensionTypes.add(dimType1);
        dimensionTypes.add(dimType2);

        Book book = new Book();
        book.setId(50020);
        book.setDimensionTypes(dimensionTypes);

        DynamoDBMapper mapper = new DynamoDBMapper(client);
        mapper.save(book);

        System.out.println("Update book info: " + mapper.load(Book.class, 50020));
    }

    @DynamoDBTable(tableName = "ProductCatalog4")
    public static class Book {
        @DynamoDBHashKey(attributeName = "Id")
        private int id;

        @DynamoDBAttribute(attributeName = "Dimensions")
        @DynamoDBTypeConverted(converter = DimensionTypeConverter.class)
        private List<DimensionType> dimensionTypes;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }


        public List<DimensionType> getDimensionTypes() {
            return dimensionTypes;
        }

        public void setDimensionTypes(List<DimensionType> dimensionTypes) {
            this.dimensionTypes = dimensionTypes;
        }

        @Override
        public String toString() {
            String typeStr = "";
            for (DimensionType dimensionType : dimensionTypes) {
                String subType = "{" + dimensionType.getHeight() + " x " + dimensionType.getLen() + " x "
                        + dimensionType.getTickness() + "}";
                typeStr += subType;
            }
            return "Book [dimensionType= " + typeStr + ", Id=" + id + "]";
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

    public static class DimensionTypeConverter implements DynamoDBTypeConverter<String, List<DimensionType>> {

        @Override
        public String convert(List<DimensionType> types) {
            String res = "";
            for (DimensionType type : types) {
                String typeStr = String.format("%s x %s x %s #", type.getLen(), type.getHeight(), type.getTickness());
                res += typeStr;
            }
            return res.substring(0, res.length() - 2);
        }

        @Override
        public List<DimensionType> unconvert(String reqStr) {
            List<DimensionType> types = new ArrayList<DimensionType>();
            if (StringUtils.isNullOrEmpty(reqStr)) {
                return types;
            }

            String[] typesStr = reqStr.split("#");
            for (String typeStr : typesStr) {
                String[] data = typeStr.split("x");
                DimensionType type = new DimensionType();
                type.setLen(data[0].trim());
                type.setHeight(data[1].trim());
                type.setTickness(data[2].trim());

                types.add(type);
            }
            return types;
        }
    }
}
