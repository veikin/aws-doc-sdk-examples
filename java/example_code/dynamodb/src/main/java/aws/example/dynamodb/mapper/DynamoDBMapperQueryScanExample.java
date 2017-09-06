package aws.example.dynamodb.mapper;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;


// http://docs.amazonaws.cn/amazondynamodb/latest/developerguide/DynamoDBMapper.QueryScanExample.html
public class DynamoDBMapperQueryScanExample {
    static AmazonDynamoDB client;
    static final String ACCESS_KEY = "AKIAI3GH2ERUU2FWY7WQ";
    static final String SECRET_KEY = "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";

    public static void main(String[] args) {
        Regions usEast2 = Regions.US_EAST_2;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(usEast2)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();

        DynamoDBMapper mapper = new DynamoDBMapper(client);
        batchWrite(mapper);

        // Get a book - Id=101
        // getBook(mapper, 101);

        // Sample forum and thread to test queries.
        // String forumName = "Amazon DynamoDB";
        // String threadSubject = "DynamoDB Thread 1";

        // Sample queries.
        // FindRepiesInLast15Days(mapper, forumName, threadSubject);
        // FindRepliesPostedWithinTimePeriod(mapper, forumName, threadSubject);

        // Scan a table and find book items priced less than specified value.
        // FindBooksPricedLessThanSpecifiedValue(mapper, "20");

        // Scan a table with multiple threads and find bicycle items with a
        // specified bicycle type

        FindBicyclesOfSpecificTypeWithMultipleThreads(mapper, 16, "Road");

    }

    static void batchWrite(DynamoDBMapper mapper) {
        Book book1 = new Book();
        book1.id = 101;
        book1.inPublication = true;
        book1.ISBN = "111-1-1-111";
        book1.pageCount = 100;
        book1.price = 10;
        book1.productCategory = "Book";
        book1.title = "This is Book 1 Title.";

        Book book2 = new Book();
        book2.id = 102;
        book2.inPublication = true;
        book2.ISBN = "2222-2-2-2222";
        book2.pageCount = 200;
        book2.price = 20;
        book2.productCategory = "Book";
        book2.title = "This is Book 2 Title.";

        mapper.batchSave(Arrays.asList(book1, book2));

        Reply reply = new Reply();
        reply.setId("rp_2");
        reply.setReplyDateTime(new Date().toString());
        reply.setMsg("msg 2");
        reply.setPostedBy("test post...");

        mapper.save(reply);

        // Bicycle bicycle = new Bicycle();
        // bicycle.bicycleType = "HH";
        //
        // // mapper.save(bicycle);
        //
        // mapper.batchSave(Arrays.asList(bicycle, new Bicycle(), new Bicycle()));
    }

    static void getBook(DynamoDBMapper mapper, int id) {
        System.out.println("GetBook: Get Book Id = " + id);
        System.out.println("Book table has no sort key. You can do GetItem, but not Query.");
        Book book = mapper.load(Book.class, id);
        System.out.format("Id = %s Title = %s, ISBN = %s %n", book.getId(), book.getTitle(), book.getISBN());
    }

    static void FindRepiesInLast15Days(DynamoDBMapper mapper, String forumName, String threadSubject) {
        System.out.println("FindRepliesInLast15Days: Replies within last 15 days.");
        String partitionKey = forumName + "#" + threadSubject;
        long twoWeeksAgoMili = (new Date()).getTime() - (15L * 24L *60L* 60L*1000L);
        Date twoWeeksAgo = new Date();
        twoWeeksAgo.setTime(twoWeeksAgoMili);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String twoWeeksAgoStr = dateFormat.format(twoWeeksAgo);

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":val1", new AttributeValue().withS(partitionKey));
        eav.put(":val2", new AttributeValue().withS(twoWeeksAgoStr.toString()));

        DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
                .withKeyConditionExpression("Id=:val1 and ReplyDateTime > :val2").withExpressionAttributeValues(eav);

        List<Reply> latestReplies = mapper.query(Reply.class, queryExpression);
        for (Reply reply : latestReplies) {
            System.out.format("Id=%s, Message=%s, PostedBy=%s %n, ReplyDateTime=%s %n", reply.getId(),
                    reply.getMsg(), reply.getPostedBy(), reply.getReplyDateTime());
        }

    }

    static void FindRepliesPostedWithinTimePeriod(DynamoDBMapper mapper,String forumName,String threadSubject){
        String partitionKey = forumName + "#" + threadSubject;
        System.out.println(
                "FindRepliesPostedWithinTimePeriod: Find replies for thread Message = 'DynamoDB Thread 2' posted within a period.");
        long startDateMilli = (new Date()).getTime() - (14L * 24L * 60L * 60L * 1000L); // two weeks
                                                                                        // ago.
        long endDateMilli = (new Date()).getTime() - (7L * 24L * 60L * 60L * 1000L); // one week
                                                                                     // ago.
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String startDate = dateFormat.format(startDateMilli);
        String endDate = dateFormat.format(endDateMilli);

        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS(partitionKey));
        eav.put(":v2", new AttributeValue().withS(startDate));
        eav.put(":v3", new AttributeValue().withS(endDate));

        DynamoDBQueryExpression<Reply> queryExpression = new DynamoDBQueryExpression<Reply>()
                .withKeyConditionExpression("Id = :v1 and ReplyDateTime between :v2 and :v3")
                .withExpressionAttributeValues(eav);

        List<Reply> betweenReplies = mapper.query(Reply.class, queryExpression);
        for (Reply reply : betweenReplies) {
            System.out.format("Id=%s, Message=%s, PostedBy=%s %n, PostedDateTime=%s %n", reply.getId(),
                    reply.getMsg(), reply.getPostedBy(), reply.getReplyDateTime());
        }
    }

    static void FindBooksPricedLessThanSpecifiedValue(DynamoDBMapper mapper, String value) {
        System.out.println("FindBooksPricedLessThanSpecifiedValue: Scan ProductCatalog.");
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withN(value));
        eav.put(":v2", new AttributeValue().withS("Book"));

        DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
                .withFilterExpression("Price < :v1 and ProductCategory = :v2").withExpressionAttributeValues(eav);
        List<Book> books = mapper.scan(Book.class, scanExpression);

        for (Book book : books) {
            System.out.println(book);
        }
    }

    static void FindBicyclesOfSpecificTypeWithMultipleThreads(DynamoDBMapper mapper, int numberOfThreads,
            String bicycleType) {
        System.out.println("FindBicyclesOfSpecificTypeWithMultipleThreads: Scan ProductCatalog With Multiple Threads.");
        Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
        eav.put(":v1", new AttributeValue().withS("Bicycle"));
        eav.put(":v2", new AttributeValue().withS(bicycleType));

        DynamoDBScanExpression scanExpression =
                new DynamoDBScanExpression().withFilterExpression("ProductCategory = :v1 and BicycleType  = :v2")
                        .withExpressionAttributeValues(eav);

        List<Bicycle> bicycles = mapper.parallelScan(Bicycle.class, scanExpression, numberOfThreads);
        for (Bicycle bicycle : bicycles) {
            System.out.println(bicycle);
        }
    }

    @DynamoDBTable(tableName = "ProductCatalog3")
    public static class Book {
        @DynamoDBHashKey(attributeName = "Id")
        private int id;

        @DynamoDBAttribute(attributeName = "Title")
        private String title;

        @DynamoDBAttribute(attributeName = "ISBN")
        private String ISBN;

        @DynamoDBAttribute(attributeName = "Price")
        private int price;

        @DynamoDBAttribute(attributeName = "PageCount")
        private int pageCount;

        @DynamoDBAttribute(attributeName = "ProductCategory")
        private String productCategory;

        @DynamoDBAttribute(attributeName = "InPublication")
        private boolean inPublication;

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

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        public int getPageCount() {
            return pageCount;
        }

        public void setPageCount(int pageCount) {
            this.pageCount = pageCount;
        }

        public String getProductCategory() {
            return productCategory;
        }

        public void setProductCategory(String productCategory) {
            this.productCategory = productCategory;
        }

        public boolean isInPublication() {
            return inPublication;
        }

        public void setInPublication(boolean inPublication) {
            this.inPublication = inPublication;
        }

        @Override
        public String toString() {
            return "Book [ISBN=" + ISBN + ", price=" + price + ", product category=" + productCategory + ", id=" + id
                    + ", title=" + title + "]";
        }
    }

    @DynamoDBTable(tableName = "Bicycle")
    public static class Bicycle {
        @DynamoDBHashKey(attributeName = "Id")
        // @DynamoDBAutoGeneratedKey 自动生成的是UUID
        private int id;

        @DynamoDBAttribute(attributeName = "Title")
        private String title;

        @DynamoDBAttribute(attributeName = "Desc")
        private String desc;

        @DynamoDBAttribute(attributeName = "BicycleType")
        private String bicycleType;

        @DynamoDBAttribute(attributeName = "Brand")
        private String brand;

        @DynamoDBAttribute(attributeName = "Price")
        private int price;

        @DynamoDBAttribute(attributeName = "Color")
        private List<String> color;

        @DynamoDBAttribute(attributeName = "ProductCategory")
        private String productCategory;

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

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        public String getBicycleType() {
            return bicycleType;
        }

        public void setBicycleType(String bicycleType) {
            this.bicycleType = bicycleType;
        }

        public String getBrand() {
            return brand;
        }

        public void setBrand(String brand) {
            this.brand = brand;
        }

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        public List<String> getColor() {
            return color;
        }

        public void setColor(List<String> color) {
            this.color = color;
        }

        public String getProductCategory() {
            return productCategory;
        }

        public void setProductCategory(String productCategory) {
            this.productCategory = productCategory;
        }

        @Override
        public String toString() {
            return "Bicycle [Type=" + bicycleType + ", color=" + color + ", price=" + price + ", product category="
                    + productCategory + ", id=" + id + ", title=" + title + "]";
        }
    }

    @DynamoDBTable(tableName = "Reply3")
    public static class Reply {
        @DynamoDBHashKey(attributeName = "Id")
        private String id;

        // Range key
        @DynamoDBRangeKey(attributeName = "ReplyDateTime")
        private String replyDateTime;

        @DynamoDBAttribute(attributeName = "Msg")
        private String msg;

        @DynamoDBAttribute(attributeName = "PostedBy")
        private String postedBy;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getReplyDateTime() {
            return replyDateTime;
        }

        public void setReplyDateTime(String replyDateTime) {
            this.replyDateTime = replyDateTime;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public String getPostedBy() {
            return postedBy;
        }

        public void setPostedBy(String postedBy) {
            this.postedBy = postedBy;
        }
    }

    @DynamoDBTable(tableName = "Thread3")
    public static class Thread {
        @DynamoDBHashKey(attributeName = "ForumName")
        private String forumName;

        // Range key
        @DynamoDBRangeKey(attributeName = "Subject")
        private String subject;

        @DynamoDBAttribute(attributeName = "Msg")
        private String msg;

        @DynamoDBAttribute(attributeName = "LastPostedDateTime")
        private String lastPostedDateTime;

        @DynamoDBAttribute(attributeName = "LastPostedBy")
        private String lastPostedBy;

        @DynamoDBAttribute(attributeName = "Tags")
        private Set<String> tags;

        @DynamoDBAttribute(attributeName = "Answered")
        private int answered;

        @DynamoDBAttribute(attributeName = "Views")
        private int views;

        @DynamoDBAttribute(attributeName = "Replies")
        private int replies;

        public String getForumName() {
            return forumName;
        }

        public void setForumName(String forumName) {
            this.forumName = forumName;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        public String getLastPostedDateTime() {
            return lastPostedDateTime;
        }

        public void setLastPostedDateTime(String lastPostedDateTime) {
            this.lastPostedDateTime = lastPostedDateTime;
        }

        public String getLastPostedBy() {
            return lastPostedBy;
        }

        public void setLastPostedBy(String lastPostedBy) {
            this.lastPostedBy = lastPostedBy;
        }

        public Set<String> getTags() {
            return tags;
        }

        public void setTags(Set<String> tags) {
            this.tags = tags;
        }

        public int getAnswered() {
            return answered;
        }

        public void setAnswered(int answered) {
            this.answered = answered;
        }

        public int getViews() {
            return views;
        }

        public void setViews(int views) {
            this.views = views;
        }

        public int getReplies() {
            return replies;
        }

        public void setReplies(int replies) {
            this.replies = replies;
        }
    }

    @DynamoDBTable(tableName = "Forum2")
    public static class Forum {
        @DynamoDBHashKey(attributeName = "Name")
        private String name;

        @DynamoDBAttribute(attributeName = "Category")
        private String category;

        @DynamoDBAttribute(attributeName = "Threads")
        private int threads;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }
    }
}
