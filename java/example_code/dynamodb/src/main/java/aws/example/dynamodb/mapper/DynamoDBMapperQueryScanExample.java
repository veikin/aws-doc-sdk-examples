package aws.example.dynamodb.mapper;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

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
    }

    static void getBook(DynamoDBMapper mapper, int id) {
        System.out.println("GetBook: Get Book Id = " + id);
        System.out.println("Book table has no sort key. You can do GetItem, but not Query.");
        Book book = mapper.load(Book.class, id);
        System.out.format("Id = %s Title = %s, ISBN = %s %n", book.getId(), book.getTitle(), book.getISBN());
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

    @DynamoDBTable(tableName = "ProductCatalog3")
    public static class Bicycle {
        @DynamoDBHashKey(attributeName = "Id")
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
