package aws.example.dynamodb.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

public class DynamoDBMapperBatchWriteExample {
    static AmazonDynamoDB client;
    static final String ACCESS_KEY = "AKIAI3GH2ERUU2FWY7WQ";
    static final String SECRET_KEY = "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";

    public static void main(String[] args) {
        Regions usEast2 = Regions.US_EAST_2;
        client = AmazonDynamoDBClientBuilder.standard().withRegion(usEast2)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY)))
                .build();

        DynamoDBMapper mapper = new DynamoDBMapper(client);
        testBatchSave(mapper);
        testBatchDelete(mapper);
        testBatchWrite(mapper);
    }

    static void testBatchSave(DynamoDBMapper mapper) {
        Book book1 = new Book();
        book1.id = 901;
        book1.inPublication = true;
        book1.ISBN = "9002-1-2-3333";
        book1.pageCount = 99;
        book1.price = 10;
        book1.productCategory = "Book";
        book1.title = "This is Book 1 Title.";

        Book book2 = new Book();
        book2.id = 902;
        book2.inPublication = true;
        book2.ISBN = "9002-1-2-4444";
        book2.pageCount = 199;
        book2.price = 20;
        book2.productCategory = "Book";
        book2.title = "This is Book 2 Title.";

        Book book3 = new Book();
        book3.id = 903;
        book3.inPublication = true;
        book3.ISBN = "9002-1-2-555";
        book3.pageCount = 555;
        book3.price = 30;
        book3.productCategory = "Book";
        book3.title = "This is Book 3 Title.";

        System.out.println("Add third book crated in batch write.");
        mapper.batchSave(Arrays.asList(book1, book2, book3));
    }

    static void testBatchDelete(DynamoDBMapper mapper) {
        Book book1 = mapper.load(Book.class, 901);
        Book book2 = mapper.load(Book.class, 902);
        System.out.println("Deleting two books from the ProductCatalog table.");
        mapper.batchDelete(Arrays.asList(book1, book2));
    }

    static void testBatchWrite(DynamoDBMapper mapper) {
        // Create Forum item to save
        Forum forumItem = new Forum();
        forumItem.name = "Test BatchWrite Forum";
        forumItem.threads = 0;
        forumItem.category = "Amazon Web Services";

        // Create Thread item to save
        Thread thredItem = new Thread();
        thredItem.forumName = "AmazonDynamoDB";
        thredItem.subject = "My Sample question";
        thredItem.msg = "BatchWrite msg";
        List<String> tags = new ArrayList<String>();
        tags.add("batch operations");
        tags.add("write");
        thredItem.tags = new HashSet<String>(tags);

        // Load ProductCatalog item to delete
        Book book3 = mapper.load(Book.class, 903);

        List<Object> objectsToWrite = Arrays.asList(forumItem, thredItem);
        List<Book> objectsToDel = Arrays.asList(book3);

        DynamoDBMapperConfig config = new DynamoDBMapperConfig(DynamoDBMapperConfig.SaveBehavior.CLOBBER);
        mapper.batchWrite(objectsToWrite, objectsToDel, config);
    }

    @DynamoDBTable(tableName = "ProductCatalog2")
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

    @DynamoDBTable(tableName = "Reply2")
    public static class Reply {
        @DynamoDBHashKey(attributeName = "Id")
        private String id;

        @DynamoDBAttribute(attributeName = "ReplyDateTime")
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

    @DynamoDBTable(tableName = "Thread2")
    public static class Thread {
        @DynamoDBHashKey(attributeName = "ForumName")
        private String forumName;

        @DynamoDBAttribute(attributeName = "Subject")
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
