initialData = load 'hdfs://localhost:9000/bigdataproject/amazon_product_review.tsv' using PigStorage('\t')
            AS (marketPlace, customerId, reviewId, productId, productParent, productTitle, productCategory, starRating,
            helpfulVotes, totalVotes, vine, verifiedPurchase, reviewHeadline, reviewBody, reviewDate);

groupByProductID = GROUP initialData By productId;

countReviewsPerProduct = FOREACH groupByProductID {
     Generate group as productId, COUNT(initialData.reviewId) as totalCount;
};
orderByReviewCount = ORDER countReviewsPerProduct BY totalCount DESC;

bestNItems = LIMIT orderByReviewCount 5;

dump bestNItems;

STORE bestNItems INTO 'hdfs://localhost:9000/bigdataproject/output/pigBestNProductItems ' USING PigStorage (',');
