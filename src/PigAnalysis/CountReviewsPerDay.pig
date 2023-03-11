initialData = load 'hdfs://localhost:9000/bigdataproject/amazon_product_review.tsv' using PigStorage('\t')
            AS (marketPlace, customerId, reviewId, productId, productParent, productTitle, productCategory, starRating,
            helpfulVotes, totalVotes, vine, verifiedPurchase, reviewHeadline, reviewBody, reviewDate);

loop = STREAM initialData THROUGH `tail -n +2`
        AS (marketPlace, customerId, reviewId, productId, productParent, productTitle, productCategory, starRating,
        helpfulVotes, totalVotes, vine, verifiedPurchase, reviewHeadline, reviewBody, reviewDate);

day = GROUP loop by reviewDate;

reviewsByDay = FOREACH day GENERATE group as reviewDate, COUNT(loop.reviewId) as count;

orderByData = ORDER reviewsByDay BY count DESC;

dump orderByData;

STORE orderByData INTO 'hdfs://localhost:9000/bigdataproject/output/pigCountReviewsPerDay ' USING PigStorage (',');
