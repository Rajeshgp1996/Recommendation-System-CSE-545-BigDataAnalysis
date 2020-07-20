# Recommendation-System-CSE-545-BigDataAnalysis
Task Requirements: Your objective is to perform item-item collaborative filtering over the provided products and ratings. Specifically, to prepare the system, you will first need to do some filtering:

Filter to only one rating per user per item by taking their most recent rating (or their last within the data file; as long as you have one rating per person it is fine)
From there, filter to items associated with at least 25 distinct users
From there, filter to users associated with at least 5 distinct items
Option: If you have a particular RDD that has less than an order of 1k entries (i.e. a list of reviewerIDs or asins), at that point, it's ok to collect them into a sc.Broadcast variable.


Then, you are ready to apply item-item collaborative filtering to predict missing values for the rows prescribed in the output. Use the following settings for your collaborative filtering:

Use 50 neighbors (or all possible neighbors if < 50 have values) with the weighted average approach (weighted by similarity) described in class and the book.
Do not include neighbors:
with negative or zero similarity or
those having less than 2 columns (i.e. users) with ratings for whom the target row (i.e. the intersection of users_with_ratings for the two is < 2; can check for this before checking similarity).
Within a target row, do not make predictions for columns (i.e. users) that do not have at least 2 neighbors with values
Only need to focus on the specified items (in practice, you wouldn't store a completed utility matrix, rather this represents querying the recommendation system, given an item, for users that might be interested in the item).

Remember to treat items as "rows" and users as "columns" where the goal is to rate one item based on its similarity to other items. Running from start (reading/filtering data) to finish (printing results should take less than 8 minutes on a cluster with >= 8 vCPUs with 8GB per vCPU and multiple disks for reading the data from HDFS (In reality, such a system could assume that the data was already filtered as that wouldn't need to happen per run but it is fine to happen per run here).


