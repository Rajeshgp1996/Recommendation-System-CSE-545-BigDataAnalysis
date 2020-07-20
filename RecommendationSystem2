# -*- coding: utf-8 -*-

import json
import sys
import numpy as np
from pyspark.sql import SparkSession


def get_user_review_pair(review):
    try:
        json_review = json.loads(review)
        reviewerID = json_review['reviewerID']
        itemID = json_review['asin']
        rating = json_review['overall']
        rev_id_item_id = reviewerID + itemID
        date = json_review['unixReviewTime']
        return [(rev_id_item_id, [(date, reviewerID, itemID, rating)])]
    except:
        return [('0', (0, 'reviewerID', 'itemID', 'rating'))]


def get_latest_user_review(date_review_list):
    date = date_review_list[0][0]
    reviewerID = date_review_list[0][1]
    rating = date_review_list[0][3]
    for date_rev in date_review_list:
        curr_date = date_rev[0]
        if curr_date > date:
            date = curr_date
            reviewerID = date_rev[1]
            rating = date_rev[3]
    return [(date_review_list[0][2], [(reviewerID, rating)])]


def get_user_with_5_items(tuple_data):
    usr_list = set()
    for users in tuple_data[1]:
        usr_list.add((users[0], 1))
    return usr_list


def get_ratings_for_filtered_users(item_list):  # item_list = (item, [(user_id, rating)])
    rating_dict = {}
    avg = 0.0
    length = 0.0
    sq_sum = 0.0
    item_rating_dict = dict(item_list[1])
    for user in users_with_5_items.value:
        val = item_rating_dict.get(user)
        if val is not None:
            avg += val
            length += 1
            rating_dict.update({user: val})
    if length != 0:
        avg = avg / length

    rating_avg_dict = {}

    for key, value in rating_dict.items():
        sq_sum += np.square(value - avg)
        rating_avg_dict.update({key: (value - avg, value)})

    return [(item_list[0], rating_avg_dict, np.sqrt(sq_sum))]


def get_neighbor(item_list):  # item_list = (item, dict{user_id : (avg_rating, rating)}, sq_sum)
    output = []
    for given_item in given_item_rating.value:
        if item_list[0] == given_item[0]:
            continue

        count = 0
        mul_val = 0
        given_item_rating_list = given_item[1]
        rating_dict = {}

        for key, value in item_list[1].items():
            rating_dict.update({key: value[1]})
            item_value = given_item_rating_list.get(key)
            if item_value is not None:
                count += 1
                mul_val += item_value[0] * value[0]

        if mul_val == 0:
            cosine_dist = 0
        else:
            cosine_dist = mul_val / (item_list[2] * given_item[2])

        output.append((given_item[0], [(rating_dict, cosine_dist, count, cosine_dist > 0)]))
    return output


def get_utility_mat(item):  # item = (target_item,[(rating_dict={user : rating}, cosine_dist, count, cosine_dist>0)])
    output = []
    for vals in item[1]:
        for user, rating in vals[0].items():
            output.append((user + "," + item[0], [(vals[1], rating)]))
    return output


def get_prediction(item):  # item = ('user, target_item', list = [(cosine_dist,rating)])
    user, target_item = item[0].split(',')

    req_user_dict = {}
    for given_item in given_item_rating.value:
        if target_item == given_item[0]:
            req_user_dict.update(given_item[1])
            break

    if len(item[1]) > 1:
        if req_user_dict.get(user) is None:  # For each list ele of [('target_item', cosine_dist,rating)]
            rating = 0.0
            numer = 0.0
            denom = 0.0
            for vals in item[1]:
                numer += vals[0] * vals[1]  # sum of cosine_dist * rating
                denom += vals[0]  # sum of cosine_dist

            rating = numer / denom
            output = (target_item, [(user, rating)])
            return [output]
        else:
            return [(target_item, [(user, 0.0)])]
    else:
        return [(target_item, [(user, 0.0)])]


if __name__ == '__main__':

    path_to_file = sys.argv[1]

    item_list = eval(sys.argv[2])

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = spark.sparkContext

    review_data = sc.textFile(path_to_file)

    latest_reviews = review_data.flatMap(lambda line: get_user_review_pair(line)) \
        .reduceByKey(lambda a, b: a + b).flatMap(lambda line: get_latest_user_review(line[1]))

    item_with_25_users = latest_reviews.reduceByKey(lambda a, b: a + b).filter(lambda x: len(x[1]) > 24)

    users_with_5_items = item_with_25_users.flatMap(lambda line: get_user_with_5_items(line)) \
        .reduceByKey(lambda a, b: a + b).filter(lambda x: x[1] > 4).flatMap(lambda x: [x[0]])

    users_with_5_items = sc.broadcast(users_with_5_items.collect())

    given_item = sc.broadcast(item_list)

    item_and_users_mat = item_with_25_users.flatMap(lambda item: get_ratings_for_filtered_users(item))

    given_item_rating = sc.broadcast(item_and_users_mat.filter(lambda x: x[0] in given_item.value).collect())

    neighbor_items_list = item_and_users_mat.flatMap(lambda item_list: get_neighbor(item_list)) \
        .filter(lambda x: x[1][0][2] > 1 and x[1][0][3]).reduceByKey(lambda a, b: a + b).top(50, key=lambda x: len(x[1]))

    neighbor_items = sc.parallelize(neighbor_items_list)

    utility_mat = neighbor_items.flatMap(lambda item: get_utility_mat(item)).reduceByKey(lambda a, b: a + b)

    pred_values = utility_mat.flatMap(lambda item: get_prediction(item)).filter(lambda x: x[1][0][1] > 0) \
        .reduceByKey(lambda a, b: a + b)

    print(pred_values.collect())

