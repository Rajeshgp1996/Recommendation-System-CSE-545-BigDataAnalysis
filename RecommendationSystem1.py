# -*- coding: utf-8 -*-

import re
import json
import sys
import numpy as np
from scipy import stats
from pyspark.sql import SparkSession


def get_relative_word_freq(review):
    review_words = []
    all_review_words_dict = {}
    try:
        review_data = json.loads(review)
        reviewText = review_data["reviewText"]
        item_rating = review_data["overall"]
        verified = int(review_data['verified'])

        regex_pattern = r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))'
        all_review_words = re.findall(regex_pattern, reviewText.lower())

        for word in all_review_words:
            all_review_words_dict.update({word: all_review_words_dict.get(word, 0) + 1})

        for word, count in top_1000_freq_words.value:
            review_words.append(
                (word, (all_review_words_dict.get(word, 0) / len(all_review_words), item_rating, verified)))
        return review_words

    except:
        return review_words


def get_word_freq(review):
    try:
        review_data = json.loads(review)
        reviewText = review_data["reviewText"]
        regex_pattern = r'((?:[\.,!?;"])|(?:(?:\#|\@)?[A-Za-z0-9_\-]+(?:\'[a-z]{1,3})?))'
        all_review_words = re.findall(regex_pattern, reviewText.lower())

        output = []

        for word in all_review_words:
            output.append((word, 1))
        return output
    except:
        return []


def get_p_val_with_LR(word_data):
    word = word_data[0]
    X = []
    Y = []
    for rel_freq, rating, verified in word_data[1]:
        X.append(rel_freq)
        Y.append(rating)

    X = np.array(X)
    Y = np.array(Y)

    X_normalized = (X - np.mean(X, axis=0)) / np.std(X, axis=0)
    X_normalized_Trans = X_normalized.T
    Y_normalized = (Y - np.mean(Y)) / np.std(Y)

    beta = [np.dot(np.dot(1 / (np.dot(X_normalized_Trans, X_normalized)), X_normalized_Trans), Y_normalized)]

    deg_freedom = X_normalized.shape[0] - (2)
    rss = np.sum(np.power(Y_normalized - np.dot(X_normalized, beta[0]), 2))
    s_square = rss / deg_freedom
    significance = np.sqrt(s_square / np.sum(X_normalized ** 2))
    t_value = beta[0] / significance
    p_value = stats.t.sf(np.abs(t_value), deg_freedom) * 2000

    return [(word, beta, p_value)]


def get_p_val_with_LR_for_verified(word_data):
    word = word_data[0]
    X = []
    Y = []
    for rel_freq, rating, verified in word_data[1]:
        X.append([rel_freq, verified])
        Y.append(rating)

    X = np.array(X)
    Y = np.array(Y)

    X_normalized = (X - np.mean(X, axis=0)) / np.std(X, axis=0)
    XX_normalized_Trans = X_normalized.T
    Y_normalized = (Y - np.mean(Y)) / np.std(Y)

    beta = np.dot(np.dot(np.linalg.inv(np.dot(XX_normalized_Trans, X_normalized)), XX_normalized_Trans), Y_normalized)

    deg_freedom = X_normalized.shape[0] - (2)
    rss = np.sum(np.power(Y_normalized - np.dot(X_normalized, beta), 2))
    s_square = rss / deg_freedom
    significance = np.sqrt(s_square / np.sum(X_normalized ** 2))
    t_value = beta[0] / significance
    p_value = stats.t.sf(np.abs(t_value), deg_freedom) * 2000

    return [(word, beta, p_value)]


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = spark.sparkContext

    amazon_review_data = sc.textFile(sys.argv[1])

    top_1000_freq_words = amazon_review_data.flatMap(lambda review: get_word_freq(review)).reduceByKey(lambda x, y: x + y).top(1000, key=lambda x: x[1])

    top_1000_freq_words = sc.broadcast(top_1000_freq_words)

    top_1000_words_in_all_review = amazon_review_data.flatMap(lambda review: get_relative_word_freq(review)).groupByKey()

    p_value_correlated = top_1000_words_in_all_review.flatMap(lambda x: get_p_val_with_LR(x))
    p_value_correlated_with_verified = top_1000_words_in_all_review.flatMap(lambda x: get_p_val_with_LR_for_verified(x))

    p_value_correlated_top_20 = p_value_correlated.top(20, key=lambda x: x[1][0])
    p_value_correlated_with_verified_top_20 = p_value_correlated_with_verified.top(20, key=lambda x: x[1][0])

    p_value_correlated_bottom_20 = p_value_correlated.top(20, key=lambda x: -x[1][0])
    p_value_correlated_with_verified_bottom_20 = p_value_correlated_with_verified.top(20, key=lambda x: -x[1][0])

    print(" ---------------p_value_correlated_top_20------------------- ")
    print(" ")
    print(p_value_correlated_top_20)
    print(" ")
    print(" ---------------p_value_correlated_with_verified_top_20------------------- ")
    print(" ")
    print(p_value_correlated_with_verified_top_20)
    print(" ")
    print(" ---------------p_value_correlated_bottom_20------------------- ")
    print(" ")
    print(p_value_correlated_bottom_20)
    print(" ")
    print(" ---------------p_value_correlated_with_verified_bottom_20------------------- ")
    print(" ")
    print(p_value_correlated_with_verified_bottom_20)
    print(" ")
