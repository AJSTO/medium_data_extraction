from collections import Counter
from typing import Any, Dict, List, Tuple
from datetime import datetime
import logging
import re

import pandas as pd
from google.oauth2 import service_account
from pandas_gbq import to_gbq

from medium_api import Medium

logger = logging.getLogger(__name__)


def initialize_medium(api_key: str):
    """
    Initializes a Medium instance with the provided API key.

    Parameters:
    - api_key (str): The API key for authentication with the Medium API.

    Returns:
    Medium: An instance of the Medium class.
    """
    medium_instance = Medium(api_key)
    return medium_instance


def fetch_top_writers(medium_instance: Any, topic_slug: str, count: str) -> List[Any]:
    """
    Fetches a list of top writers for a specific topic from Medium.

    Parameters:
    - medium_instance (Medium): An instance of the Medium class initialized with an API key.
    - topic_slug (str): The slug of the topic for which top writers are to be fetched.
    - count (int): The number of top writers to retrieve.

    Returns:
    List[Any]: A list of top writers' information.
    """
    top_writers = medium_instance.top_writers(topic_slug=topic_slug, count=count)
    return top_writers.users


def extract_users(top_writers_users: List[Any], google_cloud_config: Dict[str, Any], load_to_bq: bool) -> Tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Extracts user information, follower information, following information, and article information from a list of
    Medium top writers.

    Parameters:
    - top_writers_users (List[Any]): List of top writer instances from Medium.
    - google_cloud_config (Dict[str, Any]): Configuration parameters for Google Cloud, including project, dataset,
      credentials path, and table names.
    - load_to_bq (bool): If True, loads extracted data to BigQuery; otherwise, returns DataFrames without loading.

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    - users_info_df: DataFrame containing user information.
    - users_top_writer_in_df: DataFrame containing information about users' top writing interests.
    - user_followers_df: DataFrame containing information about users' followers.
    - user_followings_df: DataFrame containing information about users' followings.
    - users_articles_df: DataFrame containing information about users' articles.
    """
    users_info_df = pd.DataFrame()
    users_top_writer_in_df = pd.DataFrame()
    user_followers_df = pd.DataFrame()
    user_followings_df = pd.DataFrame()
    users_articles_df = pd.DataFrame()

    for user in top_writers_users:
        try:
            user_info = user.info
            user_top_writer_in = user_info.pop('top_writer_in', None)
            user_info_df = pd.DataFrame([user_info])

            if user_top_writer_in:
                user_top_writer_in_df = pd.DataFrame(
                    [{'user_id': user_info['id'], 'keyword': user_top_writer_in}]
                )
                users_top_writer_in_df = pd.concat([users_top_writer_in_df, user_top_writer_in_df.explode("keyword")],
                                                   ignore_index=True)

            users_info_df = pd.concat([users_info_df, user_info_df], ignore_index=True)

        except Exception as e:
            logger.error(f'Getting user info failed for user_id: {user._id}. Error: {str(e)}')

        try:
            user_followers_df = pd.concat([user_followers_df, pd.DataFrame(
                [{'user_id': user._id, 'follower_id': user.followers_ids}]
            ).explode('follower_id')], ignore_index=True)
        except Exception as e:
            logger.error(f'Getting followers users failed for user_id: {user._id}. Error: {str(e)}')

        try:
            user_followings_df = pd.concat([user_followings_df, pd.DataFrame(
                [{'user_id': user._id, 'following_id': user.following_ids}]
            ).explode('following_id')], ignore_index=True)
        except Exception as e:
            logger.error(f'Getting following users failed for user_id: {user._id}. Error: {str(e)}')

        try:
            users_articles_df = pd.concat([users_articles_df, pd.DataFrame(
                [{'user_id': user._id, 'article_id': user.article_ids}]
            ).explode('article_id')], ignore_index=True)
        except Exception as e:
            logger.error(f'Getting articles for user_id: {user._id} failed. Error: {str(e)}')

    if load_to_bq:
        project_id = google_cloud_config.get("project")
        dataset_id = google_cloud_config.get("dataset_id")
        credentials_path = google_cloud_config.get("credentials_path")
        user_info_table = google_cloud_config.get("user_info_table")
        users_top_writer_in_table = google_cloud_config.get("users_top_writer_in_table")
        user_followers_table = google_cloud_config.get("user_followers_table")
        user_followings_table = google_cloud_config.get("user_followings_table")
        users_articles_table = google_cloud_config.get("users_articles_table")

        credentials = service_account.Credentials.from_service_account_file(credentials_path)

        # LOADING USER INFO TABLE
        try:
            to_gbq(
                users_info_df,
                f"{project_id}.{dataset_id}.{user_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {user_info_table} failed. Error: {str(e)}')

        # LOADING USER TOP WRITER IN TABLE
        try:
            to_gbq(
                users_top_writer_in_df,
                f"{project_id}.{dataset_id}.{users_top_writer_in_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {users_top_writer_in_table} failed. Error: {str(e)}')

        # LOADING USER FOLLOWERS TABLE
        try:
            to_gbq(
                user_followers_df,
                f"{project_id}.{dataset_id}.{user_followers_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {user_followers_table} failed. Error: {str(e)}')

        # LOADING USER FOLLOWINGS TABLE
        try:
            to_gbq(
                user_followings_df,
                f"{project_id}.{dataset_id}.{user_followings_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {user_followings_table} failed. Error: {str(e)}')

        # LOADING USER ARTICLES TABLE
        try:
            to_gbq(
                users_articles_df,
                f"{project_id}.{dataset_id}.{users_articles_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {users_articles_table} failed. Error: {str(e)}')

    return users_info_df, users_top_writer_in_df, user_followers_df, user_followings_df, users_articles_df


def fetch_user_articles_info(medium_instance: Any, users_articles_df: pd.DataFrame, google_cloud_config: Dict[str, Any],
                             load_to_bq: bool) -> Tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fetches information about Medium articles authored by top writers, including details about tags, topics, fans,
    related articles, and comments.

    Parameters:
    - medium_instance: An instance of the Medium class.
    - users_articles_df: DataFrame containing information about users' articles.
    - google_cloud_config: Configuration parameters for Google Cloud, including project, dataset, credentials path, and
      table names.
    - load_to_bq: If True, loads extracted data to BigQuery; otherwise, returns DataFrames without loading.

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    - articles_info_df: DataFrame containing information about Medium articles.
    - article_tags_df: DataFrame containing information about tags associated with articles.
    - article_topics_df: DataFrame containing information about topics associated with articles.
    - article_fans_id_df: DataFrame containing information about fans associated with articles.
    - article_related_articles_ids_df: DataFrame containing information about related articles associated with articles.
    - articles_comments_info_df: DataFrame containing information about comments on articles.
    - article_comments_tag_info_df: DataFrame containing information about tags associated with comments on articles.
    - article_comments_topic_info_df: DataFrame containing information about topics associated with comments on articles.
    """
    articles_info_df = pd.DataFrame()
    article_tags_df = pd.DataFrame()
    article_topics_df = pd.DataFrame()
    article_fans_id_df = pd.DataFrame()
    article_related_articles_ids_df = pd.DataFrame()
    articles_comments_info_df = pd.DataFrame()
    article_comments_tag_info_df = pd.DataFrame()
    article_comments_topic_info_df = pd.DataFrame()

    # To avoid to many API Requests for testing purposes
    #n = 1
    for index, row in users_articles_df.iterrows():
        #if n > 10:
            #break

        article_object = medium_instance.article(article_id=row['article_id'])
        article_info = article_object.info
        article_info['fans_ids'] = article_object.fans_ids
        article_info['related_articles_ids'] = article_object.related_articles_ids
        article_info['is_self_published'] = article_object.is_self_published
        article_info['content'] = article_object.content
        article_info['markdown'] = article_object.markdown

        article_tags = article_info.pop('tags', None)
        article_topics = article_info.pop('topics', None)
        article_fans_id = article_info.pop('fans_ids', None)
        article_related_articles_ids = article_info.pop('related_articles_ids', None)

        if article_tags:
            article_tags_df = pd.concat([article_tags_df, pd.DataFrame(
                [{'article_id': article_info['id'], 'tag': article_tags}]
            ).explode('tag')], ignore_index=True)

        if article_topics:
            article_topics_df = pd.concat([article_topics_df, pd.DataFrame(
                [{'article_id': article_info['id'], 'topic': article_topics}]
            ).explode('topic')], ignore_index=True)

        if article_fans_id:
            article_fans_id_df = pd.concat([article_fans_id_df, pd.DataFrame(
                [{'article_id': article_info['id'], 'fan_id': article_fans_id}]
            ).explode('fan_id')], ignore_index=True)

        if article_related_articles_ids:
            article_related_articles_ids_df = pd.concat([article_related_articles_ids_df, pd.DataFrame(
                [{'article_id': article_info['id'], 'related_article_id': article_related_articles_ids}]
            ).explode('related_article_id')], ignore_index=True)

        articles_info_df = pd.concat([articles_info_df, pd.DataFrame([article_info])], ignore_index=True)

        comments = article_object.responses
        for comment in comments:
            comment_info = comment.info
            comment_info['article_id'] = row['article_id']
            comment_tag = comment_info.pop('tags', None)
            comment_topic = comment_info.pop('topics', None)

            articles_comments_info_df = pd.concat(
                [articles_comments_info_df, pd.DataFrame([comment_info])], ignore_index=True)

            if comment_tag:
                article_comments_tag_info_df = pd.concat([article_comments_tag_info_df, pd.DataFrame(
                    [{'comment_id': comment_info['id'], 'tag': comment_tag}]
                ).explode('tag')], ignore_index=True)

            if comment_topic:
                article_comments_topic_info_df = pd.concat([article_comments_topic_info_df, pd.DataFrame(
                    [{'comment_id': comment_info['id'], 'topic': comment_topic}]
                ).explode('topic')], ignore_index=True)

        #n += 1

    if load_to_bq:
        project_id = google_cloud_config.get("project")
        dataset_id = google_cloud_config.get("dataset_id")
        credentials_path = google_cloud_config.get("credentials_path")
        articles_info_table = google_cloud_config.get("articles_info_table")
        article_tags_table = google_cloud_config.get("article_tags_table")
        article_topics_table = google_cloud_config.get("article_topics_table")
        article_fans_id_table = google_cloud_config.get("article_fans_id_table")
        article_related_articles_table = google_cloud_config.get("article_related_articles_table")
        articles_comments_info_table = google_cloud_config.get("articles_comments_info_table")
        article_comments_tag_info_table = google_cloud_config.get("article_comments_tag_info_table")
        article_comments_topic_info_table = google_cloud_config.get("article_comments_topic_info_table")

        credentials = service_account.Credentials.from_service_account_file(credentials_path)

        # LOADING ARTICLES INFO TABLE
        try:
            to_gbq(
                articles_info_df,
                f"{project_id}.{dataset_id}.{articles_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {articles_info_table} failed. Error: {str(e)}')

        # LOADING ARTICLE TAGS TABLE
        try:
            to_gbq(
                article_tags_df,
                f"{project_id}.{dataset_id}.{article_tags_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_tags_table} failed. Error: {str(e)}')

        # LOADING ARTICLE TOPICS TABLE
        try:
            to_gbq(
                article_topics_df,
                f"{project_id}.{dataset_id}.{article_topics_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_topics_table} failed. Error: {str(e)}')

        # LOADING ARTICLE FANS ID TABLE
        try:
            to_gbq(
                article_fans_id_df,
                f"{project_id}.{dataset_id}.{article_fans_id_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_fans_id_table} failed. Error: {str(e)}')

        # LOADING ARTICLE RELATED ARTICLES TABLE
        try:
            to_gbq(
                article_related_articles_ids_df,
                f"{project_id}.{dataset_id}.{article_related_articles_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_related_articles_table} failed. Error: {str(e)}')

        # LOADING ARTICLES COMMENTS INFO TABLE
        try:
            to_gbq(
                articles_comments_info_df,
                f"{project_id}.{dataset_id}.{articles_comments_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {articles_comments_info_table} failed. Error: {str(e)}')

        # LOADING ARTICLE COMMENTS TAG INFO TABLE
        try:
            to_gbq(
                article_comments_tag_info_df,
                f"{project_id}.{dataset_id}.{article_comments_tag_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_comments_tag_info_table} failed. Error: {str(e)}')

        # LOADING ARTICLE COMMENTS TOPIC INFO TABLE
        try:
            to_gbq(
                article_comments_topic_info_df,
                f"{project_id}.{dataset_id}.{article_comments_topic_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {article_comments_topic_info_table} failed. Error: {str(e)}')

    return articles_info_df, article_tags_df, article_topics_df, article_fans_id_df, article_related_articles_ids_df, articles_comments_info_df, article_comments_tag_info_df, article_comments_topic_info_df


def fetch_publications_info(medium_instance: Any, topic_slug: str, google_cloud_config: Dict[str, Any],
                            load_to_bq: bool) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fetches information about Medium publications related to a given topic, including details about tags, editors
    and articles.
    Additionally, because cannot find Publication's Newsletter info in Medium API docs. I choose to shortening
    description of publication and also searching most relevant tag in publication info not in newsletter.

    Parameters:
    - medium_instance: An instance of the Medium class.
    - topic_slug: Topic slug for searching publications.
    - google_cloud_config: Configuration parameters for Google Cloud, including project, dataset, credentials path
    and table names.
    - load_to_bq: If True, loads extracted data to BigQuery; otherwise, returns DataFrames without loading.

    Returns:
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    - publication_info_df: DataFrame containing information about Medium publications.
    - publication_tags_df: DataFrame containing information about tags associated with publications.
    - publication_editors_df: DataFrame containing information about editors associated with publications.
    - publication_articles_df: DataFrame containing information about articles associated with publications.
    """
    publication_info_df = pd.DataFrame()
    publication_tags_df = pd.DataFrame()
    publication_editors_df = pd.DataFrame()
    publication_articles_df = pd.DataFrame()

    publications = medium_instance.search_publications(query=topic_slug)

    for publication in publications:

        publication_info = publication.info

        publication_tags = publication_info.pop('tags', None)
        publication_editors = publication_info.pop('editors', None)
        publication_articles = [article._id for article in publication.articles]

        # Selecting most relevant tag for publication
        words = re.findall(r'\b\w+\b', publication_info['description'].lower())
        hashtag_relevance = Counter()
        for tag in publication_tags:
            hashtag_relevance[tag] = sum(1 for word in words if word.lower() in tag.lower())
        publication_info['most_relevant_tag'] = hashtag_relevance.most_common(1)[0][0]

        publication_info_df = pd.concat(
            [publication_info_df, pd.DataFrame([publication_info])], ignore_index=True)

        if publication_tags:
            publication_tags_df = pd.concat([publication_tags_df, pd.DataFrame(
                [{'publication_id': publication_info['id'], 'tag': publication_tags}]
            ).explode('tag')], ignore_index=True)

        if publication_editors:
            publication_editors_df = pd.concat([publication_editors_df, pd.DataFrame(
                [{'publication_id': publication_info['id'], 'editor': publication_editors}]
            ).explode('editor')], ignore_index=True)

        if publication_articles:
            publication_articles_df = pd.concat([publication_articles_df, pd.DataFrame(
                [{'publication_id': publication_info['id'], 'article': publication_articles}]
            ).explode('article')], ignore_index=True)

    if load_to_bq:
        project_id = google_cloud_config.get("project")
        dataset_id = google_cloud_config.get("dataset_id")
        credentials_path = google_cloud_config.get("credentials_path")
        publication_info_table = google_cloud_config.get("publication_info_table")
        publication_tags_table = google_cloud_config.get("publication_tags_table")
        publication_editors_table = google_cloud_config.get("publication_editors_table")
        publication_articles_table = google_cloud_config.get("publication_articles_table")

        credentials = service_account.Credentials.from_service_account_file(credentials_path)

        # LOADING PUBLICATION INFO TABLE
        try:
            to_gbq(
                publication_info_df,
                f"{project_id}.{dataset_id}.{publication_info_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {publication_info_table} failed. Error: {str(e)}')

        # LOADING PUBLICATION TAGS TABLE
        try:
            to_gbq(
                publication_tags_df,
                f"{project_id}.{dataset_id}.{publication_tags_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {publication_tags_table} failed. Error: {str(e)}')

        # LOADING PUBLICATION EDITORS TABLE
        try:
            to_gbq(
                publication_editors_df,
                f"{project_id}.{dataset_id}.{publication_editors_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {publication_editors_table} failed. Error: {str(e)}')

        # LOADING PUBLICATION ARTICLES TABLE
        try:
            to_gbq(
                publication_articles_df,
                f"{project_id}.{dataset_id}.{publication_articles_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {publication_articles_table} failed. Error: {str(e)}')

    return publication_info_df, publication_tags_df, publication_editors_df, publication_articles_df


def fetch_top_feeds(medium_instance: Any, topic_slug: str, feed_mode: str, count: str, google_cloud_config: Dict[str, Any],
                    load_to_bq: bool) -> pd.DataFrame:
    """
    Fetches information about Medium top feeds for a given topic and captures the articles in each feed.

    Parameters:
    - medium_instance: An instance of the Medium class.
    - topic_slug: Topic slug for fetching top feeds.
    - feed_mode: List of feed modes.
    - count: Number of articles to retrieve per feed.
    - google_cloud_config: Configuration parameters for Google Cloud, including project, dataset, credentials path, and table names.
    - load_to_bq: If True, loads extracted data to BigQuery; otherwise, returns DataFrame without loading.

    Returns:
    pd.DataFrame: DataFrame containing information about Medium top feeds, captured date, and associated article IDs.
    """
    today_date = datetime.now().date()
    top_feeds_df = pd.DataFrame()

    for feed in feed_mode:
        articles = medium_instance.topfeeds(tag=topic_slug, count=count, mode=feed)
        top_feed_df = pd.DataFrame(
            [
                {
                    'feed_mode': feed,
                    'captured_on_date': today_date.strftime('%Y-%m-%d'),
                    'article_id': articles.ids
                }
            ]
        )

        top_feeds_df = pd.concat([top_feeds_df, top_feed_df.explode("article_id")], ignore_index=True)

    if load_to_bq:
        project_id = google_cloud_config.get("project")
        dataset_id = google_cloud_config.get("dataset_id")
        credentials_path = google_cloud_config.get("credentials_path")
        top_feeds_table = google_cloud_config.get("top_feeds_table")

        credentials = service_account.Credentials.from_service_account_file(credentials_path)

        # LOADING TOP FEEDS TABLE
        try:
            to_gbq(
                top_feeds_df,
                f"{project_id}.{dataset_id}.{top_feeds_table}",
                if_exists="append",
                credentials=credentials,
            )
        except Exception as e:
            logger.error(f'Uploading records to table: {top_feeds_table} failed. Error: {str(e)}')

    return top_feeds_df
