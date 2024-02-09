from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    fetch_top_writers,
    extract_users,
    initialize_medium,
    fetch_user_articles_info,
    fetch_publications_info,
    fetch_top_feeds
)


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                initialize_medium,
                inputs="params:api_key",
                outputs="medium_instance",
                name="initialize_medium_node",
            ),
            node(
                fetch_top_writers,
                inputs=["medium_instance", "params:topic_slug", "params:count"],
                outputs="top_writers_users",
                name="fetch_top_writers_node",
            ),
            node(
                extract_users,
                inputs=["top_writers_users", "params:google_cloud_config", "params:load_to_bq"],
                outputs=[
                    "users_info_df", "user_top_writer_in_df", "user_followers_df",
                    "user_followings_df", "users_articles_df"
                ],
                name="extract_users_node",
            ),
            node(
                fetch_user_articles_info,
                inputs=["medium_instance", "users_articles_df", "params:google_cloud_config", "params:load_to_bq"],
                outputs=[
                    "articles_info_df", "article_tags_df", "article_topics_df", "article_fans_id_df",
                    "article_related_df", "articles_comments_info_df", "article_comments_tag_info_df",
                    "article_comments_topic_info_df"
                ],
                name="extract_articles_info",
            ),
            node(
                fetch_publications_info,
                inputs=["medium_instance", "params:topic_slug", "params:google_cloud_config", "params:load_to_bq"],
                outputs=[
                    "publication_info_df", "publication_tags_df", "publication_editors_df",
                     "publication_articles_df"
                ],
                name="extract_publications_info",
            ),
            node(
                fetch_top_feeds,
                inputs=[
                    "medium_instance", "params:topic_slug", "params:feed_mode", "params:count",
                    "params:google_cloud_config", "params:load_to_bq"
                ],
                outputs="top_feeds_df",
                name="extract_top_feeds",
            ),
        ]
    )