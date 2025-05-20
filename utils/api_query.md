berikut adalah contoh query ke Elasticsearch
{
  "size": 10,
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "post_created_at": {
              "gte": "2025-01-01",
              "lte": "2025-02-01"
            }
          }
        },
        {
          "bool": {
            "must": [
              {
                "bool": {
                  "should": [
                    {
                      "match": {
                        "post_caption": {
                          "query": "prabowo",
                          "operator": "AND"
                        }
                      }
                    }
                  ],
                  "minimum_should_match": 1
                }
              }
            ]
          }
        }
      ],
      "filter": [
        {
          "terms": {
            "sentiment": [
              "positive",
              "negative",
              "neutral"
            ]
          }
        },
        {
          "bool": {
            "should": [
              {
                "wildcard": {
                  "region": "*bandung*"
                }
              }
            ],
            "minimum_should_match": 1
          }
        },
        {
          "bool": {
            "should": [
              {
                "wildcard": {
                  "language": "*indonesia*"
                }
              }
            ],
            "minimum_should_match": 1
          }
        },
        {
          "bool": {
            "should": [
              {
                "wildcard": {
                  "link_post": "*kumparan.com*"
                }
              },
              {
                "wildcard": {
                  "link_post": "*detik.com*"
                }
              }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  },
  "from": 0,
  "_source": ['username', 'post_created_at', 'post_caption', 'link_post',
  'engagement_rate', 'viral_score', 'influence_score', 'reach_score', 'updated_at', 
  'channel', 'title', 'description', 'creator', 'sentiment',
  'emotions', 'issue', 'language', 'region', 'intent', 'user_image_url','votes','comments',
  'likes','shares','reports','favorites','retweets','replies']
}