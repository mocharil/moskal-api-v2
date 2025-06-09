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
                          "operator": "OR"
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
  'channel', 'title', 'sentiment',
  'emotions',  'region', 'intent', 'user_image_url','votes','comments',
  'likes','shares','reports','favorites','retweets','replies','cluster','cluster_description']
}


Keterangan field
- cluster adalah kelompok issue dari post -> aggregate mengguakan .keyword
- cluster_description adalah deskripsi dari kelompok tersebut tentang apa -> aggregate menggunakan .keyword
- channel adalah nama platform
- username pada platform news adalah source dari berita tersebut


MAPPING 
{
      "properties": {
        "channel": {
          "type": "keyword"
        },
        "cluster": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "cluster_confidence": {
          "type": "float"
        },
        "cluster_description": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "cluster_keywords": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "comments": {
          "type": "integer"
        },
        "emotions": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "engagement_rate": {
          "type": "float"
        },
        "favorites": {
          "type": "long"
        },
        "influence_score": {
          "type": "float"
        },
        "intent": {
          "type": "keyword"
        },
        "issue": {
          "type": "text",
          "fields": {
            "case_insensitive": {
              "type": "text",
              "analyzer": "case_insensitive_analyzer"
            },
            "keyword": {
              "type": "keyword"
            }
          }
        },
        "language": {
          "type": "keyword"
        },
        "like_count": {
          "type": "long"
        },
        "likes": {
          "type": "long"
        },
        "link_post": {
          "type": "keyword"
        },
        "link_user": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "list_comment": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "list_word": {
          "type": "keyword"
        },
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "object": {
          "type": "keyword"
        },
        "pinned": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "post_caption": {
          "type": "text",
          "fields": {
            "case_insensitive": {
              "type": "text",
              "analyzer": "case_insensitive_analyzer"
            },
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "post_created_at": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss.SSSSSSXXX||yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        },
        "post_hashtags": {
          "type": "keyword"
        },
        "post_id": {
          "type": "keyword"
        },
        "post_media_link": {
          "type": "keyword"
        },
        "post_mentions": {
          "type": "keyword"
        },
        "post_music": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "post_type": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "processed_at": {
          "type": "date"
        },
        "processing_batch": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "profile_link": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "reach_score": {
          "type": "float"
        },
        "region": {
          "type": "keyword"
        },
        "sentiment": {
          "type": "keyword"
        },
        "shares": {
          "type": "long"
        },
        "thumbnail_url": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "topic": {
          "type": "keyword"
        },
        "updated_at": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss.SSSSSSXXX||yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
        },
        "user_bio": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_category": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_channel": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_engagement_quality": {
          "type": "float"
        },
        "user_engagement_rate": {
          "type": "float"
        },
        "user_followers": {
          "type": "long"
        },
        "user_followings": {
          "type": "long"
        },
        "user_image_url": {
          "type": "keyword"
        },
        "user_influence_score": {
          "type": "float"
        },
        "user_last_data_updated": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_likes": {
          "type": "long"
        },
        "user_link_attached": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_link_user": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_reason": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "user_viral_potential": {
          "type": "float"
        },
        "username": {
          "type": "keyword"
        },
        "uuid": {
          "type": "keyword"
        },
        "views": {
          "type": "long"
        },
        "viral_score": {
          "type": "float"
        },
        "votes": {
          "type": "integer"
        },
        "worker": {
          "type": "long"
        }
      }
    }