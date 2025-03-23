from elasticsearch import Elasticsearch, helpers
import pandas as pd
import re

DEST_ES = {
    "cloud_id": "My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ0YzJmOTUzZDY0MjQ0YTI0YThjOTQzN2NkYjYxZjQ5ZiQ1YTU2MGU0OWZmNjI0MzA2OTJjZjZiZGUwMjc5YjMyNg==",  # Jika menggunakan Cloud ID,
    "username": "elastic",
    "password": "phtYPZyE8Y9NpCxBHGfIBIXS",
}


class TikTokUserAnalytics:
    def __init__(self, es_host="http://localhost:9200", index_name="tiktok_user_profile"):
        self.es =  Elasticsearch(
                        cloud_id = DEST_ES['cloud_id'],
                        http_auth = (DEST_ES["username"], DEST_ES["password"])
                    )

        self.index_name = index_name

    def fetch_aggregated_data(self):
        """Fetch and analyze user profile data from Elasticsearch"""
        query = {
            "size": 0,
            "aggs": {
                "group_by_username": {
                    "terms": {
                        "field": "username.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "first_entry": {
                            "top_hits": {
                                "sort": [{"created_at.keyword": {"order": "asc"}}],
                                "size": 1
                            }
                        },
                        "last_entry": {
                            "top_hits": {
                                "sort": [{"created_at.keyword": {"order": "desc"}}],
                                "size": 1
                            }
                        }
                    }
                }
            }
        }

        response = self.es.search(index=self.index_name, body=query)
        data = []

        for bucket in response["aggregations"]["group_by_username"]["buckets"]:
            first = bucket["first_entry"]["hits"]["hits"][0]["_source"]
            last = bucket["last_entry"]["hits"]["hits"][0]["_source"]

            # Calculate metrics
            metrics = self._calculate_metrics(first, last)
            data.append(metrics)

        return pd.DataFrame(data)

    def _calculate_metrics(self, first, last):
        """Calculate comprehensive user metrics"""
        # Time-based calculations
        first_date = pd.to_datetime(first["created_at"])
        last_date = pd.to_datetime(last["created_at"])
        growth_period = max((last_date - first_date).total_seconds() / (60 * 60 * 24), 1)  # Convert to days

        # Basic growth calculations
        followers_growth = last["followers"] - first["followers"]
        likes_growth = last["likes"] - first["likes"]

        # Calculate advanced metrics
        metrics = {
            # Basic Information
            "username": first["username"],
            "name": last["name"],
            "bio": last["bio"],
            "link_attached": last["link_attached"],
            "last_data_updated": last["created_at"],

            # Follower Metrics
            "followers_first": first["followers"],
            "followers_last": last["followers"],
            "follower_growth": followers_growth,
            "follower_growth_rate": self._calculate_growth_rate(first["followers"], last["followers"]),
            "daily_follower_growth": followers_growth / growth_period,
            "follower_acceleration": self._calculate_acceleration(followers_growth, growth_period),

            # Likes Metrics
            "likes_first": first["likes"],
            "likes_last": last["likes"],
            "likes_growth": likes_growth,
            "likes_growth_rate": self._calculate_growth_rate(first["likes"], last["likes"]),
            "daily_likes_growth": likes_growth / growth_period,
            "likes_acceleration": self._calculate_acceleration(likes_growth, growth_period),

            # Engagement Metrics
            "engagement_rate": self._calculate_engagement_rate(last["likes"], last["followers"]),
            "engagement_quality": self._calculate_engagement_quality(last),
            "follower_to_following_ratio": self._calculate_follower_ratio(last),
            
            # Growth Metrics
            "growth_period_days": growth_period,
            "growth_consistency": self._calculate_growth_consistency(followers_growth, likes_growth, growth_period),
            "growth_velocity": self._calculate_growth_velocity(followers_growth, likes_growth, growth_period),

            # Profile Quality Metrics
            "profile_completeness": self._calculate_profile_completeness(last),
            "content_activity_score": self._calculate_activity_score(first, last, growth_period),
            
            # Influence Metrics
            "influence_score": self._calculate_influence_score(last),
            "viral_potential": self._calculate_viral_potential(last, followers_growth, likes_growth, growth_period)
        }

        return metrics

    def _calculate_growth_rate(self, first_value, last_value):
        """Calculate percentage growth rate"""
        if first_value > 0:
            return ((last_value - first_value) / first_value) * 100
        return 0

    def _calculate_acceleration(self, growth, period):
        """Calculate growth acceleration"""
        return growth / (period * period) if period > 0 else 0

    def _calculate_engagement_rate(self, likes, followers):
        """Calculate engagement rate with consideration for account size"""
        if followers == 0:
            return 0
            
        base_rate = (likes / followers) * 100
        
        # Apply size-based adjustment
        if followers < 1000:
            return base_rate * 1.2  # Small accounts tend to have higher engagement
        elif followers < 10000:
            return base_rate * 1.1
        elif followers < 100000:
            return base_rate
        else:
            return base_rate * 0.9  # Large accounts tend to have lower engagement

    def _calculate_engagement_quality(self, last_data):
        """Calculate engagement quality score"""
        followers = last_data["followers"]
        likes = last_data["likes"]
        
        if followers == 0:
            return 0
            
        likes_per_follower = likes / followers
        
        # Quality score based on likes per follower
        quality_score = min(1.0, likes_per_follower / 100)  # Normalize to 0-1
        
        return quality_score

    def _calculate_follower_ratio(self, last_data):
        """Calculate follower to following ratio"""
        following = last_data.get("following", 0)
        if following == 0:
            return 0
        return last_data["followers"] / following

    def _calculate_growth_consistency(self, followers_growth, likes_growth, period):
        """Calculate growth consistency score"""
        if period < 2:  # Need at least 2 days for consistency
            return 0
            
        daily_follower_growth = followers_growth / period
        daily_likes_growth = likes_growth / period
        
        # Higher score for steady, sustainable growth
        consistency_score = min(1.0, (daily_follower_growth * 0.6 + daily_likes_growth * 0.4) / 1000)
        
        return consistency_score

    def _calculate_growth_velocity(self, followers_growth, likes_growth, period):
        """Calculate overall growth velocity"""
        if period == 0:
            return 0
            
        # Weighted combination of follower and likes growth
        velocity = (followers_growth * 0.7 + likes_growth * 0.3) / period
        return velocity

    def _calculate_profile_completeness(self, profile_data):
        """Calculate profile completeness score"""
        fields = ['name', 'bio', 'link_attached']
        completed_fields = sum(1 for field in fields if profile_data.get(field))
        return completed_fields / len(fields)

    def _calculate_activity_score(self, first, last, period):
        """Calculate user activity score"""
        if period == 0:
            return 0
            
        # Consider both content creation and engagement
        likes_activity = (last["likes"] - first["likes"]) / period
        follower_activity = (last["followers"] - first["followers"]) / period
        
        # Normalize and combine
        activity_score = (likes_activity * 0.6 + follower_activity * 0.4) / 1000
        return min(1.0, activity_score)

    def _calculate_influence_score(self, profile_data):
        """Calculate user influence score"""
        followers = profile_data["followers"]
        likes = profile_data["likes"]
        
        # Base influence on followers and engagement
        follower_score = min(1.0, followers / 100000)  # Normalize followers
        engagement_score = min(1.0, likes / (followers * 100)) if followers > 0 else 0
        
        # Combine scores
        influence_score = (follower_score * 0.7 + engagement_score * 0.3)
        return influence_score

    def _calculate_viral_potential(self, profile_data, followers_growth, likes_growth, period):
        """Calculate viral potential score"""
        if period == 0:
            return 0
            
        # Growth rates
        follower_velocity = followers_growth / period
        likes_velocity = likes_growth / period
        
        # Engagement factor
        engagement_factor = self._calculate_engagement_rate(profile_data["likes"], profile_data["followers"])
        
        # Combined viral potential score
        viral_score = (
            (follower_velocity * 0.4) +
            (likes_velocity * 0.4) +
            (engagement_factor * 0.2)
        ) / 1000  # Normalize
        
        return min(1.0, viral_score)