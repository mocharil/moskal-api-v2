import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict
import json

class TikTokMetricsAnalyzerDF:
    def __init__(self, df: pd.DataFrame):
        """
        Initialize analyzer with DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame containing TikTok posts data
        """
        self.df = df
        self.processed_df = None
        
    def _calculate_time_based_metrics(self, row: pd.Series) -> Dict:
        """
        Calculate growth metrics based on post age
        """
        try:
            post_date = datetime.strptime(row.get('post_created_at', ''), '%Y-%m-%d')
            current_date = datetime.now()
            
            # Calculate post age in days
            post_age_days = (current_date - post_date).days
            
            likes = row.get('post_like_count', 0)
            comments = row.get('post_comment_count', 0)
            shares = row.get('post_share_count', 0)
            
            # Calculate daily growth rates
            daily_likes = likes / max(post_age_days, 1)
            daily_comments = comments / max(post_age_days, 1)
            daily_shares = shares / max(post_age_days, 1)
            
            # Calculate growth score (weighted average of daily metrics)
            growth_score = (
                (daily_likes * 0.4) +    # 40% weight
                (daily_comments * 0.3) +  # 30% weight
                (daily_shares * 0.3)      # 30% weight
            )
            
            # Calculate velocity (rate of engagement change)
            total_engagement = likes + comments + shares
            velocity = total_engagement / max(post_age_days, 1)
            
            return {
                'post_age_days': post_age_days,
                'daily_engagement_rate': growth_score,
                'engagement_velocity': velocity,
                'daily_likes': daily_likes,
                'daily_comments': daily_comments,
                'daily_shares': daily_shares
            }
        except Exception as e:
            return {
                'post_age_days': 0,
                'daily_engagement_rate': 0,
                'engagement_velocity': 0,
                'daily_likes': 0,
                'daily_comments': 0,
                'daily_shares': 0
            }

    def _calculate_engagement_metrics(self, row: pd.Series) -> Dict:
        """
        Calculate key engagement metrics with time consideration
        """
        likes = row.get('post_like_count', 0)
        comments = row.get('post_comment_count', 0)
        shares = row.get('post_share_count', 0)
        favorites = row.get('post_favorite_count', 0)
        
        time_metrics = self._calculate_time_based_metrics(row)
        
        # Calculate weighted interactions with time decay
        age_factor = max(1, np.log(time_metrics['post_age_days'] + 1))
        weighted_interactions = (
            (likes + 
            (comments * 2) +  
            (shares * 3) +    
            favorites) / age_factor
        )
        
        total_interactions = likes + comments + shares + favorites
        
        return {
            'total_interactions': total_interactions,
            'weighted_engagement_score': weighted_interactions,
            'engagement_rate': (total_interactions / (likes + 1)) * 100 if likes > 0 else 0,
            **time_metrics
        }

    def _calculate_virality_metrics(self, row: pd.Series) -> Dict:
        """
        Calculate virality metrics with time consideration
        """
        likes = row.get('post_like_count', 0)
        shares = row.get('post_share_count', 0)
        comments = row.get('post_comment_count', 0)
        
        time_metrics = self._calculate_time_based_metrics(row)
        
        # Adjust viral coefficient based on post age
        age_factor = max(1, np.log(time_metrics['post_age_days'] + 1))
        viral_coefficient = (((shares * 2) + comments) / (likes + 1)) * (1 + (1/age_factor))
        
        share_rate = (shares / (likes + 1)) * 100 if likes > 0 else 0
        
        return {
            'viral_score': viral_coefficient,
            'share_rate': share_rate
        }

    def _calculate_influence_score(self, row: pd.Series) -> Dict:
        """
        Calculate influence score with time consideration
        """
        likes = row.get('post_like_count', 0)
        comments = row.get('post_comment_count', 0)
        shares = row.get('post_share_count', 0)
        favorites = row.get('post_favorite_count', 0)
        
        time_metrics = self._calculate_time_based_metrics(row)
        
        # Adjust scores based on post age
        age_factor = max(1, np.log(time_metrics['post_age_days'] + 1))
        
        # Engagement component with time decay
        engagement_score = min(60, (
            (likes * 0.3 + 
            comments * 0.8 + 
            shares * 1.0 + 
            favorites * 0.4) / age_factor
        ) / 10)
        
        # Reach component with growth consideration
        reach_score = min(40, ((likes + shares * 2) / 20) * (1 + time_metrics['daily_engagement_rate']))
        
        total_influence = engagement_score + reach_score
        
        return {
            'influence_score': total_influence,
            'engagement_component': engagement_score,
            'reach_score': reach_score
        }

    def process_metrics(self) -> pd.DataFrame:
        """Process core metrics for the DataFrame"""
        metrics_data = []
        
        for _, row in self.df.iterrows():
            # Calculate core metrics
            engagement_metrics = self._calculate_engagement_metrics(row)
            virality_metrics = self._calculate_virality_metrics(row)
            influence_metrics = self._calculate_influence_score(row)
            
            # Combine metrics with original data
            metrics_row = {
                'post_id': row.get('link_post', '').split('/')[-1],
                'username': row.get('username', ''),
                'post_created_at': row.get('post_created_at', ''),
                'post_caption': row.get('post_caption', ''),
                'likes': row.get('post_like_count', 0),
                'comments': row.get('post_comment_count', 0),
                'shares': row.get('post_share_count', 0),
                'favorites': row.get('post_favorite_count', 0),
                'list_comment': row.get('list_comment'),
                'link_post': row.get('link_post'),
                'post_media_link': row.get('post_media_link'),
                **engagement_metrics,
                **virality_metrics,
                **influence_metrics,
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            metrics_data.append(metrics_row)
        
        self.processed_df = pd.DataFrame(metrics_data)
        return self.processed_df