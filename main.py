from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import (
    keyword_trends, context_of_discussion, mentions, stats, 
    mentions_by_categories, sentiment_breakdown, presence_score, share_of_voice,
    sentiment_by_categories, popular_emojis, trending_links, topics_to_watch, kol_to_watch,
    analysis_overview, analysis_overview, trending_hashtags, most_followers, summary
)

app = FastAPI(
    title="Dashboard API",
    description="API for Dashboard Menu and Keyword Trends",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
#### DASHBOARD ####

app.include_router(topics_to_watch.router)
app.include_router(kol_to_watch.router)
app.include_router(keyword_trends.router)
app.include_router(context_of_discussion.router)
app.include_router(mentions.router)

#### Topics ####
#diambil dari topics to watch
############## BELUM##############

##### Summary ####
app.include_router(summary.router)
app.include_router(stats.router)


#### ANALYSIS ####
app.include_router(analysis_overview.router)
app.include_router(mentions_by_categories.router)
app.include_router(sentiment_by_categories.router)
app.include_router(share_of_voice.router)
app.include_router(most_followers.router)
app.include_router(presence_score.router)
app.include_router(sentiment_breakdown.router)
app.include_router(trending_hashtags.router)
app.include_router(trending_links.router)
app.include_router(popular_emojis.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1000)
