script_score = {
    "lang": "painless",
    "params": {
        "whitelist": [
            "kompas.com", "detik.com", "cnnindonesia.com", "cnbcindonesia.com", "suara.com", "tribunnews.com",
            "liputan6.com", "katadata.co.id", "apnews.com", "dawn.com", "republika.co.id", "viva.co.id",
            "idntimes.com", "mediaindonesia.com", "okezone.com", "tvonenews.com", "jpnn.com", "antaranews.com","viva.co.id"
        ],
        "max_val": 500.0
    },
    "source": """
        double logNorm(def val, double max) {
            return Math.log(1 + val) / Math.log(1 + max);
        }

        String channel = doc.containsKey('channel') && !doc['channel'].empty ? doc['channel'].value : "";
        double likes = doc.containsKey('likes') && !doc['likes'].empty ? doc['likes'].value : 0;
        double comments = doc.containsKey('comments') && !doc['comments'].empty ? doc['comments'].value : 0;
        double replies = doc.containsKey('replies') && !doc['replies'].empty ? doc['replies'].value : 0;
        double retweets = doc.containsKey('retweets') && !doc['retweets'].empty ? doc['retweets'].value : 0;
        double reposts = doc.containsKey('reposts') && !doc['reposts'].empty ? doc['reposts'].value : 0;
        double shares = doc.containsKey('shares') && !doc['shares'].empty ? doc['shares'].value : 0;
        double favorites = doc.containsKey('favorites') && !doc['favorites'].empty ? doc['favorites'].value : 0;
        double votes = doc.containsKey('votes') && !doc['votes'].empty ? doc['votes'].value : 0;
        double views = doc.containsKey('views') && !doc['views'].empty ? doc['views'].value : 0;
        double score = 0;

        if (channel == 'twitter') {
            double E = logNorm(likes, params.max_val) * 0.4 + logNorm(replies, params.max_val) * 0.3 + logNorm(retweets, params.max_val) * 0.3;
            double R = logNorm(views, params.max_val);
            score = (0.7 * E + 0.3 * R) * 10;
        } else if (channel == 'linkedin') {
            double E = logNorm(likes, params.max_val) * 0.5 + logNorm(comments, params.max_val) * 0.3;
            double R = logNorm(reposts, params.max_val) * 0.2;
            score = (0.6 * E + 0.4 * R) * 10;
        } else if (channel == 'tiktok') {
            double E = logNorm(likes, params.max_val) * 0.4 + logNorm(comments, params.max_val) * 0.3 + logNorm(favorites, params.max_val) * 0.1;
            double R = logNorm(shares, params.max_val) * 0.2;
            score = (0.7 * E + 0.3 * R) * 10;
        } else if (channel == 'instagram') {
            if (views > 0) {
            double E = logNorm(likes, params.max_val) * 0.5 + logNorm(comments, params.max_val) * 0.3;
            double R = logNorm(views, params.max_val) * 0.2;
            score = (0.6 * E + 0.4 * R) * 10;
            } else {
            double E = logNorm(likes, params.max_val) * 0.6 + logNorm(comments, params.max_val) * 0.4;
            score = 0.6 * E * 10;
            }
        } else if (channel == 'reddit') {
            double E = logNorm(votes, params.max_val) * 0.6;
            double R = logNorm(comments, params.max_val) * 0.4;
            score = (0.6 * E + 0.4 * R) * 10;
        } else if (channel == 'youtube') {
            double E = logNorm(likes, params.max_val) * 0.4 + logNorm(comments, params.max_val) * 0.2;
            double R = logNorm(views, params.max_val) * 0.4;
            score = (0.6 * E + 0.4 * R) * 10;
        } else if (channel == 'news') {
            String username = doc.containsKey('username.keyword') && !doc['username.keyword'].empty ? doc['username.keyword'].value : "";
            double A = params.whitelist.contains(username) ? 1.0 : 0.0;
            double M = (doc.containsKey('post_media_link') && !doc['post_media_link'].empty && doc['post_media_link'].value.contains("http")) ? 1.0 : 0.0;
            double Q = doc.containsKey('list_quotes.keyword') && !doc['list_quotes.keyword'].empty && doc['list_quotes.keyword'].value.contains("quotes") ? 1.0 : 0.0;
            score = (0.8 * A + 0.1 * M + 0.1 * Q) * 10;
        }

        return Math.min(score, 10.0);
    """
}