module.exports = {
    
    Document: function(){

    },
    NoPostsAndComments: function(db, subreddit, callback){
        let query = {"id":subreddit}
        db.collection('subreddits').find(query).toArray(function(err, result) {
            let obj = {
                comments: result[0].no_comments,
                posts: result[0].no_posts
            }
            callback(obj)
        }) 
    }
}
