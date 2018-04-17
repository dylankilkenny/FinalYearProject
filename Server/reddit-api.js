module.exports = {
    Social: function(db, callback){
        let query = 
        db.collection('social').find().project({
            "_id":0,
            "total_volume":0,
            "volume_by_day":0,
            "sentiment_by_day":0,
            "total_sentiment":0
        }).toArray(function(err, result) {
            callback(result)
        }) 
    }
}
