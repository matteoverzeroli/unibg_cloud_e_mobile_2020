const connect_to_db = require('./db');

// ADD COMMENT HANDLER

const talk = require('./Talk');

module.exports.add_comment = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    
    let body = {}
    //parse body content
    if (event.body) {
        body = JSON.parse(event.body)
    }
    //control if id is null
    if(!body.id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not add the comment. Id is null.'
        })
    }
    //control if comment is null
    if (!body.comment) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not add the comment. Comment is null.'
        })
    }
    //control if user_id is null
    if (!body.user_id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not add the comment. user_id is null.'
        })
    }
    
    connect_to_db().then(() => {
        console.log('=> push comment');
        
        talk.find({_id: body.id})
        .then(tedtalk => {
            
            console.log("TedTalk:" + tedtalk)
            //control if any talk is found
            if (isEmpty(tedtalk)) {
                callback(null, {
                    statusCode: 500,
                    body: 'Could not find the talk.'
                })
            }
            //get current date
            let date = new Date()
            //update talk in MongoDB
            talk.update(
                {_id: body.id},
                { "$push": 
                    { "comments":
                        {
                            "user_id": body.user_id,
                            "comment": body.comment,
                            "date": date
                        }
                    }
                }
            ).then(updated => {
                callback(null, {
                    statusCode: 200,
                    body: "UPDATED!" //return answer 
                })
            })
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Error while uploading the comment.'
                })
            )
        }
        ).catch(err =>
            callback(null, {
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not fetch the talks.'
            })
        );
        
        
    });
};

//control if an object is empty
function isEmpty(obj) {

    // null and undefined are "empty"
    if (obj == null) return true;

    // Assume if it has a length property with a non-zero value
    // that that property is correct.
    if (obj.length > 0)    return false;
    if (obj.length === 0)  return true;

    // If it isn't an object at this point
    // it is empty, but it can't be anything *but* empty
    // Is it empty?  Depends on your application.
    if (typeof obj !== "object") return true;

    // Otherwise, does it have any properties of its own?
    // Note that this doesn't handle
    // toString and valueOf enumeration bugs in IE < 9
    for (var key in obj) {
        if (hasOwnProperty.call(obj, key)) return false;
    }

    return true;
}
