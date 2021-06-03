const connect_to_db = require('./db');

// GET BY TALK HANDLER

const watch = require('./Watch');

module.exports.get_by_tag = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if(!body.id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the watch next. Id is null.'
        })
    }
    
    connect_to_db().then(() => {
        console.log('=> get_all watchnext');
        watch.find({_id: body.id},{_id: 1, watch_next_obj: 1})
            .then(talks => {
                    callback(null, {
                        statusCode: 200,
                        body: JSON.stringify(talks)
                    })
                }
            )
            .catch(err =>
                callback(null, {
                    statusCode: err.statusCode || 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the id.'
                })
            );
    });
};
