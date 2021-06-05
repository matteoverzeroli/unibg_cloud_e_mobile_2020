// GET BY QUESTION HANDLER

//initialize Amazon Translate
var AWS = require('aws-sdk')
AWS.config.update({region: 'us-east-1'})
var translate = new AWS.Translate();

const connect_to_db = require('./db');

const questions = require('./Question');

module.exports.get_question_id = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    
    console.log('Received event:', JSON.stringify(event, null, 2));
    
    let body = {}
    // parse body content
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // control id
    if(!body.id) {
        callback(null, {
                    statusCode: 500,
                    headers: { 'Content-Type': 'text/plain' },
                    body: 'Could not fetch the watch next. Id is null.'
        })
    }
    // set english as default target language if not specified
    if (!body.language) {
        body.language = 'en';
    }
    
    if (!body.level) {
        body.level = "1"; //set default requested level to 1 if not specified
    } else {
        if (body.level > 5 || body.level < 1) { //control level filed
            callback(null, {
                statusCode: 500,
                body: 'The level must be an integer between 1-5'
            })
        }
    }

    console.log("Connect to db...")
    
    connect_to_db().then(() => {
        console.log('=> get questions');
        //get question from MongoDB with the specified id and level
        questions.aggregate([
            { "$match" : {"_id" : body.id } },
            { "$project" : {_id: 1, questions_obj: {
                "$filter" : {
                    "input" : "$questions_obj",
                    "as" : "quest",
                    "cond" : { $eq: [ "$$quest.level", body.level]}
                }
            }
            }
        }
        ])
        .then(async (quest) => {
                console.log("We found this questions:" + quest);
                console.log("Typeof:" + typeof(quest));
                
                if (body.language != 'en') {
                    for (var key in quest) {
                        console.log("Object extracted" + JSON.stringify(quest[key].questions_obj));
                        
                        for (var element in quest[key].questions_obj) {
                            console.log("Questions extracted "+ element + " " + quest[key].questions_obj[element] + JSON.stringify(quest[key].questions_obj[element]));
                            var question_obj = quest[key].questions_obj[element];
                            
                            for (var item in question_obj) {
                                console.log("Item:" + question_obj[item]);
                                
                                if (item != "level") //do not translate level field
                                    await translate_item(question_obj,item, body.language);
                            }
                        }
                    }
                }
                // send response
                callback(null, {
                    statusCode: 200,
                    body: JSON.stringify(quest)
                })
            }
        )
        .catch(err =>
            callback(null, {
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not fetch the questions' + JSON.stringify(err)
            })
        );
    });
};

// translating items using AWS Translate
const translate_item = async (question_obj,item,language) => {
    return new Promise((resolve, reject) => {
        var translateParams = {
            SourceLanguageCode: 'en',
            TargetLanguageCode: language,
            Text: JSON.stringify(question_obj[item])
        };
        console.log("inside we have " + item + " "+ question_obj[item] + " " + JSON.stringify(question_obj[item]))
        translate.translateText(translateParams, (err, data) => {
            
            if (err){
                console.log("Erros String: " + err)
            }
            
            console.log("Latest Translation: " + data.TranslatedText)
            let translated = JSON.stringify(data.TranslatedText)
            question_obj[item] = translated.substring(2, translated.length - 2) //remove quote from the translation
            
            resolve();// resolve promise
        })
    });
}
