const mongoose = require('mongoose');

const question_schema = new mongoose.Schema({
    "_id": String
}, { collection: 'tedx_data' });

module.exports = mongoose.model('question', question_schema);
