const mongoose = require('mongoose');

const watch_schema = new mongoose.Schema({
    "_id": String,
    "watch_next_obj":Array
}, { collection: 'tedx_data' });

module.exports = mongoose.model('watch', watch_schema);
