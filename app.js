const mongoose = require("mongoose");
const express = require("express");
const {
    Parser
} = require("json2csv");
const fs = require("fs");

const app = express();

// Define your schema
var barcodeSchema = new mongoose.Schema({
    Name: String,
    MRP: Number,
    EAN: Number,
    productLink: String
});

// Define your model
var EANCODE_DB = mongoose.model('EANCODE_DB', barcodeSchema);

// MongoDB connection URI
const uri = 'mongodb+srv://walkupwagon:walkup_up_wagon%40MK123@cluster0.uc34gsl.mongodb.net/SK?retryWrites=true&w=majority';

// Connect to MongoDB
mongoose.connect(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true
}).then(async () => {
    console.log('Database connected!!!!');

    try {
        // Fetch data from the database
        const ean_data = await EANCODE_DB.find({});
        console.log("Electrical database loaded, number of items: " + ean_data.length);

        // Remove duplicates using a Set
        const seenEANs = new Set();
        const uniqueData = [];

        for (const item of ean_data) {
            if (!seenEANs.has(item.EAN)) {
                seenEANs.add(item.EAN);
                uniqueData.push(item);
            }
        }

        console.log("Number of unique items: " + uniqueData.length);

        // Convert unique data to CSV
        const fields = ['Name', 'MRP', 'EAN', 'productLink']; // Fields to include in the CSV
        const json2csvParser = new Parser({
            fields
        });
        const csv = json2csvParser.parse(uniqueData);

        // Write CSV to file
        fs.writeFileSync('unique_ean_data.csv', csv);
        console.log('CSV file successfully created: unique_ean_data.csv');
    } catch (err) {
        console.error("Error processing data:", err);
    } finally {
        mongoose.connection.close(); // Ensure the connection is closed after the operation
    }
}).catch(err => {
    console.error('Database connection error:', err);
});

// Start the server
app.listen(8000, () => {
    console.log("Server running at http://localhost:8000");
});