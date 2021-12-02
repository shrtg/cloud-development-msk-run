import fs from 'fs';
import csv from 'csvtojson';
import path from 'path';
import stream from 'stream';

const CSV_FILE_PATH = path.join(__dirname, './csv/nodejs-hw1-ex1.csv');
const OUTPUT_DIR_PATH = path.join(__dirname, './output');
const OUTPUT_TXT_FILE_PATH = path.join(OUTPUT_DIR_PATH, 'result.txt');

const prepareOutputFolder = () => {
    try {
        fs.mkdirSync(OUTPUT_DIR_PATH);
    } catch(error: any) {
        if (error.code !== 'EEXIST') {
            throw error;
        }
    }

    fs.rmSync(OUTPUT_TXT_FILE_PATH, {
        force: true,
    });
};

prepareOutputFolder();

const createErrorLogger = (description: string) => (error: any) => {
    console.error(description);
    console.error(error);
};

const writeToDb = (data: any) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (Math.random() > 0.2) {
                console.log('WRITE TO DB');
                console.log(data);
                resolve(true);
            } else {
                reject(new Error('DB ERROR'))
            }
        }, 500)
    })
};


const fileReadStream = fs.createReadStream(CSV_FILE_PATH)
    .on('error', createErrorLogger('CSV FILE READING ERROR'));

const csvParseStream = csv()
    .on('error', createErrorLogger('CSV PARSING ERROR'));

const writeToDbStream = new stream.Transform({
    async transform(chunk, encoding, callback) {
        try {
            const {
                Book: book,
                Author: author,
                Price: price
            } = JSON.parse(chunk.toString());

            const result = { book, author, price };

            await writeToDb(result);

            callback(null, JSON.stringify(result) + '\n');
        } catch (error: any) {
            callback(error);
        }
    }
}).on('error', createErrorLogger('WRITE TO DB ERROR'));

const writeToFileStream = fs.createWriteStream(OUTPUT_TXT_FILE_PATH)
    .on('error', createErrorLogger('WRITE TO FILE ERROR'));

fileReadStream
    .pipe(csvParseStream)
    .pipe(writeToDbStream)
    .pipe(writeToFileStream)
    .on('error', createErrorLogger('UNHANDLED ERROR'))
    .on('finish', () => console.log('FINISHED SUCCESSFULLY'));
