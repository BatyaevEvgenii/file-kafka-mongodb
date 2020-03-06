const express = require('express');
const multer = require('multer');
const fs = require('fs');

const kafka = require('kafka-node');

const app = express();

var bodyParser = require('body-parser');

app.use(bodyParser.json());

app.use(bodyParser.urlencoded({
  extended: true
})); 

var Producer = kafka.Producer,
  client = new kafka.KafkaClient(),
  producer = new Producer(client);

producer.on('ready', () => {
    console.log('Producer is ready to send...');
    // console.log(producer);
});

producer.on('error', (err) => {
    console.log('Producer is error');
    console.log(err);
});

app.use(express.static('./public'));

app.get('/', (request, response) => {
  response.sendFile(__dirname + '/public/index.html');
});

app.get('/success', (request, response) => {
  response.sendFile(__dirname + '/public/success.html');
});

const path = './tmp'

var storage = multer.diskStorage({
  destination: (req, file, callback) => {
    if (!fs.existsSync(path)){
      fs.mkdir(path, (err) => {
        if(err) {
          console.log(err.stack)
        } else {
            callback(null, path);
        }
      });
    } else {
        callback(null, path);
    };
  },
  filename: (req, file, callback) => {
    callback(null, Date.now() + '-' + file.originalname);
  }
});

var upload = multer({ storage : storage});

app.post('/uploadfile', upload.single('userFile'), (req, res, next) => {
  var sentMessage = JSON.stringify(req.file, ['filename', 'destination']);
  payloads = [{ topic: req.file.fieldname, messages:sentMessage , partition: 0 }];
  producer.send(payloads, (err, data) => {

  });
  console.log('File has downloaded');
  console.log(req.file);

  res.redirect('/success');
});

app.listen(3001,() => {
  console.log('Server has started on 3001!');
});