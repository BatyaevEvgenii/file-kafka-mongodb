const express = require('express');
const app = express();

const kafka = require('kafka-node');

var Consumer = kafka.Consumer,
  client = new kafka.KafkaClient(),
  consumer = new Consumer(client, [{ topic: 'userFile', partition: 0}], {autoCommit: false});

const mongoose = require('mongoose');
const dbURI = 'mongodb://localhost:32769/files';
mongoose.connect(dbURI, {useNewUrlParser: true, useUnifiedTopology: true });

const filesSchema = new mongoose.Schema({ filename: String, destination: String });
const File = mongoose.model('File', filesSchema);

// мониторим успешное соединение и пишем в базу
mongoose.connection.once('connected', (connected) => {
  console.log('Connected to ' + dbURI);
  consumer.on('message', (message) => {
    console.log(message.value);  
    const parsed = JSON.parse(message.value);
    const file = new File({ 
      filename: parsed.filename, 
      destination: parsed.destination});
    file.save().then(() => console.log('Yep message saved!'));
  });
});

// ошибка соединения
mongoose.connection.on('error', (err) => {
  console.log('error: ' + err);
  consumer.on('error', (err) => {
    console.log('Error:',err);
  });
});

// если разрыв соединения
mongoose.connection.on('disconnected', (connected) => {
  console.log('Disconnected');
});

app.listen(3002,() => {
  console.log('Server has started on 3002!');
});