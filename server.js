const express = require('express');
const app = express();

app.get('/', (req,res)=>{
  res.send('RT7 CLOUD SERVER OK');
});

app.listen(process.env.PORT || 3000, ()=>{
  console.log('server start');
});
