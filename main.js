const express = require("express");
const app = express();
const port = 8082;

const request = require("request");

app.get("/", (req, res) => res.send("Hello World from user!"));

app.listen(port, () =>
  console.log(`App listening at http://localhost:${port}`)
);

setInterval(() => {
  request("http://api:3000", { json: true }, (err, res, body) => {
    console.log(err ? err : body);
  });
}, 2000);
