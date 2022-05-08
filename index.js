const app = require("express")();
const { Client } = require("pg");
const crypto = require("crypto");
const HashRing = require("hashring");
const { url } = require("inspector");
const { rows } = require("pg/lib/defaults");
const hr = new HashRing();

const port = process.env.PORT || 8000;

hr.add("5556");
hr.add("5433");
hr.add("5434");

const clients = {
  5556: new Client({
    host: "localhost",
    port: 5556,
    user: "postgres",
    password: "postgres",
    database: "postgres",
  }),
  5433: new Client({
    host: "localhost",
    port: 5433,
    user: "postgres",
    password: "postgres",
    database: "postgres",
  }),
  5434: new Client({
    host: "localhost",
    port: 5434,
    user: "postgres",
    password: "postgres",
    database: "postgres",
  }),
};

async function connect() {
  await clients[5556].connect();
  await clients[5433].connect();
  await clients[5434].connect();
}
connect();

app.get("/:urlId", async (req, res) => {
  const urlId = req.params.urlId;
  const server = hr.get(urlId);

  const result = await clients[server].query(
    `SELECT * FROM url_table WHERE url_id = $1;`,
    [urlId]
  );
  if (result.rows.length > 0) {
    return res.send({
      url: result.rows[0],
      server: server,
      urlId: urlId,
      message: `success`,
    });
  } else {
    return res.send({
      url: "",
      server: "",
      urlId: "",
      message: "URL not found",
    });
  }
});

app.post("/", async (req, res) => {
  const url = req.query.url;
  //consistency hash to get a port
  const hash = crypto.createHash("sha256").update(url).digest("base64");
  const urlId = hash.substring(0, 5);
  const server = hr.get(urlId);

  await clients[server].query(
    `INSERT INTO url_table (url, url_id) VALUES ($1,$2);`,
    [url, urlId]
  );
  return res.send({
    hash: hash,
    urlId: urlId,
    url: url,
    server: server,
    console: console.log(`${url} hashed to ${hash} and is on server ${server}`),
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
