# mongo-gridfs

This is a simple wrapper for the new [MongoDB GridFSBucket API](http://mongodb.github.io/node-mongodb-native/3.0/tutorials/gridfs/streaming/).

The old GridStore-API is now [deprecated](http://mongodb.github.io/node-mongodb-native/3.0/tutorials/gridfs/gridstore/).


## How to install

That is simple

`npm install mongo-gridfs`

OR

`yarn add mongo-gridfs`

## Usage

### With MongoClient

```js
const mongoDBConnection = await MongoClient.connect('mongodb://localhost:27017/files');
let gridFS = new MongoGridFS(mongoDBConnection, "attachments");
gridFS.findById("59e085f272882d728e2fa4c2").then((item) => {
    console.log(item);
}).catch((err) => {
    console.error(err);
});

```

### With Mongoose

```js
const mongooseConnection = await Mongoose.connect('mongodb://localhost:27017/files');
let gridFS = new MongoGridFS(mongooseConnection.db, "attachments");
gridFS.findById("59e085f272882d728e2fa4c2").then((item) => {
    console.log(item);
}).catch((err) => {
    console.error(err);
});

```


## Methods

### findById

By this method you will simple get the meta-object from the MongoDB as a Promise-Object.
If nothing found at the Database, then it will reject and the catch-block will be executed.

```js
gridFS.findById("59e085f272882d728e2fa4c2").then((item) => {
    console.log(item);
}).catch((err) => {
    console.error(err);
});
```

### downloadFile

You will get the file simple written to the filesystem directly from the Database.
If nothing found at the Database, then it will reject and the catch-block will be executed.

```js
gridFS.downloadFile("59e085f272882d728e2fa4c2", {
    filename: "test.gif",
    targetDir: "/tmp"
}).then((downloadedFilePath) => {
    console.log(downloadedFilePath);
}).catch((err) => {
    console.error(err);
});
```

### readFileStream

You will get a GridFSBucketReadStream as Promise.
If nothing found at the Database, then it will reject and the catch-block will be executed.

This method is very useful, to stream the content directly to the user.

For example with express:
```js
return gridFS.readFileStream(req.params.id).then((item) => {
    item
    .once("error", () => {
        return res.status(400).end();
    }).pipe(res);
}).catch(() => res.status(500));
```

