import {ObjectID} from "bson";
import * as fs from "fs";
import {GridFSBucket, GridFSBucketReadStream, MongoClient, MongoClientOptions} from "mongodb";
import {Stream} from "stream";

export interface IGridFSObject {
    _id: ObjectID;
    length: number;
    chunkSize: number;
    uploadDate: Date;
    md5: string;
    filename: string;
    contentType: string;
    metadata: object;
}

export interface IGridFSWriteOption {
    filename: string;
    chunkSizeBytes?: number;
    metadata?: any;
    contentType?: string;
    aliases?: string[];
}

export class MongoGridFS {

    private readonly mongoClient: MongoClient;
    private bucketName: string;
    private basePath: string;

    /**
     * Constructor
     * @param {mongoClient} mongoClient
     * @param {string} databaseName
     * @param {MongoClientOptions} mongoOptions
     * @param {string} bucketName
     * @param {string} basePath
     */
    constructor(mongoClient: MongoClient, bucketName: string, basePath?: string) {
        this.mongoClient = mongoClient;
        this.bucketName = bucketName || "fs";
        this.basePath = basePath || `${__dirname}/../cache`;
    }

    get connection(): MongoClient {
        return this.mongoClient;
    }

    /**
     * Returns a stream of a file from the GridFS.
     * @param {string} id
     * @return {Promise<GridFSBucketReadStream>}
     */
    public async readFileStream(id: string): Promise<GridFSBucketReadStream> {
        const object = await this.findById(id);
        return this.bucket.openDownloadStream(object._id);
    }

    /**
     * Save the File from the GridFs to the filesystem and get the Path back
     * @param {string} id
     * @param {string} fileName
     * @param {string} filePath
     * @return {Promise<string>}
     */
    public async downloadFile(id: string, fileName?: string, filePath?: string): Promise<string> {
        const object = await this.findById(id);
        if (!fileName) {
            fileName = object.filename;
        }
        if (!filePath) {
            filePath = "";
        }
        if (this.basePath.charAt(this.basePath.length - 1) !== "/") {
            filePath += "/";
        }
        if (!fs.existsSync(`${this.basePath}${filePath}`)) {
            throw new Error("Path not found");
        }
        return new Promise<string>(async (resolve, reject) => {
            this.bucket.openDownloadStream(object._id)
                .once("error", async (error) => {
                    reject(error);
                })
                .once("end", async () => {
                    // await client.close();
                    resolve(`${this.basePath}${filePath}${fileName}`);
                })
                .pipe(fs.createWriteStream(`${this.basePath}${filePath}${fileName}`));
        });
    }

    /**
     * Find a single object by id
     * @param {string} id
     * @return {Promise<IGridFSObject>}
     */
    public async findById(id: string): Promise<IGridFSObject> {
        return await this.findOne({_id: new ObjectID(id)});
    }

    /**
     * Find a single object by condition
     * @param filter
     * @return {Promise<IGridFSObject>}
     */
    public async findOne(filter: any): Promise<IGridFSObject> {
        const result = await this.find(filter);
        if (result.length === 0) {
            throw new Error("No Object found");
        }
        return result[0];
    }

    /**
     * Find a list of object by condition
     * @param filter
     * @return {Promise<IGridFSObject[]>}
     */
    public async find(filter: any): Promise<IGridFSObject[]> {
        return await this.bucket.find(filter).toArray();
    }

    /**
     * Find objects by condition
     * @param stream
     * @param options
     */
    public writeFileStream(stream: Stream, options: IGridFSWriteOption): Promise<IGridFSObject> {
        return new Promise((resolve, reject) => stream
            .pipe(this.bucket.openUploadStream(options.filename, {
                aliases: options.aliases,
                chunkSizeBytes: options.chunkSizeBytes,
                contentType: options.contentType,
                metadata: options.metadata,
            }))
            .on("error", async (err) => {
                reject(err);
            })
            .on("finish", async (item: IGridFSObject) => {
                resolve(item);
            }),
        );
    }

    /**
     * Upload a file directly from a fs Path
     * @param {string} uploadFilePath
     * @param {IGridFSWriteOption} options
     * @param {boolean} deleteFile
     * @return {Promise<IGridFSObject>}
     */
    public async uploadFile(
        uploadFilePath: string,
        options: IGridFSWriteOption,
        deleteFile: boolean = true): Promise<IGridFSObject> {
        if (!fs.existsSync(uploadFilePath)) {
            throw new Error("File not found");
        }
        const tryDeleteFile = (obj?: any): any => {
            if (fs.existsSync(uploadFilePath) && deleteFile === true) {
                fs.unlinkSync(uploadFilePath);
            }
            return obj;
        };
        return await this.writeFileStream(fs.createReadStream(uploadFilePath), options)
            .then(tryDeleteFile)
            .catch((err) => {
                tryDeleteFile();
                throw err;
            });
    }

    /**
     * Delete an File from the GridFS
     * @param {string} id
     * @return {Promise<boolean>}
     */
    public delete(id: string): Promise<boolean> {
        return new Promise<boolean>((resolve, reject) => {
            this.bucket.delete(new ObjectID(id), (async (err) => {
                if (err) {
                    reject(err);
                }
                resolve(true);
            }));
        });
    }

    private get bucket(): GridFSBucket {
        const connection = this.mongoClient.db();
        return new GridFSBucket(connection, {bucketName: this.bucketName});
    }
}
