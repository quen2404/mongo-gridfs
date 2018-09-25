import {ObjectID} from 'bson';
import * as fs from 'fs';
import {Db, GridFSBucket, GridFSBucketReadStream} from 'mongodb';
import osTmpdir = require('os-tmpdir');
import {Stream} from 'stream';
import uniqueFilename = require('unique-filename');

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

export interface IDownloadOptions {
    filename: boolean | string;
    targetDir?: string;
}

export class MongoGridFS {

    /**
     * Constructor
     * @param {connection} connection
     * @param {string} bucketName
     */
    constructor(public readonly connection: Db, public readonly bucketName: string = 'fs') {
    }

    public get bucket(): GridFSBucket {
        return new GridFSBucket(this.connection, {bucketName: this.bucketName});
    }

    public static getDownloadPath(object: IGridFSObject, options: IDownloadOptions = {
        filename: false,
    }) {
        let finalPath = '';
        if (!options.targetDir) {
            if (typeof options.filename === 'string') {
                finalPath = `${osTmpdir()}/${options.filename}`;
            } else {
                if (options.filename === true) {
                    finalPath = `${osTmpdir()}/${object._id}`;
                } else {
                    finalPath = uniqueFilename(osTmpdir());
                }
            }
        } else {
            if (typeof options.filename === 'string') {
                finalPath = `${options.targetDir}/${options.filename}`;
            } else {
                if (options.filename === true) {
                    finalPath = object.filename;
                } else {
                    finalPath = uniqueFilename(options.targetDir);
                }
            }
        }
        return finalPath;
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
     * @param {IDownloadOptions} options
     * @return {Promise<string>}
     */
    public async downloadFile(id: string, options?: IDownloadOptions): Promise<string> {
        const object = await this.findById(id);
        const downloadPath = MongoGridFS.getDownloadPath(object, options);
        return new Promise<string>(async (resolve, reject) => {
            this.bucket.openDownloadStream(object._id)
                .once('error', async (error) => {
                    reject(error);
                })
                .once('end', async () => {
                    resolve(downloadPath);
                })
                .pipe(fs.createWriteStream(downloadPath, {}));
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
            throw new Error('No Object found');
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
            .on('error', async (err) => {
                reject(err);
            })
            .on('finish', async (item: IGridFSObject) => {
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
            throw new Error('File not found');
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
}
