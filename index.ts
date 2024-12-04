import "dotenv/config";
import path from "node:path";
import * as fs from "fs";

import * as Minio from "minio";
import { ObjectMetaData } from "minio/dist/main/internal/type";

const bucketEndpoint = process.env.S3_ENDPOINT as string;
const bucketName = process.env.S3_BUCKET as string;
const bucketRegion = process.env.S3_REGION as string;
const bucketKey = process.env.S3_KEY as string;
const bucketSecret = process.env.S3_SECRET as string;
const projectId = process.env.S3_PROJECTID as string;

let filePath = path.join(__dirname, "test-files");

export const minioClient = new Minio.Client({
  endPoint: bucketEndpoint,
  region: bucketRegion,
  useSSL: true,
  accessKey: bucketKey,
  secretKey: bucketSecret,
});

const setPolicy = async () => {
  let setPolicy = {
    Version: "2012-10-17",
    Statement: [
      {
        Sid: "allow-user-to-access-to-bucket",
        Effect: "Allow",
        Principal: {
          AWS: `arn:aws:iam:::user/p${projectId}:${bucketKey}`,
        },
        Action: ["s3:GetBucketLocation", "s3:ListBucket"],
        Resource: [`arn:aws:s3:::${bucketName}`],
      },
      {
        Sid: "allow-user-to-read-objects",
        Effect: "Deny",
        Principal: {
          AWS: `arn:aws:iam:::user/p${projectId}:${bucketKey}`,
        },
        Action: ["s3:GetObject"],
        Resource: [`arn:aws:s3:::${bucketName}/*`],
      },
    ],
  };

  await minioClient
    .setBucketPolicy(bucketName, JSON.stringify(setPolicy))
    .then((res) => {
      console.log(res);
    })
    .catch((e) => {
      console.log(e);
    });
};

export const s3ClientCheck = async () => {
  try {
    const exists = await minioClient.bucketExists(bucketName);
    if (exists) {
      console.log("Bucket " + bucketName + " exists.");
      let policy = await minioClient.getBucketPolicy(bucketName);
      console.log("Bucket Policy: ", policy);
    } else {
      await minioClient.makeBucket(bucketName, bucketRegion);
      await setPolicy();
      console.log("Bucket " + bucketName + " created in " + bucketRegion + ".");
    }
  } catch (error) {
    console.log(error);
  }
};

const uploadFiles = async () => {
  let files = fs.readdirSync(filePath);

  let uploadPromises = files.map(async (fileName) => {
    console.log("Uploading file: ", fileName);

    var metaData: ObjectMetaData = {
      "Content-Type": "text/plain",
    };
    let file = path.join(filePath, fileName);

    let res = await minioClient.fPutObject(
      bucketName,
      "test/" + fileName,
      file,
      metaData
    );
    console.log("File uploaded successfully", fileName, res);
  });

  await Promise.all(uploadPromises);
  console.log("All files uploaded successfully");
};

const listFiles = async () => {
  let stream = minioClient.listObjects(bucketName, "", true);
  let objects = [] as any[];
  let objectCount = 0;
  stream.on("data", (obj) => {
    objects.push(obj);
    objectCount++;
  });
  stream.on("end", () => {
    console.log("End of list objects");
    console.log("Objects found:", objects);
    console.log("Total Objects", objectCount);
  });
};

(async () => {
  await s3ClientCheck();
  await uploadFiles();
  listFiles();
})();
