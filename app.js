"use strict";

var AWS = require("aws-sdk");
var async = require("async");
var _ = require("lodash");
var moment = require("moment");
var remove = require("delete");
var fs = require("fs");
var path  = require("path");
var jimp = require("jimp");

AWS.config.loadFromPath('../WebPage/config/aws-config.json');

var sqs = new AWS.SQS();
var s3 = new AWS.S3();

var queueUrl = "https://sqs.us-west-2.amazonaws.com/440412059271/awsPsoirQueue";
var bucketName = "awspsoir";
var avalaibleConvertions = ["greyScale", "invert", "sepia", "blur"];

work(); // initial iteration

function work() {
    var receiptHandleMsg;

    async.waterfall([
            function (cb) {
                return receiveQueueMsg(cb);
            },
            function (msgBody, receiptHandle, cb) {
                receiptHandleMsg = receiptHandle;
                return convertImage(msgBody, cb);
            },
            function (convertedFileName, cb) {
                return saveImageInBucket(convertedFileName, cb);
            },
            function (cb) {
                return deleteQueueMsg(receiptHandleMsg, cb);
            }
        ], function (err, result) { //default error
		    if(err) 
			{
                console.log("ERROR: " + err);
            } 
			else
			{
                console.log("Coversion successfully done");
            }
            return work();
        }
    );
}

function receiveQueueMsg(cb) {
    var params = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1
    };
    sqs.receiveMessage(params, function(err, data) { /*Receive Message from queue*/
        if (err) {
            console.log(err, err.stack);
            return cb(err);
        }
        else {
            if(!data.hasOwnProperty("Messages")) {
                return cb("No messages on the queue");
            }

            console.log("Received message");
            var messsageBody = JSON.parse(data.Messages[0].Body);
            var receiptHandle = data.Messages[0].ReceiptHandle;
			/*Check if received message is correct*/
			if(!messsageBody.hasOwnProperty("key") || !messsageBody.hasOwnProperty("option")) {
				return cb("Invalid message");
			}

			if(!_.includes(avalaibleConvertions, messsageBody.option)) {
				return cb("Wrong option");
			}
            return cb(null, messsageBody, receiptHandle);
        }
    });
}


function convertImage(msgBody, cb) {
    var imageLink = encodeURI("https://s3-us-west-2.amazonaws.com/" + bucketName + "/" + msgBody.key);

    jimp.read(imageLink, function (err, image) {
        if (err) {
            return cb(err);
        }

        var extension = imageLink.split('.').pop();
        var convertedFileName =  moment() + "." + extension;

        switch (msgBody.option) {
            case "greyScale":
                image.greyscale();
                break;
            case "invert":
                image.invert();
                break;
			case "sepia":
				image.sepia();
				break;
			case "blur":
				image.blur(50);
				break;
            default:
                console.log("Case " + msgBody.operation + " doesn't exist");
        }
        console.log("File converted");

        image.write(convertedFileName, function () { //Save temp file locally
            console.log("File Saved");
            return cb(null, convertedFileName);
        });

    });
}

function saveImageInBucket(convertedFileName, cb) {

    var fileStream = fs.createReadStream(path.join( __dirname, convertedFileName));

    var params = {
        Bucket: bucketName,
        Key: "convertedImages/" + convertedFileName,
        ACL: "public-read",
        Body: fileStream
    };

    fileStream.on('error', function (err) {
        if (err) 
		{
			return cb(err); 
		}
    });
    fileStream.on('open', function () {
        s3.putObject(params, function(err, data) {
            if (err) {
                console.log(err, err.stack);
                return cb(err);
            }
            else {
                console.log("Image saved in bucket");
				/*Image saved in bucket, local file removal started*/
				remove([convertedFileName], function(err,removed){
					if(err)
					{
						console.log("Failed to remove local file");
						return cb(err);
					}
					return cb();
				});
            }
        });
    });
}

function deleteQueueMsg(receiptHandleMsg, cb) {

    var params = {
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandleMsg
    };
    sqs.deleteMessage(params, function(err, data) {
        if (err) {
            console.log(err, err.stack);
            return cb(err);
        }
        else {
            console.log("Msg deleted from queue");
            return cb();
        }
    });
}





