import base64
import hashlib
import io
from flask import Flask
import boto3
import pytube

app = Flask(__name__)


def upload_video(youtube_link, bucket_name):
    if bucket_name is None:
        return {
            "status": 500,
            "message": "No AWS Bucket configured on server!"
        }
    response_code = 200
    yt = pytube.YouTube(youtube_link)
    channel_id = yt.channel_id
    video_id = yt.video_id
    # get highest quality mp4 stream of video
    stream = yt.streams.filter(progressive=True, file_extension='mp4') \
        .order_by('resolution') \
        .desc() \
        .first()
    file_size = stream.filesize
    file_name = f"{channel_id}/{video_id}.mp4"
    response_message = ""
    print(f"Uploading {youtube_link} to {file_name} in bucket {bucket_name}")
    try:
        push_to_s3(file_name, file_size, bucket_name, stream)
        response_message = f"{youtube_link} uploaded to bucket {bucket_name} at {file_name}."
        print(response_message)
    except Exception as e:
        print(e)
        response_code = 500
        response_message = f"{youtube_link} upload failed:\n{e}"
        print(response_message)
    finally:
        return {
            "status": response_code,
            "youtube_link": youtube_link,
            "video_id": video_id,
            "channel_id": channel_id,
            "file_name": file_name,
            "file_size": file_size,
            "bucket_name": bucket_name,
            "message": response_message
        }


def push_to_s3(file_name, file_size, bucket_name, stream):
    s3 = boto3.client('s3')
    response = s3_upload(s3, file_name, bucket_name, stream) \
        if file_size < 5000000 \
        else s3_multipart_upload(s3, file_name, file_size, bucket_name, stream)
    if len(response) != 2:
        raise Exception(f"Received malformed response: {response}")
    if response[0]:
        return response[1]
    else:
        # let higher level function handle the error
        raise response[1]


def get_video_buffer(stream):
    buffer = io.BytesIO()
    stream.stream_to_buffer(buffer)
    buffer.seek(0)
    return buffer


def s3_upload(s3, file_name, bucket_name, stream):
    try:
        video_buffer = get_video_buffer(stream)
        s3.upload_fileobj(video_buffer, bucket_name, file_name)
        return True, file_name
    except Exception as e:
        return False, e


def md5_str(chunk):
    return base64.b64encode(hashlib.md5(chunk).digest()).decode('UTF-8')


def s3_multipart_upload(s3, file_name, file_size, bucket_name, stream):
    chunks = 0
    # store part_numbers and ETags for completion
    parts = []
    # must be between 1 - 10k
    part_number = 1
    # get iterable stream
    stream = pytube.request.stream(stream.url)
    multipart_upload = s3.create_multipart_upload(Bucket=bucket_name, ContentType='video/mp4', Key=file_name)
    upload_id = multipart_upload['UploadId']
    try:
        # iterate over stream, return None when done
        chunk = next(stream, None)
        while chunk is not None:
            if part_number > 10000:
                raise Exception('Number of parts exceeded threshold')
            # upload chunk
            content_md5 = md5_str(chunk)
            part_response = s3.upload_part(Bucket=bucket_name, Key=file_name,
                                           UploadId=upload_id, ContentMD5=content_md5,
                                           Body=chunk, PartNumber=part_number)
            chunks += len(chunk)
            print(f'Chunk {part_number}: Uploaded {chunks} / {file_size}')
            parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
            part_number += 1
            chunk = next(stream, None)
        # return all parts to signal completion
        s3.complete_multipart_upload(Bucket=bucket_name, Key=file_name,
                                     MultipartUpload={'Parts': parts},
                                     UploadId=upload_id)
        return True, file_name
    except Exception as e:
        s3.abort_multipart_upload(Bucket=bucket_name, Key=file_name, UploadId=upload_id)
        # let calling function handle the error
        return False, e

