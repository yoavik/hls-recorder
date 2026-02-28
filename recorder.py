import subprocess, boto3, os, time, threading
from datetime import datetime, timezone, timedelta

STREAM_URL = os.environ.get("STREAM_URL",
    "https://mako-streaming.akamaized.net/direct/hls/live/2033791/k12/"
    "hdntl=exp=1772376820~acl=%2f*~data=hdntl~hmac="
    "ee12dade6de5aa005be336f94436f14ec931aebe18eeeae11fe2bde3e1973dc4/index_4000.m3u8")
BUCKET = os.environ.get("S3_BUCKET", "tv-recorder")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SEGMENT_MINUTES = int(os.environ.get("SEGMENT_MINUTES", "30"))
RETENTION_HOURS = int(os.environ.get("RETENTION_HOURS", "6"))
TEMP_DIR = "/tmp/recordings"

s3 = boto3.client("s3", region_name=REGION,
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

def record_segment():
    os.makedirs(TEMP_DIR, exist_ok=True)
    now = datetime.now(timezone.utc)
    filename = now.strftime("%Y-%m-%d_%H-%M-%S") + ".mp4"
    filepath = os.path.join(TEMP_DIR, filename)
    s3_key = f"recordings/{now.strftime('%Y-%m-%d')}/{filename}"
    print(f"[{now.isoformat()}] Recording {SEGMENT_MINUTES}min -> {filename}")
    try:
        result = subprocess.run(["ffmpeg","-y","-i",STREAM_URL,"-t",str(SEGMENT_MINUTES*60),
            "-c","copy","-movflags","+faststart",filepath],
            capture_output=True, text=True, timeout=SEGMENT_MINUTES*60+120)
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            print(f"  Uploading to s3://{BUCKET}/{s3_key}")
            s3.upload_file(filepath, BUCKET, s3_key, ExtraArgs={"ContentType":"video/mp4"})
            os.remove(filepath)
            print(f"  Done: {s3_key}")
        else:
            print(f"  ERROR: Recording failed")
            if result.stderr: print(f"  {result.stderr[:500]}")
    except subprocess.TimeoutExpired:
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            s3.upload_file(filepath, BUCKET, s3_key, ExtraArgs={"ContentType":"video/mp4"})
            os.remove(filepath)
    except Exception as e:
        print(f"  ERROR: {e}")
        if os.path.exists(filepath): os.remove(filepath)

def cleanup_old():
    print(f"[Cleanup] Removing files older than {RETENTION_HOURS}h...")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=RETENTION_HOURS)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix="recordings/"):
            for obj in page.get("Contents", []):
                if obj["LastModified"].replace(tzinfo=timezone.utc) < cutoff:
                    print(f"  Deleting {obj['Key']}")
                    s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
    except Exception as e:
        print(f"  Cleanup error: {e}")

def cleanup_loop():
    while True:
        time.sleep(1800)
        cleanup_old()

if __name__ == "__main__":
    print(f"=== HLS Recorder === Bucket:{BUCKET} Seg:{SEGMENT_MINUTES}m Ret:{RETENTION_HOURS}h")
    t = threading.Thread(target=cleanup_loop, daemon=True); t.start()
    cleanup_old()
    while True:
        try: record_segment()
        except Exception as e: print(f"Error: {e}"); time.sleep(10)
