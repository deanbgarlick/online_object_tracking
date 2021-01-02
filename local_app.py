import base64
import datetime
import pickle

import cv2
import numpy as np
from PIL import Image

from flask import Flask, Response
from kafka import KafkaConsumer


# Fire up the Kafka Consumer
# topic_in = "raw-video"
# topic_in = "object-detections-test"
topic_in = "object-tracking-video"

consumer = KafkaConsumer(
    topic_in,
    bootstrap_servers=['localhost:9091']
)

# Set the consumer in a Flask App
app = Flask(__name__)


def from_base64(buf):
    buf_decode = base64.b64decode(buf)
    buf_arr = np.fromstring(buf_decode, dtype=np.uint8)
    return cv2.imdecode(buf_arr, cv2.IMREAD_UNCHANGED)


@app.route('/', methods=['GET'])
def home():
    return "Hello from flask"


@app.route('/video', methods=['GET'])
def video():
    """
    This is the heart of our video display. Notice we set the mimetype to
    multipart/x-mixed-replace. This tells Flask to replace any old images with
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(),
        mimetype='multipart/x-mixed-replace; boundary=frame')


def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert
    them to a Flask-readable format.
    """
    for msg in consumer:
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + base64.b64decode(msg.value) + b'\r\n\r\n')


@app.route('/annotated_video', methods=['GET'])
def annotated_video():
    """
    This is the heart of our video display. Notice we set the mimetype to
    multipart/x-mixed-replace. This tells Flask to replace any old images with
    new values streaming through the pipeline.
    """
    return Response(
        get_annotation_video_stream(),
        mimetype='multipart/x-mixed-replace; boundary=frame')


def get_annotation_video_stream():

    score_threshold = 0

    for msg in consumer:

        buf = base64.b64decode(msg.value)
        payload = pickle.loads(buf)
        image_id, img, predictions = payload['image_id'], payload['img'], np.array(payload['frame_results'])

        image = from_base64(img)
        # img = from_base64(msg.value)
        # image = Image.fromarray(img)
        # image = cv2.cvtColor(np.float32(image), cv2.COLOR_BGR2RGB)

        # image = Image.fromarray(img)
        image = np.array(image)

        predictions = predictions[predictions[:, 6] > score_threshold, :]

        height, width, channels = image.shape

        for annotation in predictions.tolist():
            tl = (int(annotation[2]), int(annotation[3]))
            br = (int(annotation[4])+int(annotation[2]), int(annotation[5])+int(annotation[3]))
            cv2.rectangle(image, tl, br, (0,255,0),2)

        _, buffer = cv2.imencode('.jpg', image)

        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + base64.b64decode(base64.b64encode(buffer)) + b'\r\n\r\n')



def decode(string):

    jpg_original = base64.b64decode(string)
    jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8)
    img = cv2.imdecode(jpg_as_np, flags=1)
    return img

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8001, debug=True)