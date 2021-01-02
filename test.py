import base64
import datetime
import glob
import os
import pickle
import time

import cv2
import numpy as np
import pandas as pd
import torch

from flask import Flask, Response
from kafka import KafkaConsumer, KafkaProducer


def from_base64(buf):
    buf_decode = base64.b64decode(buf)
    buf_arr = np.fromstring(buf_decode, dtype=np.uint8)
    return cv2.imdecode(buf_arr, cv2.IMREAD_UNCHANGED)


producer = KafkaProducer(bootstrap_servers='localhost:9091')


def main(annotations=False):

    if not annotations:

        topic_out = "raw-video"

        for filename in sorted(glob.glob(os.path.join('data/object_detection_microservice/sequence-1/img1', '*.jpg'))):
            # img = cv2.imread(filename)
            # img_array.append(img)
            buffer = open(filename,'rb').read()
            producer.send(topic_out, base64.b64encode(buffer))

            time.sleep(0.02)


    else:

        topic_out = "object-detections"
        #topic_out = "object-detections-test"

        img_array = []
        for filename in sorted(glob.glob(os.path.join('data/object_detection_microservice/sequence-1/img1', '*.jpg'))):
            # img = cv2.imread(filename)
            # img_array.append(img)
            buffer = open(filename,'rb').read()
            img_array.append(buffer)

        # for buffer in img_array:
        #     producer.send(topic_out, base64.b64encode(buffer))
        #     time.sleep()


        seq1 = pd.read_csv('data/object_detection_microservice/sequence-1/Seq1-Vis.txt', sep='\t', header=None)
        # frame_annotations = seq1.groupby([0]).apply(lambda x: pd.Seriesx.values)
        frame_annotations = seq1.values

        frame_annotations = frame_annotations[:, [0,1,3,4,5,6]]

        frame_annotations = np.hstack([frame_annotations, np.array([1]*frame_annotations.shape[0]).reshape(-1,1)])

        for _ in range(3):
            frame_annotations = np.hstack([frame_annotations, np.array([-1]*frame_annotations.shape[0]).reshape(-1,1)])


        for image_id, img in enumerate(img_array):

            time.sleep(0.05)

            frame_results = frame_annotations[frame_annotations[:,0]==image_id]

            print(frame_results)

            # Convert to bytes and send to kafka
            # producer.send(topic, buffer.tobytes())
            buffer = pickle.dumps({'image_id':image_id , 'img':base64.b64encode(img), 'frame_results':frame_results})
            producer.send(topic_out, base64.b64encode(buffer))


if __name__ == '__main__':
    main()
