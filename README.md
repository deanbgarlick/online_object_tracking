This is a microservice based application for the online tracking of objects in videos using SSD, DeepSort, Kafka, Flask and Docker. 

To use this appllication git clone this repo, cd into the repo, then run:

```
docker-compose up -d --build
```


Once all microservices have booted, the service is ran with the following command to track objects in a video file:

```
python run.py <path_to_mp4_file.mp4>
```

Alternatively to use the tracker to track objects from your webcam run ```python run.py```.

In a terminal running ```python local_app.py``` will launch a local flask app on port 8001. Navigating to route
/video will render the frame by frame output of the tracking service in the browser.


![Browser image](screenshot.png)
