# Movie Repository
## Run on Docker
1. You should use `docker build -t movie-repository .` command to build docker image first.
2. Then use command `docker run --name="movie-repository" -p 8080:8080 -d` to build a docker container by using this image.
3. Open [localhost:8080](http://localhost:8080/) to visit your docker application.

## Run on Local
1. Use command `docker pull mongo` to pull the latest mongodb image from docker hub.
2. Run your mongo image to docker container and expose it on port `27017`.
3. Edit fast-api start configuration, add environment variables `total=10;page-size=5`.
4. Start this poetry project on PyCharm.

## Contributions
Contact with me on discord dioxide_cn or on QQ 1050177109. 
