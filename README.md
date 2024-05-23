# Movie Repository

## Run on Docker
1. First, use the command `docker build -t movie-repository .` to build the Docker image.
2. Then use the command `docker run --name="movie-repository" -p 8080:8080 -d` to create a Docker container using this image.
3. Open [localhost:8080](http://localhost:8080/) to access your Docker application.

## Run Locally
1. Use commands below to config poetry project:
   ```shell
   poetry config virtualenvs.in-project true
   poetry install
   poetry shell
   ```
2. Use the command `docker pull mongo` to pull the latest MongoDB image from Docker Hub.
3. Run the MongoDB image in a Docker container and expose it on port `27017`.
4. Edit the FastAPI start configuration to add environment variables: `total=10` and `page-size=5`.
5. Start this Poetry project in PyCharm.

## Contributions
Contact me on Discord at dioxide_cn or on QQ at 1050177109.
