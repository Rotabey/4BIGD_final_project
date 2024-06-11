FROM python:3.12.3-bookworm

RUN pip install poetry 

COPY pyproject.toml poetry.lock /home/.

WORKDIR /home

RUN apt-get update && apt-get install -y default-jdk

RUN poetry install

COPY main.py README.MD dataset_downloader.py pipeline.py /home/.
COPY mongo-spark-connector_2.13-10.3.0.jar /home/mongo-spark-connector_2.13-10.3.0.jar

ENTRYPOINT ["poetry", "run", "python3", "main.py"]