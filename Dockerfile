FROM python:3.10

COPY conf /api-medium/conf
COPY data /api-medium/data
COPY info.log /api-medium/info.log
COPY pyproject.toml /api-medium/pyproject.toml
COPY credentials /api-medium/credentials
COPY docs /api-medium/docs
COPY notebooks /api-medium/notebooks
COPY src /api-medium/src

WORKDIR /api-medium/src

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

WORKDIR /api-medium

CMD ["kedro", "run"]