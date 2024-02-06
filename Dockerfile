FROM python:3.10-slim
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
COPY ./src /app/src

ENV PYTHONPATH=.

WORKDIR /app
CMD ["python", "./src/main.py"]
