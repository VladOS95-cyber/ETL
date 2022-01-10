FROM python:3.9

WORKDIR /etl

COPY requirements.txt /etl

RUN pip3 install --upgrade pip && \
    pip3 install -r /etl/requirements.txt --no-cache-dir

COPY . /etl

CMD ["python3", "ETLoader.py"]