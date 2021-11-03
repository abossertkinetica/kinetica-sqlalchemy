FROM python
RUN apt-get update && apt-get install -y unixodbc-dev
RUN mkdir /app
COPY . /app
COPY ./dbfiles/libKineticaODBC.so /etc
COPY ./dbfiles/odbc.ini /etc
COPY ./dbfiles/odbcinst.ini /etc
WORKDIR /app
RUN pip install . && \
    pip install -r requirements.txt && \
    pip install -r requirements-dev.txt
ENTRYPOINT ["pytest"]
