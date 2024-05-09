FROM python:3.9

ADD requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt


ADD . /src

WORKDIR /src

# Make the entrypoint script executable so it can be run as a program.
RUN chmod +x ./agg.sh

ENTRYPOINT ./agg.sh
